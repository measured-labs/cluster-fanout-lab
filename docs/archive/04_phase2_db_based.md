# Phase 2 — DB 기반 병렬 처리

> 보존 원문(2026-11-18 `docs/archive/`로 옮김). 정리본은 [ADR-000](../adr/ADR-000-fanout-fanin-design.md)이다. 코드가 아직 없어서, 이 문서의 수치(응답 예시의 시간 · 건수 · 비율)는 모두 예상(실측 아님)이다.

## 1. 개요

Phase 2는 Phase 1과 **동일한 Fan-out/Fan-in 구조**를 저장소만 DB로 교체하여 구현한다.

txt 데이터를 여러 개의 파티션 테이블에 분산 INSERT하고, 각 테이블에 대해 별도의 Virtual Thread Worker가 병렬 쿼리로 단어 빈도를 집계한 뒤, 동일한 Aggregator가 결과를 통합한다.

**이 Phase에서 체험할 것:**
- 저장소가 파일에서 DB로 바뀌어도 병렬 구조는 동일함
- DB 샤딩(테이블 분리)의 기본 원리
- SQL 집계 vs Java 집계의 성능 차이
- Aggregator의 저장소 독립성 (Phase 1과 동일한 컴포넌트 재사용)

---

## 2. 아키텍처

### 2.1 전체 흐름

```
[원본 txt 파일]
       │
       ▼
[DataDistributor] ─── 분산 INSERT ───→ text_partition_0
                                        text_partition_1
                                        text_partition_2
                                        text_partition_3
       │
       ▼
[DbClusterCoordinator]
       │
       ├── [Worker 0] → text_partition_0 SELECT → Map<String, Long>
       ├── [Worker 1] → text_partition_1 SELECT → Map<String, Long>
       ├── [Worker 2] → text_partition_2 SELECT → Map<String, Long>
       └── [Worker 3] → text_partition_3 SELECT → Map<String, Long>
       │
       ▼
[WordCountAggregator] ─── 결과 합산 ───→ 최종 Map<String, Long>  ← Phase 1과 동일
       │
       ▼
[AnalysisResult + PerformanceResult]
```

### 2.2 Phase 1과의 구조 비교

```
Phase 1:  FilePartitioner    → FileWordCounter      → WordCountAggregator
Phase 2:  DataDistributor    → DbWordCounter         → WordCountAggregator
          ~~~~~~~~~~~~~~~      ~~~~~~~~~~~~~~           ==================
          저장소에 따라 다름    저장소에 따라 다름        완전히 동일
```

### 2.3 컴포넌트 상세

| 컴포넌트 | 클래스명 | 역할 | Phase 1 대응 |
|----------|----------|------|-------------|
| Splitter | `DataDistributor` | 데이터를 N개 테이블에 분산 저장 | FilePartitioner |
| Worker | `DbWordCounter` | 파티션 테이블에서 쿼리로 빈도 집계 | FileWordCounter |
| Coordinator | `DbClusterCoordinator` | Worker 생성/관리, 오케스트레이션 | FileClusterCoordinator |
| Aggregator | `WordCountAggregator` | 결과 합산 **(재사용)** | 동일 |

---

## 3. 데이터베이스 설계

### 3.1 샤딩 시뮬레이션 전략

실제 분산 DB에서는 여러 물리 서버에 데이터를 분산한다. 이 프로젝트에서는 **하나의 H2 인스턴스 내에 여러 테이블을 생성**하여 각 테이블을 독립 노드(샤드)로 취급한다.

```
실제 클러스터:
  [Server A] → shard_0 테이블
  [Server B] → shard_1 테이블
  [Server C] → shard_2 테이블

이 프로젝트 (시뮬레이션):
  [H2 Instance] → text_partition_0 테이블
                → text_partition_1 테이블
                → text_partition_2 테이블
```

한계점: 실제 분산 환경의 네트워크 지연, 노드 장애는 시뮬레이션되지 않는다. 그러나 **병렬 쿼리 → 결과 통합**이라는 핵심 구조는 동일하게 체험할 수 있다.

### 3.2 테이블 스키마

```sql
-- 동적으로 생성되는 파티션 테이블
-- N은 0부터 partitionCount-1까지

CREATE TABLE text_partition_{N} (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    line_number BIGINT NOT NULL,
    content     VARCHAR(4000) NOT NULL
);

CREATE INDEX idx_partition_{N}_line ON text_partition_{N}(line_number);
```

| 컬럼 | 타입 | 설명 |
|------|------|------|
| id | BIGINT | 자동 증가 PK |
| line_number | BIGINT | 원본 파일에서의 행 번호 (추적용) |
| content | VARCHAR(4000) | 텍스트 내용 (한 줄) |

### 3.3 분산 저장 전략

라운드 로빈 방식으로 원본 파일의 각 행을 파티션 테이블에 분배한다.

```
원본 파일 (10줄, 파티션 3개):

  Line 1 → INSERT INTO text_partition_0 (line_number, content) VALUES (1, '...')
  Line 2 → INSERT INTO text_partition_1 (line_number, content) VALUES (2, '...')
  Line 3 → INSERT INTO text_partition_2 (line_number, content) VALUES (3, '...')
  Line 4 → INSERT INTO text_partition_0 (line_number, content) VALUES (4, '...')
  Line 5 → INSERT INTO text_partition_1 (line_number, content) VALUES (5, '...')
  ...
```

**배치 INSERT 적용**: 한 행씩 INSERT하면 네트워크/커밋 오버헤드가 크므로, 1000행 단위 배치 INSERT를 적용한다.

---

## 4. 컴포넌트 상세 설계

### 4.1 DataDistributor

**책임**: 원본 txt 파일의 데이터를 N개의 파티션 테이블에 분산 저장한다.

**처리 과정:**
```
1. 기존 파티션 테이블 DROP (있으면)
2. N개의 파티션 테이블 CREATE + 인덱스 생성
3. 원본 파일을 BufferedReader로 한 줄씩 읽으며:
   a. 라운드 로빈으로 대상 테이블 결정: lineNumber % partitionCount
   b. 배치 버퍼에 추가
   c. 버퍼가 1000행 차면 배치 INSERT 실행
4. 잔여 버퍼 플러시
5. 각 테이블의 행 수 통계 반환
```

**입력/출력:**
```
입력: source.txt (원본 파일 경로), partitionCount (테이블 수)
출력: List<PartitionInfo> (테이블명, 행 수)
```

### 4.2 DbWordCounter

**책임**: 하나의 파티션 테이블에서 단어 빈도를 집계한다. Worker 역할.

**두 가지 집계 방식 제공:**

#### 방식 A: SQL 집계

DB 엔진이 집계를 수행한다.

```sql
-- 각 행의 content를 가져와서 Java에서 토큰화 후 집계
SELECT content FROM text_partition_{N}
```

```
처리 과정:
1. SELECT로 해당 파티션의 모든 content를 조회
2. Java에서 각 줄을 토큰화
3. 로컬 Map<String, Long>에 빈도 누적
4. 완성된 Map 반환
```

#### 방식 B: 애플리케이션 집계 (기본)

Java 레벨에서 모든 집계를 수행한다. Phase 1의 FileWordCounter와 동일한 로직이다.

```
처리 과정:
1. JdbcTemplate으로 SELECT content FROM text_partition_{N} 실행
2. ResultSet을 순회하며:
   a. 소문자 변환
   b. 특수문자 제거
   c. 공백 기준 토큰화
   d. Map에 빈도 누적
3. 완성된 Map<String, Long> 반환
```

**설계 포인트:**
- Phase 1의 FileWordCounter와 동일한 토큰화/집계 로직을 사용한다
- 차이점은 **데이터를 읽어오는 방식만** 다르다 (파일 → DB 쿼리)
- 이것이 저장소 추상화의 핵심이다

**입력/출력:**
```
입력: String tableName (파티션 테이블명)
출력: Map<String, Long> (단어 빈도 맵)
```

### 4.3 DbClusterCoordinator

**책임**: Phase 1의 FileClusterCoordinator와 동일한 구조. Worker만 DB 기반으로 교체.

**처리 과정:**
```
1. 성능 측정 시작
2. Fan-out: 파티션 테이블 수만큼 Virtual Thread Worker 생성
   - 각 Worker에게 담당 테이블명 할당
   - Future<Map<String, Long>> 목록 수집
3. 모든 Worker 완료 대기
4. Fan-in: WordCountAggregator에게 결과 목록 전달
5. 성능 측정 완료
6. AnalysisResult 반환
```

**단일 테이블 모드:**
- 비교를 위해 모든 데이터를 하나의 테이블에 넣고 단일 스레드로 처리하는 모드
- 분산하지 않았을 때의 성능 베이스라인

---

## 5. API 상세

### 5.1 데이터 분산 저장

```
POST /api/v1/db/distribute?partitions=4

Response 200:
{
  "sourceFile": "source.txt",
  "partitionCount": 4,
  "totalRows": 1500000,
  "partitions": [
    { "tableName": "text_partition_0", "rowCount": 375000 },
    { "tableName": "text_partition_1", "rowCount": 375000 },
    { "tableName": "text_partition_2", "rowCount": 375000 },
    { "tableName": "text_partition_3", "rowCount": 375000 }
  ],
  "distributionTimeMs": 3240
}
```

### 5.2 병렬 분석

```
POST /api/v1/db/analyze?threads=4

Response 200:
{
  "topWords": [
    { "word": "the", "count": 15234 },
    { "word": "and", "count": 8921 }
  ],
  "totalUniqueWords": 45678,
  "totalWordCount": 18500000,
  "performance": {
    "totalTimeMs": 312,
    "workerTimesMs": [78, 75, 82, 77],
    "aggregationTimeMs": 12,
    "threadCount": 4,
    "partitionCount": 4
  }
}
```

### 5.3 단일 스레드 분석

```
POST /api/v1/db/analyze-single

Response 200:
{
  "topWords": [...],
  "performance": {
    "totalTimeMs": 1050,
    "workerTimesMs": [1050],
    "aggregationTimeMs": 0,
    "threadCount": 1,
    "partitionCount": 1
  }
}
```

### 5.4 성능 비교

```
GET /api/v1/db/compare

Response 200:
{
  "singleThread": {
    "totalTimeMs": 1050,
    "threadCount": 1
  },
  "multiThread": {
    "totalTimeMs": 312,
    "threadCount": 4,
    "aggregationTimeMs": 12
  },
  "speedupRatio": 3.37,
  "aggregationOverhead": "3.8%"
}
```

---

## 6. Phase 1 vs Phase 2 실험 비교

### 6.1 동일한 데이터로 교차 비교

Phase 1과 Phase 2에서 동일한 원본 데이터를 사용하여 결과를 교차 비교한다.

**비교 항목:**

| 항목 | Phase 1 (파일) | Phase 2 (DB) | 비교 포인트 |
|------|---------------|--------------|------------|
| 분산 저장 시간 | 파일 분할 시간 | 분산 INSERT 시간 | DB INSERT 오버헤드 |
| 단일 스레드 처리 | 파일 순차 읽기 | 단일 테이블 쿼리 | 저장소별 I/O 특성 |
| 4 스레드 병렬 | 파일 병렬 읽기 | 병렬 쿼리 | 병렬화 효율 차이 |
| 통합 시간 | Map merge | Map merge (동일) | 동일해야 함 |
| Speedup Ratio | 파일 기준 | DB 기준 | 저장소에 따른 병렬 효율 |

### 6.2 예상 차이점(실측 아님)

**DB가 파일보다 느릴 수 있는 부분:**
- INSERT 시 인덱스 갱신 비용
- SQL 파싱/실행 계획 수립 오버헤드
- JDBC 커넥션 관리 비용

**DB가 파일보다 빠를 수 있는 부분:**
- 인덱스를 활용한 선택적 조회
- DB 버퍼 캐시에 의한 반복 조회 성능 향상
- 커넥션 풀링에 의한 오버헤드 감소

### 6.3 핵심 확인 사항

**반드시 동일해야 하는 것:**
- 최종 단어 빈도 결과 (정확성 검증)
- Aggregator의 코드 (재사용 확인)
- Fan-out/Fan-in 구조 (패턴의 저장소 독립성)

**달라도 되는 것:**
- 절대적인 처리 시간 (저장소 I/O 특성 차이)
- Speedup Ratio (I/O 패턴에 따라 병렬 효율이 다름)
- Worker 간 시간 편차 (DB 커넥션 풀 경합 등)

---

## 7. 핵심 학습 포인트

### 7.1 저장소 추상화

Phase 1에서 만든 `WordCountAggregator`를 Phase 2에서 그대로 재사용한다. Worker의 출력 형태(`Map<String, Long>`)만 같으면 Aggregator는 데이터가 파일에서 왔든 DB에서 왔든 상관없다. 이것이 클러스터링에서 **저장소 추상화**가 중요한 이유다.

### 7.2 샤딩의 본질

여러 테이블에 데이터를 나눠 저장하고 병렬로 쿼리하는 것이 곧 샤딩의 본질이다. 실제 운영에서는 테이블이 아닌 물리적 서버로 분리되지만, **쿼리를 분산하고 결과를 통합하는 구조**는 동일하다.

### 7.3 DB I/O 특성

파일 I/O와 DB I/O는 특성이 다르다. DB는 커넥션 관리, SQL 파싱, 버퍼 캐시 등 추가 레이어가 있어 단순 비교가 아닌 각 레이어의 비용을 이해하는 것이 중요하다.
