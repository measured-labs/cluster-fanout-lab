# Cluster Simulator — 프로젝트 설계 문서

> 보존 원문(2026-11-18 `docs/archive/`로 옮김). 정리본은 [ADR-000](../adr/ADR-000-fanout-fanin-design.md)이다. 코드가 아직 없어서, 이 문서의 수치(응답 예시의 시간 · 건수 · 비율)는 모두 예상(실측 아님)이다.

## 1. 프로젝트 개요

### 1.1 목적

클러스터링의 핵심 원리인 **Fan-out/Fan-in 패턴**을 한 대의 PC에서 직접 구현하고 체험하는 학습 프로젝트다.

대용량 텍스트 데이터의 단어 빈도 분석을 주제로, 파일 기반(Phase 1)과 DB 기반(Phase 2)을 단계적으로 구현하여 **저장소가 바뀌어도 병렬 처리 구조는 동일함**을 체감한다.

### 1.2 핵심 학습 목표

- Fan-out/Fan-in 패턴의 실제 동작 방식 이해
- 단일 스레드 vs 멀티 스레드 성능 차이 측정
- N+1 구조(Worker + Coordinator/Aggregator)의 필수성 체감
- 저장소 추상화: 파일 → DB로 바뀌어도 동일한 병렬 구조 적용
- Amdahl의 법칙 실체험 (스레드 수 증가 대비 성능 향상 한계)

### 1.3 기술 스택

| 구분 | 상세 |
|------|------|
| Language | Java 21 (Virtual Thread 활용) |
| Framework | Spring Boot 3.5.8 |
| Database | H2 Embedded (별도 설치 불필요) |
| Build | Gradle |
| API | REST API (Spring Web MVC) |

### 1.4 Java 21 Virtual Thread 선택 이유

| 항목 | Platform Thread | Virtual Thread |
|------|----------------|----------------|
| OS 스레드 매핑 | 1:1 | M:N (JVM 관리) |
| 스택 크기 | ~1MB | ~수 KB |
| 생성 가능 수 | 수천 개 | 수백만 개 |
| I/O 대기 시 | OS 스레드 점유 | 자동 yield |
| 클러스터 시뮬레이션 | Worker 수 제한 | Worker 수 자유로움 |

파일 I/O와 DB I/O 모두 **I/O 집약적 작업**이므로 Virtual Thread가 최적이다.

---

## 2. 전체 아키텍처

### 2.1 시스템 구조

```
[REST API Layer]
       │
       ├── /api/v1/file/*   → FileClusterController
       └── /api/v1/db/*     → DbClusterController
                │
                ▼
       [Service Layer]
       ├── Coordinator (스레드 관리, 오케스트레이션)
       ├── Worker (파티션별 독립 처리)
       └── Aggregator (결과 통합) ← 공통 컴포넌트
                │
                ▼
       [Storage Layer]
       ├── Phase 1: File System (txt 파일)
       └── Phase 2: H2 Database (파티션 테이블)
```

### 2.2 공통 처리 흐름

Phase 1과 Phase 2 모두 동일한 흐름을 따른다.

```
1. 데이터 분산 저장
   원본 데이터 → N개의 파티션(파일 또는 테이블)에 분할 저장

2. Fan-out (분산)
   Coordinator가 N개의 Virtual Thread Worker 생성
   각 Worker에게 담당 파티션 할당

3. 병렬 처리
   각 Worker가 독립적으로 자신의 파티션에서 단어 빈도 집계
   결과: Map<String, Long>

4. Fan-in (통합)
   Aggregator가 모든 Worker의 Map을 수집하여 합산
   결과: 최종 단어 빈도 Map

5. 성능 측정
   전체 처리 시간, Worker별 시간, 통합 시간, Speedup Ratio 기록
```

---

## 3. 패키지 구조

```
com.example.clustersimulator/
│
├── config/
│   ├── ThreadPoolConfig.java          // Virtual Thread Executor 설정
│   └── H2Config.java                  // H2 데이터소스 설정
│
├── common/
│   ├── dto/
│   │   ├── AnalysisResult.java        // 분석 결과 (빈도 맵 + 성능 데이터)
│   │   ├── PerformanceResult.java     // 성능 측정 결과
│   │   └── CompareResult.java         // 병렬 vs 단일 비교 결과
│   └── service/
│       └── WordCountAggregator.java   // 결과 통합 (Phase 1, 2 공통)
│
├── file/
│   ├── controller/
│   │   └── FileClusterController.java
│   └── service/
│       ├── FilePartitioner.java       // 파일 분할
│       ├── FileWordCounter.java       // 파일 기반 Worker
│       └── FileClusterCoordinator.java // 파일 기반 Coordinator
│
├── db/
│   ├── controller/
│   │   └── DbClusterController.java
│   ├── service/
│   │   ├── DataDistributor.java       // DB 분산 저장
│   │   ├── DbWordCounter.java         // DB 기반 Worker
│   │   └── DbClusterCoordinator.java  // DB 기반 Coordinator
│   ├── entity/
│   │   └── TextPartition.java         // JPA 엔티티
│   └── repository/
│       └── TextPartitionRepository.java
│
└── ClusterSimulatorApplication.java
```

---

## 4. 컴포넌트 설계

### 4.1 공통 컴포넌트

#### WordCountAggregator

Phase 1, Phase 2 모두에서 재사용되는 핵심 컴포넌트다. 이것이 **저장소에 독립적인 병렬 처리 패턴**을 증명하는 포인트다.

```
역할: 여러 Worker의 Map<String, Long> 결과를 하나로 합산
입력: List<Map<String, Long>> (각 Worker의 결과)
출력: Map<String, Long> (최종 통합 결과)
방식: 동일 키의 값을 합산 (merge 연산)
```

#### PerformanceResult

```
필드:
- totalTimeMs: 전체 처리 시간
- workerTimesMs: 각 Worker의 처리 시간 목록
- aggregationTimeMs: 통합 소요 시간
- threadCount: 사용된 스레드 수
- partitionCount: 파티션 수
- speedupRatio: 단일 대비 속도 향상 비율
```

### 4.2 Phase 1 컴포넌트

| 컴포넌트 | 역할 | 입력 → 출력 |
|----------|------|------------|
| FilePartitioner | 파일 분할 | source.txt → partition_0.txt ~ partition_N.txt |
| FileWordCounter | Worker | partition_X.txt → Map<String, Long> |
| FileClusterCoordinator | Coordinator | N개 Worker 생성/관리 → Aggregator 호출 |

### 4.3 Phase 2 컴포넌트

| 컴포넌트 | 역할 | 입력 → 출력 |
|----------|------|------------|
| DataDistributor | 분산 저장 | source.txt → text_partition_0 ~ text_partition_N 테이블 |
| DbWordCounter | Worker | text_partition_X 테이블 → Map<String, Long> |
| DbClusterCoordinator | Coordinator | N개 Worker 생성/관리 → Aggregator 호출 |

---

## 5. API 설계

### 5.1 Phase 1 — 파일 기반

| Method | Endpoint | 설명 | 파라미터 |
|--------|----------|------|----------|
| POST | `/api/v1/file/upload` | 원본 txt 파일 업로드 | multipart file |
| POST | `/api/v1/file/partition` | 파일을 N개 파티션으로 분할 | `count` (파티션 수) |
| POST | `/api/v1/file/analyze` | 병렬 단어 빈도 분석 | `threads` (스레드 수) |
| POST | `/api/v1/file/analyze-single` | 단일 스레드 분석 (비교용) | — |
| GET | `/api/v1/file/compare` | 병렬 vs 단일 성능 비교 | — |

### 5.2 Phase 2 — DB 기반

| Method | Endpoint | 설명 | 파라미터 |
|--------|----------|------|----------|
| POST | `/api/v1/db/distribute` | 데이터를 N개 테이블에 분산 | `partitions` (테이블 수) |
| POST | `/api/v1/db/analyze` | 병렬 DB 쿼리 분석 | `threads` (스레드 수) |
| POST | `/api/v1/db/analyze-single` | 단일 테이블 단일 스레드 분석 | — |
| GET | `/api/v1/db/compare` | 병렬 vs 단일 성능 비교 | — |

### 5.3 응답 형식 예시 (수치는 예상 · 실측 아님)

```json
{
  "wordFrequency": {
    "the": 15234,
    "and": 8921,
    "to": 7845
  },
  "performance": {
    "totalTimeMs": 245,
    "workerTimesMs": [62, 58, 65, 60],
    "aggregationTimeMs": 12,
    "threadCount": 4,
    "partitionCount": 4,
    "speedupRatio": 3.42
  }
}
```

---

## 6. 성능 측정 전략

### 6.1 측정 항목

| 항목 | 설명 | 활용 |
|------|------|------|
| Total Time | 전체 처리 시간 (ms) | 병렬 vs 단일 비교의 기본 지표 |
| Worker Time | 각 Worker의 처리 시간 | 부하 분배 균형 확인 |
| Aggregation Time | 통합 소요 시간 | Fan-in 비용 시각화 |
| Speedup Ratio | 단일 시간 / 병렬 시간 | 병렬화 효율성 평가 |
| Thread Count | 사용된 스레드 수 | 스레드 수 대비 성능 변화 분석 |

### 6.2 실험 매트릭스

```
파티션 수: [2, 4, 8, 16]
스레드 수: [1, 2, 4, 8, 16]
데이터 크기: [10MB, 50MB, 100MB]
저장소: [파일, DB]
```

### 6.3 예상 결과(실측 아님) 및 학습 포인트

- 스레드 수가 CPU 코어 수를 초과하면 성능 향상이 둔화된다 → **Amdahl의 법칙**
- Aggregation 시간은 파티션 수에 비례하여 증가한다 → **통합 비용은 공짜가 아니다**
- 소규모 데이터에서는 병렬 처리가 오히려 느릴 수 있다 → **스레드 생성 비용 > 처리 비용**
- DB 방식은 파일 방식 대비 I/O 특성이 달라 성능 차이가 발생한다 → **저장소 특성 이해**
- Phase 1과 Phase 2의 Aggregator는 동일하다 → **병렬 패턴의 저장소 독립성**

---

## 7. Phase 간 비교 포인트

| 구분 | Phase 1 (파일) | Phase 2 (DB) |
|------|---------------|--------------|
| 저장소 | 파일 시스템 (.txt) | H2 테이블 |
| 분할 방식 | 행 단위 파일 분할 | 행 단위 테이블 분산 INSERT |
| 읽기 방식 | BufferedReader 파일 I/O | JDBC SELECT 쿼리 |
| Worker 처리 | Java 문자열 토큰화 | SQL GROUP BY 집계 |
| Aggregator | **Map merge (동일)** | **Map merge (동일)** |
| 병렬 구조 | **Fan-out/Fan-in (동일)** | **Fan-out/Fan-in (동일)** |

**Aggregator와 병렬 구조가 동일**한 것이 이 프로젝트의 핵심 학습 포인트다.

---

## 8. 데이터베이스 설계 (Phase 2)

### 8.1 샤딩 시뮬레이션

H2 내장 DB에서 여러 테이블을 생성하여 각각을 독립 노드(샤드)로 취급한다.

### 8.2 테이블 구조

```sql
-- 파티션 테이블 (동적 생성: text_partition_0, text_partition_1, ...)
CREATE TABLE text_partition_{N} (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    line_number BIGINT NOT NULL,
    content     VARCHAR(4000) NOT NULL
);

CREATE INDEX idx_partition_{N}_line ON text_partition_{N}(line_number);
```

### 8.3 분산 전략

라운드 로빈 방식으로 행을 각 파티션 테이블에 분배한다.

```
행 1 → text_partition_0
행 2 → text_partition_1
행 3 → text_partition_2
행 4 → text_partition_3
행 5 → text_partition_0  (다시 처음부터)
...
```

---

## 9. 확장 가능성

본 프로젝트는 Phase 1, 2에 집중하되, 향후 다음과 같이 확장 가능하다.

- Virtual Thread vs Platform Thread 성능 비교 실험
- CompletableFuture 비동기 방식과의 비교
- Aggregator를 계층적 트리 구조로 확장
- WebSocket을 통한 실시간 처리 진행률 모니터링
- 복수 H2 인스턴스로 실제 분산 DB 시뮬레이션
