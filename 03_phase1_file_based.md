# Phase 1 — 파일 기반 병렬 처리

## 1. 개요

Phase 1은 클러스터링의 가장 기본적인 형태를 파일 시스템 위에서 구현한다.

대용량 txt 파일을 여러 개의 파티션 파일로 분할하고, 각 파티션을 별도의 Virtual Thread Worker가 병렬로 읽어 단어 빈도를 집계한 뒤, Aggregator가 결과를 통합한다.

**이 Phase에서 체험할 것:**
- 데이터를 물리적으로 분산 저장하는 것의 의미
- 병렬 읽기가 왜 빠른지 (I/O 병목 분산)
- 통합(Fan-in) 비용이 실제로 얼마나 드는지
- 스레드 수를 늘릴수록 항상 빨라지는 것은 아닌 이유

---

## 2. 아키텍처

### 2.1 전체 흐름

```
[원본 txt 파일]
       │
       ▼
[FilePartitioner] ─── 파일 분할 ───→ partition_0.txt
                                      partition_1.txt
                                      partition_2.txt
                                      partition_3.txt
       │
       ▼
[FileClusterCoordinator]
       │
       ├── [Worker 0] → partition_0.txt 읽기 → Map<String, Long>
       ├── [Worker 1] → partition_1.txt 읽기 → Map<String, Long>
       ├── [Worker 2] → partition_2.txt 읽기 → Map<String, Long>
       └── [Worker 3] → partition_3.txt 읽기 → Map<String, Long>
       │
       ▼
[WordCountAggregator] ─── 결과 합산 ───→ 최종 Map<String, Long>
       │
       ▼
[AnalysisResult + PerformanceResult]
```

### 2.2 컴포넌트 상세

| 컴포넌트 | 클래스명 | 역할 |
|----------|----------|------|
| Splitter | `FilePartitioner` | 원본 파일을 N개 파티션 파일로 분할 |
| Worker | `FileWordCounter` | 단일 파티션 파일에서 단어 빈도 집계 |
| Coordinator | `FileClusterCoordinator` | Worker 생성/관리, Aggregator 호출, 성능 측정 |
| Aggregator | `WordCountAggregator` | 모든 Worker 결과를 합산 (공통 컴포넌트) |

---

## 3. 컴포넌트 상세 설계

### 3.1 FilePartitioner

**책임**: 원본 txt 파일을 N개의 파티션 파일로 균등 분할한다.

**분할 전략**: 행(line) 단위 라운드 로빈

```
원본 파일 (10줄, 파티션 3개):
  Line 1 → partition_0.txt
  Line 2 → partition_1.txt
  Line 3 → partition_2.txt
  Line 4 → partition_0.txt
  Line 5 → partition_1.txt
  ...
```

**설계 포인트:**
- 행 단위 분할이므로 단어가 잘리지 않는다
- 라운드 로빈으로 각 파티션의 크기가 대략 균등하다
- 분할된 파일은 `data/partitions/` 디렉토리에 저장된다

**입력/출력:**
```
입력: source.txt (원본 파일 경로), partitionCount (분할 수)
출력: List<Path> (생성된 파티션 파일 경로 목록)
```

### 3.2 FileWordCounter

**책임**: 하나의 파티션 파일을 읽어 단어 빈도를 집계한다. Worker 역할.

**처리 과정:**
```
1. BufferedReader로 파티션 파일 열기
2. 각 줄(line)을 읽으며:
   a. 소문자 변환
   b. 특수문자 제거
   c. 공백 기준 토큰화
   d. 각 단어의 빈도를 Map에 누적
3. 완성된 Map<String, Long> 반환
```

**설계 포인트:**
- 각 Worker는 완전히 독립적으로 동작한다 (공유 상태 없음)
- 결과는 Worker 로컬 Map에 저장되므로 동기화가 불필요하다
- 이것이 **메시지 전달 방식**에 해당한다 (공유 메모리가 아님)

**입력/출력:**
```
입력: Path (파티션 파일 경로)
출력: Map<String, Long> (단어 빈도 맵)
```

### 3.3 FileClusterCoordinator

**책임**: 전체 병렬 처리를 오케스트레이션한다. Fan-out과 Fan-in을 관리한다.

**처리 과정:**
```
1. 성능 측정 시작 (System.nanoTime)
2. Fan-out: 파티션 수만큼 Virtual Thread Worker 생성
   - ExecutorService.newVirtualThreadPerTaskExecutor() 사용
   - 각 Worker에게 담당 파티션 파일 할당
   - Future<Map<String, Long>> 목록 수집
3. 모든 Worker 완료 대기 (join)
4. Fan-in: WordCountAggregator에게 결과 목록 전달
5. 성능 측정 완료
6. AnalysisResult 반환
```

**설계 포인트:**
- Coordinator가 Aggregator를 겸한다 (동기 방식)
- 모든 Worker가 끝날 때까지 기다린 후 통합한다
- 이것이 N+1 구조에서 +1의 역할이다

**단일 스레드 모드:**
- 비교를 위해 모든 파티션을 순차적으로 처리하는 모드도 제공한다
- 동일한 Aggregator를 사용하되 Worker를 하나의 스레드에서 순차 실행한다

---

## 4. API 상세

### 4.1 파일 업로드

```
POST /api/v1/file/upload
Content-Type: multipart/form-data

Body: file (txt 파일)

Response 200:
{
  "fileName": "source.txt",
  "fileSize": "104857600",
  "lineCount": 1500000,
  "message": "파일 업로드 완료"
}
```

### 4.2 파일 파티셔닝

```
POST /api/v1/file/partition?count=4

Response 200:
{
  "originalFile": "source.txt",
  "partitionCount": 4,
  "partitions": [
    { "name": "partition_0.txt", "lineCount": 375000, "sizeBytes": 26214400 },
    { "name": "partition_1.txt", "lineCount": 375000, "sizeBytes": 26214400 },
    { "name": "partition_2.txt", "lineCount": 375000, "sizeBytes": 26214400 },
    { "name": "partition_3.txt", "lineCount": 375000, "sizeBytes": 26214400 }
  ]
}
```

### 4.3 병렬 분석

```
POST /api/v1/file/analyze?threads=4

Response 200:
{
  "topWords": [
    { "word": "the", "count": 15234 },
    { "word": "and", "count": 8921 },
    { "word": "to", "count": 7845 }
  ],
  "totalUniqueWords": 45678,
  "totalWordCount": 18500000,
  "performance": {
    "totalTimeMs": 245,
    "workerTimesMs": [62, 58, 65, 60],
    "aggregationTimeMs": 12,
    "threadCount": 4,
    "partitionCount": 4
  }
}
```

### 4.4 단일 스레드 분석

```
POST /api/v1/file/analyze-single

Response 200:
{
  "topWords": [...],
  "totalUniqueWords": 45678,
  "totalWordCount": 18500000,
  "performance": {
    "totalTimeMs": 838,
    "workerTimesMs": [838],
    "aggregationTimeMs": 0,
    "threadCount": 1,
    "partitionCount": 1
  }
}
```

### 4.5 성능 비교

```
GET /api/v1/file/compare

Response 200:
{
  "singleThread": {
    "totalTimeMs": 838,
    "threadCount": 1
  },
  "multiThread": {
    "totalTimeMs": 245,
    "threadCount": 4,
    "aggregationTimeMs": 12
  },
  "speedupRatio": 3.42,
  "aggregationOverhead": "4.9%"
}
```

---

## 5. 디렉토리 구조

```
data/
├── source/
│   └── source.txt              # 업로드된 원본 파일
└── partitions/
    ├── partition_0.txt          # 분할된 파티션 파일
    ├── partition_1.txt
    ├── partition_2.txt
    └── partition_3.txt
```

---

## 6. 핵심 학습 포인트

### 6.1 파일 I/O에서의 병렬 처리 이점

단일 파일을 하나의 스레드로 읽으면 디스크 I/O가 병목이 된다. 여러 파일로 나누고 동시에 읽으면 OS의 I/O 스케줄러가 읽기 요청을 효율적으로 처리할 수 있다.

다만 SSD 환경에서는 디스크 자체가 병렬 읽기를 지원하므로 효과가 크지만, HDD에서는 디스크 헤드가 물리적으로 하나이므로 병렬 읽기의 이점이 제한적일 수 있다. 이 차이도 실험을 통해 확인할 수 있다.

### 6.2 Worker 독립성의 중요성

각 Worker가 자신만의 로컬 Map을 가지고 독립적으로 처리하기 때문에 Lock이 불필요하다. 만약 하나의 공유 ConcurrentHashMap을 사용했다면 경합이 발생하여 오히려 성능이 나빠질 수 있다.

### 6.3 통합 비용의 실체

Aggregation 시간을 별도로 측정함으로써, 병렬 처리의 이점이 통합 비용에 의해 얼마나 상쇄되는지를 수치로 확인할 수 있다.

### 6.4 Phase 2로의 연결

이 Phase에서 구현한 `WordCountAggregator`는 Phase 2에서 그대로 재사용된다. Worker만 파일 기반에서 DB 기반으로 교체되고, Coordinator의 구조도 동일하다. 이것이 **클러스터링의 저장소 독립성**이다.
