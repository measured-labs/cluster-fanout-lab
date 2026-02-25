# Cluster Simulator

한 대의 PC에서 클러스터링의 핵심 원리(Fan-out/Fan-in)를 직접 실험하는 학습 프로젝트

## 프로젝트 소개

클러스터링은 결국 **데이터를 나누고, 병렬로 처리하고, 결과를 합치는 것**이다. 이 프로젝트는 대용량 텍스트의 단어 빈도 분석을 주제로, 파일 기반과 DB 기반 두 가지 방식으로 이 구조를 구현하여 **저장소가 바뀌어도 병렬 처리 패턴은 동일함**을 직접 체험한다.

```
[Client 요청]
      │
      ▼
 [Coordinator]
  /    |    \
 ▼     ▼     ▼
[W1] [W2]  [W3]    ← Fan-out (병렬 처리)
 \    |    /
  ▼   ▼   ▼
 [Aggregator]       ← Fan-in (통합)
      │
      ▼
 [최종 결과]
```

## 기술 스택

| 구분 | 기술 |
|------|------|
| Language | Java 21 (Virtual Thread) |
| Framework | Spring Boot 3.5.8 |
| Database | H2 Embedded |
| Build | Gradle |

## 프로젝트 구조

```
cluster-simulator/
├── docs/                              # 문서
│   ├── 01_clustering_fundamentals.md  # 클러스터링 기본 개념
│   ├── 02_project_design.md           # 프로젝트 설계 문서
│   ├── 03_phase1_file_based.md        # Phase 1 상세
│   └── 04_phase2_db_based.md          # Phase 2 상세
│
├── src/main/java/com/example/clustersimulator/
│   ├── config/                        # 설정
│   ├── common/                        # 공통 컴포넌트
│   │   ├── dto/                       # AnalysisResult, PerformanceResult
│   │   └── service/                   # WordCountAggregator
│   ├── file/                          # Phase 1: 파일 기반
│   │   ├── controller/
│   │   └── service/
│   ├── db/                            # Phase 2: DB 기반
│   │   ├── controller/
│   │   ├── service/
│   │   ├── entity/
│   │   └── repository/
│   └── ClusterSimulatorApplication.java
│
├── data/
│   ├── source/                        # 원본 txt 파일
│   └── partitions/                    # 분할된 파티션 파일
│
├── build.gradle
└── README.md
```

## Phase 구성

### Phase 1 — 파일 기반 병렬 처리

대용량 txt 파일을 N개의 파티션 파일로 분할하고 병렬로 읽어 단어 빈도를 분석한다.

```
source.txt → [partition_0.txt, partition_1.txt, ...] → 병렬 읽기 → 결과 통합
```

**API:**

| Method | Endpoint | 설명 |
|--------|----------|------|
| POST | `/api/v1/file/upload` | 원본 파일 업로드 |
| POST | `/api/v1/file/partition?count=4` | 4개 파티션으로 분할 |
| POST | `/api/v1/file/analyze?threads=4` | 4 스레드 병렬 분석 |
| POST | `/api/v1/file/analyze-single` | 단일 스레드 분석 |
| GET | `/api/v1/file/compare` | 성능 비교 |

### Phase 2 — DB 기반 병렬 처리

동일한 데이터를 N개의 파티션 테이블에 분산 저장하고 병렬 쿼리로 분석한다.

```
source.txt → [table_0, table_1, ...] → 병렬 쿼리 → 결과 통합
```

**API:**

| Method | Endpoint | 설명 |
|--------|----------|------|
| POST | `/api/v1/db/distribute?partitions=4` | 4개 테이블에 분산 저장 |
| POST | `/api/v1/db/analyze?threads=4` | 4 스레드 병렬 쿼리 |
| POST | `/api/v1/db/analyze-single` | 단일 스레드 분석 |
| GET | `/api/v1/db/compare` | 성능 비교 |

## 핵심 학습 포인트

### 1. N+1 구조의 필수성

병렬 처리에서는 N개의 Worker + 1개의 Coordinator/Aggregator가 반드시 필요하다. 호출자가 통합 역할을 겸해도 그 비용은 어딘가에서 반드시 발생한다.

### 2. 저장소 독립적인 병렬 패턴

Phase 1(파일)과 Phase 2(DB)에서 `WordCountAggregator`가 동일하게 재사용된다. Fan-out/Fan-in 구조는 저장소에 독립적이다.

### 3. 통합 비용은 공짜가 아니다

스레드 수를 늘릴수록 처리는 빨라지지만 통합(Aggregation) 비용도 증가한다. Amdahl의 법칙에 따라 순차 처리 부분이 전체 성능의 한계를 결정한다.

### 4. 병렬이 항상 빠른 것은 아니다

소규모 데이터에서는 스레드 생성 비용이 처리 비용을 초과할 수 있다. CPU 코어 수를 넘는 스레드는 성능 향상에 기여하지 못한다.

## 실행 방법

### 사전 요구사항

- Java 21+
- Gradle 8+

### 빌드 및 실행

```bash
# 빌드
./gradlew build

# 실행
./gradlew bootRun

# 또는
java -jar build/libs/cluster-simulator-0.0.1-SNAPSHOT.jar
```

서버 기동 후 `http://localhost:8080`에서 API를 사용할 수 있다.

### 실행 시나리오

#### Phase 1 실험

```bash
# 1. 테스트용 txt 파일 업로드
curl -X POST -F "file=@large_text.txt" http://localhost:8080/api/v1/file/upload

# 2. 4개 파티션으로 분할
curl -X POST "http://localhost:8080/api/v1/file/partition?count=4"

# 3. 4 스레드로 병렬 분석
curl -X POST "http://localhost:8080/api/v1/file/analyze?threads=4"

# 4. 단일 스레드로 분석 (비교용)
curl -X POST "http://localhost:8080/api/v1/file/analyze-single"

# 5. 성능 비교
curl http://localhost:8080/api/v1/file/compare
```

#### Phase 2 실험

```bash
# 1. 4개 테이블에 분산 저장
curl -X POST "http://localhost:8080/api/v1/db/distribute?partitions=4"

# 2. 4 스레드로 병렬 쿼리
curl -X POST "http://localhost:8080/api/v1/db/analyze?threads=4"

# 3. 단일 스레드 분석
curl -X POST "http://localhost:8080/api/v1/db/analyze-single"

# 4. 성능 비교
curl http://localhost:8080/api/v1/db/compare
```

#### 스레드 수별 성능 변화 실험

```bash
# 스레드 수를 변경하며 반복 실험
for threads in 1 2 4 8 16; do
  echo "=== Threads: $threads ==="
  curl -s -X POST "http://localhost:8080/api/v1/file/analyze?threads=$threads" | jq '.performance'
done
```

## 문서 목록

| 문서 | 내용 |
|------|------|
| [클러스터링 기본 개념](docs/01_clustering_fundamentals.md) | 컴퓨터 동작 방식, 클러스터링 원리, 장치별 클러스터링 방식 |
| [프로젝트 설계](docs/02_project_design.md) | 전체 아키텍처, 패키지 구조, 컴포넌트 설계, API 설계, 성능 측정 전략 |
| [Phase 1 상세](docs/03_phase1_file_based.md) | 파일 기반 병렬 처리 아키텍처, 컴포넌트 상세, API 상세 |
| [Phase 2 상세](docs/04_phase2_db_based.md) | DB 기반 병렬 처리 아키텍처, 샤딩 설계, Phase 1과의 비교 |

## 라이선스

학습 목적 프로젝트
