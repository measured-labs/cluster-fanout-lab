# Cluster Simulator

## 상태와 다음 할 일

- 질문: 데이터를 N개로 나눠 병렬로 처리하고 합칠 때, 스레드 수 · 파티션 수 · 저장소(파일, DB)에 따라 전체 시간과 합치는 비용이 어떻게 달라지는가.
- 상태: **예정**. 설계 문서만 있고 코드 · 빌드 파일 · 실행 결과 기록은 아직 없다. 이 README의 API · 실행 방법 · 구조는 설계안이다. 설계는 [ADR-000](docs/adr/ADR-000-fanout-fanin-design.md)에 정리했고 원문은 [docs/archive/](docs/archive/)에 있다.
- 수치: 이 저장소의 수치(응답 예시의 `totalTimeMs`, `speedupRatio` 등)는 모두 예상(실측 아님)이다.
- 다음 할 일:
  1. DB Worker의 집계 위치(SQL 집계, 애플리케이션 집계)를 정한다([ADR-000 §4](docs/adr/ADR-000-fanout-fanin-design.md#4-선택지)).
  2. 파일 방식부터 만들고, 병렬 결과와 단일 스레드 결과의 단어 빈도가 같은지 먼저 확인한다.
  3. 측정 조건과 원본을 저장소에 남긴 뒤 결과를 이 절에 적는다.

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

> 설계안이다. 지금 저장소에는 `docs/`와 이 README만 있다. `src/` · `data/` · `build.gradle`은 아직 없다([ADR-000 §6](docs/adr/ADR-000-fanout-fanin-design.md#6-아직-없는-것)).

```
cluster-simulator/
├── docs/                              # 문서
│   ├── 01_clustering_fundamentals.md  # 클러스터링 기본 개념
│   ├── adr/ADR-000-fanout-fanin-design.md  # 설계 · 범위 정리
│   └── archive/                       # 설계 원문(02 설계, 03 Phase 1, 04 Phase 2)
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

## 핵심 학습 포인트 (예상 · 실측 아님)

> 코드가 아직 없어서, 아래는 설계 의도와 만든 뒤 확인할 예상이다.

### 1. N+1 구조의 필수성

설계 의도: 병렬 처리에는 N개의 Worker와 1개의 Coordinator/Aggregator가 필요하다고 본다. 호출자가 통합 역할을 겸해도 그 비용은 어딘가에서 생길 것으로 예상한다.

### 2. 저장소 독립적인 병렬 패턴

설계 의도: Phase 1(파일)과 Phase 2(DB)에서 `WordCountAggregator`를 그대로 재사용한다. Fan-out/Fan-in 구조가 저장소와 상관없이 같은지는 만든 뒤 확인한다.

### 3. 통합 비용은 공짜가 아니다

예상: 스레드 수를 늘리면 처리는 빨라지겠지만 통합(Aggregation) 비용도 늘어날 것이다. Amdahl의 법칙대로라면 순차 처리 부분이 전체 성능의 한계를 정한다. 측정으로 확인할 항목이다.

### 4. 병렬이 항상 빠른 것은 아니다

예상: 데이터가 작으면 스레드 생성 비용이 처리 비용보다 클 수 있다. CPU 코어 수를 넘는 스레드는 성능 향상이 둔화될 것으로 본다. 측정으로 확인할 항목이다.

## 실행 방법

> 아직 없음. 빌드 파일과 코드가 없어서 아래 명령은 지금 동작하지 않는다. 설계안으로 남겨 둔다.

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
| [ADR-000 설계 · 범위](docs/adr/ADR-000-fanout-fanin-design.md) | 설계 요약, 범위, 정하지 않은 선택지, 아직 없는 것, 원문 대응표 |
| [프로젝트 설계 (원문)](docs/archive/02_project_design.md) | 전체 아키텍처, 패키지 구조, 컴포넌트 설계, API 설계, 성능 측정 전략 |
| [Phase 1 상세 (원문)](docs/archive/03_phase1_file_based.md) | 파일 기반 병렬 처리 아키텍처, 컴포넌트 상세, API 상세 |
| [Phase 2 상세 (원문)](docs/archive/04_phase2_db_based.md) | DB 기반 병렬 처리 아키텍처, 샤딩 설계, Phase 1과의 비교 |

## 라이선스

학습 목적 프로젝트
