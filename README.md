# pandas-to-pyspark-native-agent

pandas 기반 Python 코드를 Spark-native PySpark 코드로 변환하기 위한 Codex 스킬 저장소입니다.

이 에이전트는 단순 문법 치환이 아니라 아래 목표를 기준으로 동작하도록 설계되어 있습니다.

- `pyspark.sql` 기반의 native 구조 유지
- UDF 사용 금지
- `pyspark.sql.functions as F` 내장 함수 우선 사용
- 대용량 데이터 처리에 맞는 가독성과 유지보수성 확보
- pandas 결과와 PySpark 결과를 비교하는 검증 코드 자동 생성

## 저장소 구성

- [README.md](/Users/joohani/Documents/New%20project/README.md)
  사용 방법과 전체 개요
- [pandas-to-pyspark-native/SKILL.md](/Users/joohani/Documents/New%20project/pandas-to-pyspark-native/SKILL.md)
  Codex가 직접 읽는 스킬 본문
- [pandas-to-pyspark-native/references/migration-rules.md](/Users/joohani/Documents/New%20project/pandas-to-pyspark-native/references/migration-rules.md)
  pandas -> PySpark 변환 규칙, 성능 원칙, 검증 규칙
- [pandas-to-pyspark-native/scripts/scan_pandas_usage.py](/Users/joohani/Documents/New%20project/pandas-to-pyspark-native/scripts/scan_pandas_usage.py)
  프로젝트 내 pandas 사용 위치를 찾는 스캐너
- [pandas-to-pyspark-native/scripts/batch_convert_to_pyspark.py](/Users/joohani/Documents/New%20project/pandas-to-pyspark-native/scripts/batch_convert_to_pyspark.py)
  프로젝트 또는 특정 폴더의 Python 파일을 한 번에 읽어서 변환본을 새 파일로 저장하는 CLI

## 이 에이전트가 하는 일

이 스킬은 pandas 코드가 입력되면 반드시 아래 4개 섹션으로 결과를 생성하도록 설계되어 있습니다.

1. `PySpark Code`
2. `Key Transformation Notes`
3. `Validation Code`
4. `Risk / Edge Cases`

즉, 변환 코드만 주는 것이 아니라 아래까지 함께 제공합니다.

- `apply`, `map`, `groupby`, `merge`가 어떻게 바뀌었는지 설명
- pandas 대비 검증 코드
- row count / aggregation / sample data 비교
- null, 타입, 정렬, 분산 처리 차이에 대한 리스크 정리

## 주요 원칙

- `pyspark.pandas` 또는 호환 계층을 사용하지 않습니다.
- Python UDF, pandas UDF, `rdd.map` 사용을 금지합니다.
- `apply()` 패턴은 Spark-native 표현식으로 치환합니다.
- `withColumn`, `groupBy`, `join`, `Window`, `expr`, `F.*` 내장 함수 중심으로 변환합니다.
- 규칙 기반 모드에서도 `import pyspark.sql.functions as F`, `import pyspark.sql.types as T` 구조를 강제합니다.
- 검증용 소량 데이터 비교를 제외하면 `collect()`를 피합니다.
- deterministic 비교를 위해 정렬 기준 또는 키를 명시합니다.
- 대용량 데이터 처리 가독성을 위해 긴 체인을 중간 DataFrame 단계로 분리합니다.

## 사용 방법

### 0. CLI로 파일을 한 번에 변환하기

이제 이 저장소에는 실제 파일을 읽어서 변환 결과를 새 파일로 저장하는 CLI도 포함되어 있습니다.

중요:

- 이 CLI는 혼합형입니다.
- `OPENAI_API_KEY`가 있으면 AI 변환 모드로 동작합니다.
- `OPENAI_API_KEY`가 없으면 규칙 기반 fallback 모드로 동작합니다.
- 원본 파일을 직접 덮어쓰지 않고, 지정한 출력 폴더에 새 파일을 생성합니다.
- 파일마다 아래 3종 결과물이 생성됩니다.
  - `_pyspark.py`
  - `_validation.py`
  - `_notes.md` 또는 `_notes.json`

모드 요약:

- `auto`
  기본값. API 키가 있으면 AI 변환, 없으면 규칙 기반 변환
- `api`
  OpenAI API만 사용. API 키가 없으면 실패
- `rule`
  API 없이 규칙 기반 변환만 수행

#### 프로젝트 전체 변환

```bash
python3 /Users/joohani/Documents/New\ project/pandas-to-pyspark-native/scripts/batch_convert_to_pyspark.py \
  /변환할/프로젝트 \
  --output-dir /변환결과/출력폴더
```

#### 특정 폴더만 변환

```bash
python3 /Users/joohani/Documents/New\ project/pandas-to-pyspark-native/scripts/batch_convert_to_pyspark.py \
  /변환할/특정폴더 \
  --output-dir /변환결과/출력폴더
```

#### 단일 파일만 변환

```bash
python3 /Users/joohani/Documents/New\ project/pandas-to-pyspark-native/scripts/batch_convert_to_pyspark.py \
  /변환할/파일/example.py \
  --output-dir /변환결과/출력폴더
```

#### AI 모드로 강제 실행

```bash
export OPENAI_API_KEY="YOUR_API_KEY"

python3 /Users/joohani/Documents/New\ project/pandas-to-pyspark-native/scripts/batch_convert_to_pyspark.py \
  /변환할/프로젝트 \
  --output-dir /변환결과/출력폴더 \
  --conversion-mode api
```

#### API 없이 규칙 기반 모드로 강제 실행

```bash
python3 /Users/joohani/Documents/New\ project/pandas-to-pyspark-native/scripts/batch_convert_to_pyspark.py \
  /변환할/프로젝트 \
  --output-dir /변환결과/출력폴더 \
  --conversion-mode rule
```

#### 자주 쓰는 옵션

```bash
python3 /Users/joohani/Documents/New\ project/pandas-to-pyspark-native/scripts/batch_convert_to_pyspark.py \
  /변환할/프로젝트 \
  --output-dir /변환결과/출력폴더 \
  --overwrite \
  --model gpt-5 \
  --notes-format md \
  --max-files 20 \
  --conversion-mode auto
```

옵션 설명:

- `--output-dir`
  변환 결과가 저장될 폴더
- `--overwrite`
  기존 생성 파일이 있어도 덮어쓰기
- `--model`
  사용할 OpenAI 모델. 기본값은 `gpt-5`
- `--notes-format`
  변환 노트 저장 형식. `md` 또는 `json`
- `--max-files`
  테스트용으로 일부 파일만 변환할 때 사용
- `--include-tests`
  기본적으로 제외되는 `tests/`, `test/` 폴더의 파일까지 포함
- `--conversion-mode`
  `auto`, `api`, `rule` 중 선택. 기본값은 `auto`

#### 생성 결과 예시

입력이 아래 파일이면:

```text
/project/src/jobs/sales_report.py
```

출력 폴더 아래에 이런 식으로 생성됩니다.

```text
/변환결과/출력폴더/src/jobs/sales_report_pyspark.py
/변환결과/출력폴더/src/jobs/sales_report_validation.py
/변환결과/출력폴더/src/jobs/sales_report_notes.md
```

#### 동작 방식

1. 입력 경로가 파일이면 그 파일만 변환합니다.
2. 입력 경로가 폴더면 `.py` 파일을 재귀적으로 찾습니다.
3. 숨김 폴더, `.git`, `.venv`, `venv`, `__pycache__`, `node_modules`, `build`, `dist`는 기본 제외합니다.
4. `auto` 모드에서는 API 키가 있으면 OpenAI Responses API를 사용하고, 없으면 규칙 기반 변환을 수행합니다.
5. 결과를 파일별 결과물로 저장합니다.

#### 주의 사항

- 이 CLI는 원본 pandas 파일을 직접 수정하지 않습니다.
- AI 모드는 변환 품질이 높지만 API 키와 네트워크가 필요합니다.
- 규칙 기반 모드는 API 없이도 동작하지만 복잡한 `apply/map/custom logic`은 TODO 주석과 함께 남길 수 있습니다.
- 규칙 기반 모드는 `UDF 금지`, `broadcast join 검토`, `Window 기반 순차 처리`, `null/schema 명시` 체크를 `_notes.md`에 함께 남깁니다.
- 중요한 배치 로직은 생성된 `_validation.py`와 `_notes.md`를 함께 검토하는 것이 좋습니다.
- 프로젝트 전체를 한 번에 돌릴 때는 먼저 `--max-files`로 몇 개 파일만 시험해보는 것을 권장합니다.
- AI 모드에서는 API 호출 비용과 시간이 파일 수에 비례해 증가합니다.

### 1. Codex에서 직접 스킬로 호출하기

이 저장소의 스킬 경로는 아래입니다.

```text
/Users/joohani/Documents/New project/pandas-to-pyspark-native
```

Codex에서 가장 기본적으로는 아래처럼 요청하면 됩니다.

```text
Use $pandas-to-pyspark-native at /Users/joohani/Documents/New project/pandas-to-pyspark-native to convert the following pandas code into Spark-native PySpark and include validation code.

<<<PANDAS_CODE_HERE>>>
```

### 2. 권장 프롬프트 형태

아래처럼 요구사항을 함께 적어주면 더 안정적으로 동작합니다.

```text
Use $pandas-to-pyspark-native at /Users/joohani/Documents/New project/pandas-to-pyspark-native.

Convert the following pandas code to Spark-native PySpark.

Requirements:
- no UDF
- use pyspark.sql.functions as F
- replace all apply() patterns
- keep deterministic ordering
- include validation code
- explain how apply/map/groupby were converted
- highlight null/type/ordering risks

<<<PANDAS_CODE_HERE>>>
```

### 3. 한글 프롬프트 예시

```text
Use $pandas-to-pyspark-native at /Users/joohani/Documents/New project/pandas-to-pyspark-native.

아래 pandas 코드를 Spark-native PySpark 코드로 변환해줘.

조건:
- UDF 금지
- pyspark.sql.functions as F 사용
- apply()는 모두 제거
- 내장함수 중심으로 변환
- Validation Code 반드시 포함
- pandas와 PySpark 결과 비교 코드 포함
- 정렬 기준을 명시해서 deterministic 하게 비교
- null/type/order 차이 리스크도 정리

<<<PANDAS_CODE_HERE>>>
```

## 결과 형식

정상적으로 동작하면 결과는 항상 아래 구조를 따릅니다.

### 1. PySpark Code

- 완전히 변환된 PySpark 코드
- `SparkSession`, `F`, `Window`, `expr` 사용 가능
- Spark-native transformation 중심

### 2. Key Transformation Notes

- 어떤 pandas 로직이 어떤 Spark 패턴으로 바뀌었는지 설명
- 예:
  - `apply(axis=1)` -> `F.when`, `F.expr`, window 함수
  - `map(dict)` -> `F.create_map` 또는 join
  - `groupby().agg()` -> `groupBy().agg()`

### 3. Validation Code

- pandas 결과와 PySpark 결과 비교
- row count check
- aggregation comparison
- sample data comparison
- 가능하면 mismatch row 확인 코드 포함

### 4. Risk / Edge Cases

- null 처리 차이
- 타입 차이
- 순서/정렬 차이
- join cardinality 문제
- timezone / decimal / duplicate key 이슈

## 프로젝트 단위로 사용할 때

실제 프로젝트를 통째로 옮길 때는 먼저 pandas 사용 위치를 찾는 것이 좋습니다.

### pandas 사용 위치 스캔

아래 스크립트로 프로젝트 내부의 pandas 사용 지점을 빠르게 찾을 수 있습니다.

```bash
python3 /Users/joohani/Documents/New\ project/pandas-to-pyspark-native/scripts/scan_pandas_usage.py /변환할/프로젝트/경로
```

프로젝트를 대량 변환할 때 권장 순서는 아래와 같습니다.

1. `scan_pandas_usage.py`로 pandas hotspot을 찾습니다.
2. `batch_convert_to_pyspark.py`를 `--max-files` 옵션과 함께 작은 범위로 먼저 실행합니다.
3. 생성된 `_validation.py`와 `_notes.md`를 확인합니다.
4. 문제가 없으면 대상 범위를 넓혀 전체 프로젝트에 적용합니다.

이 스크립트는 다음 같은 항목을 찾아줍니다.

- `import pandas`
- `pd.read_csv`, `pd.read_parquet`
- `apply`, `groupby`, `merge`, `map`
- `sort_values`, `fillna`, `drop_duplicates`

### 프로젝트 변환 추천 순서

1. 스캐너로 pandas 사용 파일을 찾습니다.
2. I/O가 많은 파일부터 Spark read/write 구조로 바꿉니다.
3. `apply`, row loop, index 의존 로직을 우선 제거합니다.
4. `groupby`, `merge`, `window` 로직을 Spark-native로 재작성합니다.
5. pandas 결과와 비교하는 검증 코드를 붙입니다.
6. 마지막에 null, 타입, 정렬 리스크를 점검합니다.

## 이 에이전트가 특히 잘 다루는 패턴

- `apply(axis=1)` 제거
- `map(dict)`를 `create_map` 또는 join으로 전환
- `groupby().agg()`를 `groupBy().agg()`로 전환
- `merge()`를 `join()`으로 전환
- 누적 합계, 이전 행 참조, 순위 계산을 window 함수로 전환
- 문자열/날짜 파싱을 `F.*` 내장 함수로 전환

## 제한 사항

- 매우 복잡한 Python 로직은 바로 Spark 표현식으로 바꾸기 어려울 수 있습니다.
- 사용자 정의 파싱 함수가 많은 경우 built-in 함수 조합으로 재설계가 필요할 수 있습니다.
- pandas index 의미가 중요한 코드는 명시적 key/order column 설계가 필요합니다.
- 검증 코드는 입력 데이터 규모에 따라 샘플 기반 비교가 적절할 수 있습니다.

## 참고 문서

- 스킬 본문: [pandas-to-pyspark-native/SKILL.md](/Users/joohani/Documents/New%20project/pandas-to-pyspark-native/SKILL.md)
- 변환 규칙: [pandas-to-pyspark-native/references/migration-rules.md](/Users/joohani/Documents/New%20project/pandas-to-pyspark-native/references/migration-rules.md)

## 빠른 시작

가장 짧게는 아래 프롬프트 하나로 시작하면 됩니다.

```text
Use $pandas-to-pyspark-native at /Users/joohani/Documents/New project/pandas-to-pyspark-native.

아래 pandas 코드를 PySpark native 코드로 바꾸고, 변환 노트와 validation code, risk까지 함께 작성해줘.

<<<PANDAS_CODE_HERE>>>
```
