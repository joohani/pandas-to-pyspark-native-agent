#!/usr/bin/env python3
"""Batch-convert pandas Python files to Spark-native PySpark.

This tool supports two modes:
1. AI mode via the OpenAI Responses API when an API key is available
2. Rule-based fallback mode when no API key is available
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


DEFAULT_MODEL = "gpt-5"
DEFAULT_API_URL = "https://api.openai.com/v1/responses"
DEFAULT_EXCLUDE_DIRS = {
    ".git",
    ".hg",
    ".svn",
    ".venv",
    "venv",
    "__pycache__",
    "build",
    "dist",
    "node_modules",
}

SYSTEM_PROMPT = """You are an expert PySpark engineer.

Your task is to convert pandas code into PySpark (Spark-native) code AND generate validation code.

# OUTPUT (MANDATORY STRUCTURE)

## 1. PySpark Code
- Fully converted PySpark version
- Use pyspark.sql.functions as F
- NO UDF unless absolutely unavoidable
- Use Spark-native transformations (withColumn, groupBy, Window, expr)
- Maintain logic equivalence

## 2. Key Transformation Notes
- Explain how apply/map/groupby were converted
- Highlight any assumptions

## 3. Validation Code
- Compare pandas vs PySpark results
- Row count check
- Aggregation comparison
- Sample data comparison

## 4. Risk / Edge Cases
- Null handling differences
- Type differences
- Ordering issues

# STRICT RULES
- DO NOT use UDF unless unavoidable
- Replace all apply() patterns
- Avoid collect() unless used in validation with small data
- Use column expressions only
- Ensure deterministic output

# PERFORMANCE RULES
- Avoid unnecessary shuffle
- Use broadcast join if needed
- Minimize wide transformations

# VALIDATION RULES
- Use assert or comparison logic
- Highlight mismatched rows if possible
"""


JSON_SCHEMA = {
    "name": "pyspark_conversion_bundle",
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "pyspark_code": {"type": "string"},
            "transformation_notes": {
                "type": "array",
                "items": {"type": "string"},
            },
            "validation_code": {"type": "string"},
            "risk_edge_cases": {
                "type": "array",
                "items": {"type": "string"},
            },
        },
        "required": [
            "pyspark_code",
            "transformation_notes",
            "validation_code",
            "risk_edge_cases",
        ],
    },
    "strict": True,
}


@dataclass
class ConversionResult:
    pyspark_code: str
    transformation_notes: list[str]
    validation_code: str
    risk_edge_cases: list[str]
    mode: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read one Python file or a directory tree of Python files and generate "
            "Spark-native PySpark conversion outputs into a separate output directory."
        )
    )
    parser.add_argument(
        "input_path",
        help="Path to a single .py file or a directory containing Python files",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        required=True,
        help="Directory where converted files will be written",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help=f"OpenAI model to use. Default: {DEFAULT_MODEL}",
    )
    parser.add_argument(
        "--api-url",
        default=DEFAULT_API_URL,
        help=f"Responses API URL. Default: {DEFAULT_API_URL}",
    )
    parser.add_argument(
        "--api-key-env",
        default="OPENAI_API_KEY",
        help="Environment variable name containing the OpenAI API key",
    )
    parser.add_argument(
        "--notes-format",
        choices=("md", "json"),
        default="md",
        help="Format for transformation notes/risk output. Default: md",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing generated files",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="Process at most N Python files after filtering",
    )
    parser.add_argument(
        "--include-tests",
        action="store_true",
        help="Include files under test directories instead of skipping them",
    )
    parser.add_argument(
        "--temperature",
        type=float,
        default=0.2,
        help="Sampling temperature for the API request. Default: 0.2",
    )
    parser.add_argument(
        "--reasoning-effort",
        choices=("minimal", "low", "medium", "high"),
        default="medium",
        help="Reasoning effort passed to the model. Default: medium",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.0,
        help="Optional delay between file conversions",
    )
    parser.add_argument(
        "--conversion-mode",
        choices=("auto", "api", "rule"),
        default="auto",
        help="Conversion mode. 'auto' uses API when available, otherwise rule-based fallback.",
    )
    return parser.parse_args()


def is_python_file(path: Path) -> bool:
    return path.is_file() and path.suffix == ".py"


def iter_python_files(root: Path, include_tests: bool, output_dir: Path) -> Iterable[Path]:
    for path in sorted(root.rglob("*.py")):
        if output_dir in path.parents:
            continue
        if any(part in DEFAULT_EXCLUDE_DIRS for part in path.parts):
            continue
        if not include_tests and any(part in {"tests", "test"} for part in path.parts):
            continue
        if any(part.startswith(".") for part in path.parts):
            continue
        yield path


def discover_files(input_path: Path, include_tests: bool, output_dir: Path) -> list[Path]:
    if is_python_file(input_path):
        return [input_path]
    if input_path.is_dir():
        return list(iter_python_files(input_path, include_tests, output_dir))
    raise FileNotFoundError(f"Input path is not a Python file or directory: {input_path}")


def build_user_prompt(source_path: Path, source_code: str) -> str:
    return f"""Now convert the following pandas code.

Source path: {source_path}

Return valid JSON matching the provided schema.

<<<PANDAS_CODE_HERE>>>
{source_code}
"""


def extract_response_text(response_json: dict) -> str:
    output = response_json.get("output", [])
    texts: list[str] = []
    for item in output:
        for content in item.get("content", []):
            text = content.get("text")
            if text:
                texts.append(text)
    if texts:
        return "\n".join(texts)
    raise ValueError("No text content found in API response")


def request_conversion(
    *,
    api_url: str,
    api_key: str,
    model: str,
    temperature: float,
    reasoning_effort: str,
    source_path: Path,
    source_code: str,
) -> ConversionResult:
    payload = {
        "model": model,
        "instructions": SYSTEM_PROMPT,
        "input": build_user_prompt(source_path, source_code),
        "reasoning": {"effort": reasoning_effort},
        "text": {
            "format": {
                "type": "json_schema",
                "name": JSON_SCHEMA["name"],
                "schema": JSON_SCHEMA["schema"],
                "strict": JSON_SCHEMA["strict"],
            }
        },
        "temperature": temperature,
    }
    request = urllib.request.Request(
        api_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"API request failed for {source_path}: {exc.code} {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"API connection failed for {source_path}: {exc.reason}") from exc

    response_json = json.loads(body)
    response_text = extract_response_text(response_json)
    bundle = json.loads(response_text)
    return ConversionResult(
        pyspark_code=bundle["pyspark_code"].rstrip() + "\n",
        transformation_notes=list(bundle["transformation_notes"]),
        validation_code=bundle["validation_code"].rstrip() + "\n",
        risk_edge_cases=list(bundle["risk_edge_cases"]),
        mode="api",
    )


def replace_imports(source_code: str) -> tuple[str, list[str]]:
    notes: list[str] = []
    updated = source_code
    spark_import_block = (
        "from pyspark.sql import SparkSession\n"
        "import pyspark.sql.functions as F\n"
        "import pyspark.sql.types as T\n"
        "from pyspark.sql.window import Window"
    )
    if "import pandas as pd" in updated:
        updated = updated.replace(
            "import pandas as pd",
            spark_import_block,
        )
        notes.append("`import pandas as pd`를 `SparkSession`, `F`, `T`, `Window` import 블록으로 치환했습니다.")
    if "from pandas import" in updated:
        updated = re.sub(
            r"from pandas import .+",
            spark_import_block,
            updated,
        )
        notes.append("`from pandas import ...` 구문을 Spark import로 치환했습니다.")
    if "import pyspark.sql.functions as F" not in updated:
        updated = spark_import_block + "\n\n" + updated
        notes.append("파일 상단에 `F`, `T`, `Window`를 포함한 Spark import 블록을 추가했습니다.")
    if "SparkSession.builder" not in updated:
        updated = (
            spark_import_block
            + '\n\nspark = SparkSession.builder.appName("pandas-to-pyspark-native").getOrCreate()\n\n'
            + updated.removeprefix(spark_import_block).lstrip("\n")
        )
        notes.append("파일 상단에 기본 SparkSession 초기화 코드를 추가했습니다.")
    return updated, notes


def apply_basic_rules(source_code: str) -> tuple[str, list[str], list[str]]:
    code, notes = replace_imports(source_code)
    risks: list[str] = []
    replacements = [
        (r"\bpd\.read_csv\(", "spark.read.csv(", "`pd.read_csv()`를 `spark.read.csv()`로 치환했습니다."),
        (r"\bpd\.read_parquet\(", "spark.read.parquet(", "`pd.read_parquet()`를 `spark.read.parquet()`로 치환했습니다."),
        (r"\.to_parquet\(", ".write.parquet(", "`.to_parquet()`를 `.write.parquet()`로 치환했습니다."),
        (r"\.to_csv\(", ".write.csv(", "`.to_csv()`를 `.write.csv()`로 치환했습니다."),
        (r"\.merge\(", ".join(", "`.merge()`를 `.join()` 형태로 치환했습니다."),
        (r"\.groupby\(", ".groupBy(", "`.groupby()`를 `.groupBy()`로 치환했습니다."),
        (r"\.sort_values\(", ".orderBy(", "`.sort_values()`를 `.orderBy()`로 치환했습니다."),
        (r"\.drop_duplicates\(", ".dropDuplicates(", "`.drop_duplicates()`를 `.dropDuplicates()`로 치환했습니다."),
        (r"\.fillna\(", ".fillna(", "`.fillna()`는 이름이 동일해 그대로 Spark DataFrame API로 대응시켰습니다."),
        (r"\.assign\(", ".withColumn(", "`.assign()`를 `.withColumn()` 기반 변환 후보로 치환했습니다."),
    ]
    for pattern, replacement, note in replacements:
        new_code, count = re.subn(pattern, replacement, code)
        if count:
            code = new_code
            notes.append(note)

    code, astype_count = re.subn(
        r"([A-Za-z_][A-Za-z0-9_]*(?:\[[^\]]+\]|\.[A-Za-z_][A-Za-z0-9_]*))\.astype\(\s*str\s*\)",
        r"\1.cast(T.StringType())",
        code,
    )
    if astype_count:
        notes.append("`.astype(str)` 패턴을 `cast(T.StringType())`로 치환했습니다.")

    code, filter_count = re.subn(
        r"([A-Za-z_][A-Za-z0-9_]*)\[\s*\1\[([\"'][^\"']+[\"'])\]\s*([=!><]{1,2})\s*([^\]]+)\]",
        r"\1.filter(F.col(\2) \3 \4)",
        code,
    )
    if filter_count:
        notes.append("기본적인 boolean indexing 패턴을 `.filter(F.col(...))`로 치환했습니다.")

    code, arithmetic_count = re.subn(
        r"(?m)^([A-Za-z_][A-Za-z0-9_]*)\[([\"'][^\"']+[\"'])\]\s*=\s*\1\[([\"'][^\"']+[\"'])\]\s*\+\s*([0-9]+(?:\.[0-9]+)?)\s*$",
        r"\1 = \1.withColumn(\2, F.col(\3) + F.lit(\4))",
        code,
    )
    if arithmetic_count:
        notes.append("`df['A'] = df['B'] + 1` 형태의 기본 산술 대입을 `withColumn`으로 치환했습니다.")

    if re.search(r"\.apply\s*\(", code):
        code = re.sub(
            r"(?m)^([^\n]*\.apply\s*\([^\n]*)$",
            r"# TODO: manual rewrite required for apply()\n# \1",
            code,
        )
        notes.append("`apply()`는 자동 규칙 기반으로 안전 치환이 어려워 TODO 주석으로 표시했습니다. 수동 변환 시 `F.when/otherwise`, `F.expr`, window 함수만 사용해야 합니다.")
        risks.append("`apply()` 패턴은 반드시 `F.when`, `F.expr`, `regexp_*`, `to_date`, window 함수 등 Spark native 표현식으로 재작성해야 합니다.")

    if re.search(r"\.map\s*\(", code):
        code = re.sub(
            r"(?m)^([^\n]*\.map\s*\([^\n]*)$",
            r"# TODO: manual rewrite required for map()\n# \1",
            code,
        )
        notes.append("복잡한 `map()` 패턴은 자동 규칙 기반으로 안전 치환이 어려워 TODO 주석으로 표시했습니다.")
        risks.append("`map()`은 작은 매핑이면 `F.create_map`, 큰 매핑이면 `F.broadcast(small_df)` 또는 일반 join으로 재작성해야 합니다.")

    udf_patterns = [
        r"\budf\s*\(",
        r"@udf\b",
        r"\bpandas_udf\b",
        r"\.rdd\.",
        r"\.map\s*\(",
    ]
    if any(re.search(pattern, code) for pattern in udf_patterns):
        risks.append("UDF/RDD 기반 패턴이 감지되었습니다. 이 표준에서는 `udf`, `pandas_udf`, `rdd.map` 사용이 금지되므로 반드시 내장 함수 또는 join/window로 재작성해야 합니다.")

    if ".join(" in code:
        notes.append("join 로직은 작은 참조 테이블일 경우 `F.broadcast(small_df)` 적용 여부를 검토해야 합니다.")
        risks.append("join 대상 중 작은 reference table이 있으면 `F.broadcast()`를 적용해 shuffle을 줄일 수 있는지 확인해야 합니다.")

    if re.search(r"\bshift\s*\(|\brolling\s*\(|\bdiff\s*\(", source_code):
        notes.append("`shift/rolling/diff` 계열 패턴은 Window 기반 재작성 대상입니다.")
        risks.append("순차 데이터 처리 로직은 `Window.partitionBy(...).orderBy(...)`를 명시해서 `lag`, `lead`, 누적 집계 등으로 수동 전환해야 합니다.")

    if "fillna(" in source_code or "isna(" in source_code or "isnull(" in source_code:
        notes.append("결측치 처리 패턴에 대해 Spark의 `fillna`/`coalesce` 사용을 우선하도록 가이드했습니다.")
        risks.append("Spark는 null과 타입에 엄격하므로 `fillna`, `coalesce`, cast를 명시적으로 점검해야 합니다.")

    if "explode(" in source_code or "from_json(" in source_code or "json_normalize(" in source_code:
        risks.append("JSON/Array explode 계열 로직은 `StructType`/`ArrayType` 등 명시적 스키마를 먼저 정의해야 안전합니다.")

    action_patterns = [
        ("collect(", "`collect()`"),
        ("count(", "`count()`"),
        ("toPandas(", "`toPandas()`"),
        ("show(", "`show()`"),
    ]
    for raw, label in action_patterns:
        if raw in code:
            risks.append(f"{label} 는 transformation 중간 단계에서 사용하면 병목/OOM을 유발할 수 있으므로 제거 또는 검증 전용으로 제한해야 합니다.")

    if "axis=1" in code:
        risks.append("row-wise `axis=1` 로직은 Spark native 표현식으로 재설계가 필요할 수 있습니다.")
    if "reset_index" in code:
        risks.append("pandas index 관련 의미는 Spark에서 사라지므로 explicit key/order 컬럼이 필요할 수 있습니다.")
    if ".show(" in source_code or ".count(" in source_code or ".collect(" in source_code or ".toPandas(" in source_code:
        notes.append("중간 action 호출은 성능 병목이 될 수 있으므로 transformation 단계에서 제거 또는 축소해야 합니다.")

    notes.append("규칙 기반 fallback 모드로 변환했습니다. 이 모드는 UDF 금지, F/T import, action 최소화, broadcast 검토, window/null/schema 점검 기준을 함께 남깁니다.")
    return code, notes, risks


def build_rule_validation(source_path: Path) -> str:
    stem = source_path.stem
    return f'''"""Validation scaffold for {stem}."""

# Fill in the concrete pandas and PySpark data loading paths before running.
import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T


spark = SparkSession.builder.appName("validate-{stem}").getOrCreate()


def load_pandas_df():
    raise NotImplementedError("Load the original pandas result here.")


def load_pyspark_df():
    raise NotImplementedError("Load the converted PySpark result here.")


def main():
    pandas_df = load_pandas_df()
    pyspark_df = load_pyspark_df()

    pandas_count = len(pandas_df)
    pyspark_count = pyspark_df.count()
    assert pandas_count == pyspark_count, f"Row count mismatch: {{pandas_count}} != {{pyspark_count}}"

    # Replace metric columns and sort keys for your dataset.
    metric_columns = []
    sort_keys = []

    if metric_columns:
        pandas_agg = pandas_df[metric_columns].sum(numeric_only=False).to_dict()
        pyspark_agg_row = pyspark_df.agg(
            *[F.sum(F.col(col)).alias(col) for col in metric_columns]
        ).collect()[0]
        pyspark_agg = pyspark_agg_row.asDict()
        assert pandas_agg == pyspark_agg, f"Aggregation mismatch: {{pandas_agg}} != {{pyspark_agg}}"

    if sort_keys:
        pandas_sample = pandas_df.sort_values(sort_keys).head(20).to_dict("records")
        pyspark_sample = [
            row.asDict()
            for row in pyspark_df.orderBy(*sort_keys).limit(20).collect()
        ]
        assert pandas_sample == pyspark_sample, "Sample data mismatch detected"


if __name__ == "__main__":
    main()
'''


def rule_based_conversion(source_path: Path, source_code: str) -> ConversionResult:
    converted_code, notes, risks = apply_basic_rules(source_code)
    validation_code = build_rule_validation(source_path)
    if not risks:
        risks = [
            "규칙 기반 변환은 복잡한 로직의 의미 보존을 완전히 보장하지 않습니다.",
            "null/type/ordering 차이는 validation 코드로 반드시 확인해야 합니다.",
        ]
    return ConversionResult(
        pyspark_code=converted_code.rstrip() + "\n",
        transformation_notes=notes,
        validation_code=validation_code.rstrip() + "\n",
        risk_edge_cases=risks,
        mode="rule",
    )


def relative_output_base(input_root: Path, source_path: Path, output_dir: Path) -> Path:
    if input_root.is_file():
        return output_dir / source_path.stem
    return (output_dir / source_path.relative_to(input_root)).with_suffix("")


def render_notes_markdown(source_path: Path, result: ConversionResult) -> str:
    lines = [
        f"# Conversion Notes: {source_path.name}",
        "",
        f"- Mode: `{result.mode}`",
        "",
        "## Key Transformation Notes",
        "",
    ]
    for note in result.transformation_notes:
        lines.append(f"- {note}")
    lines.extend(["", "## Risk / Edge Cases", ""])
    for risk in result.risk_edge_cases:
        lines.append(f"- {risk}")
    lines.append("")
    return "\n".join(lines)


def render_notes_json(result: ConversionResult) -> str:
    payload = {
        "mode": result.mode,
        "transformation_notes": result.transformation_notes,
        "risk_edge_cases": result.risk_edge_cases,
    }
    return json.dumps(payload, ensure_ascii=False, indent=2) + "\n"


def write_output(path: Path, content: str, overwrite: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not overwrite:
        raise FileExistsError(f"Refusing to overwrite existing file: {path}")
    path.write_text(content, encoding="utf-8")


def convert_files(args: argparse.Namespace) -> int:
    input_path = Path(args.input_path).resolve()
    output_dir = Path(args.output_dir).resolve()
    api_key = os.environ.get(args.api_key_env)
    use_api = args.conversion_mode == "api" or (
        args.conversion_mode == "auto" and bool(api_key)
    )
    if args.conversion_mode == "api" and not api_key:
        raise RuntimeError(
            f"Environment variable {args.api_key_env} is required when --conversion-mode=api"
        )

    files = discover_files(input_path, args.include_tests, output_dir)
    if args.max_files is not None:
        files = files[: args.max_files]
    if not files:
        print("No Python files found to convert.", file=sys.stderr)
        return 1

    print(f"Discovered {len(files)} file(s) to convert.")
    print(f"Conversion mode: {'api' if use_api else 'rule'}")
    failures: list[tuple[Path, str]] = []

    for index, path in enumerate(files, start=1):
        print(f"[{index}/{len(files)}] Converting {path}")
        try:
            source_code = path.read_text(encoding="utf-8")
            if use_api:
                result = request_conversion(
                    api_url=args.api_url,
                    api_key=api_key,
                    model=args.model,
                    temperature=args.temperature,
                    reasoning_effort=args.reasoning_effort,
                    source_path=path,
                    source_code=source_code,
                )
            else:
                result = rule_based_conversion(path, source_code)
            base = relative_output_base(input_path, path, output_dir)
            write_output(base.with_name(base.name + "_pyspark.py"), result.pyspark_code, args.overwrite)
            write_output(
                base.with_name(base.name + "_validation.py"),
                result.validation_code,
                args.overwrite,
            )
            notes_content = (
                render_notes_markdown(path, result)
                if args.notes_format == "md"
                else render_notes_json(result)
            )
            notes_suffix = ".md" if args.notes_format == "md" else ".json"
            write_output(base.with_name(base.name + "_notes" + notes_suffix), notes_content, args.overwrite)
        except Exception as exc:  # noqa: BLE001
            failures.append((path, str(exc)))
            print(f"  FAILED: {exc}", file=sys.stderr)
        if args.sleep_seconds > 0:
            time.sleep(args.sleep_seconds)

    print("")
    print(f"Converted successfully: {len(files) - len(failures)}")
    print(f"Failed: {len(failures)}")
    if failures:
        print("")
        print("Failure details:")
        for path, message in failures:
            print(f"- {path}: {message}")
        return 2
    return 0


def main() -> int:
    args = parse_args()
    return convert_files(args)


if __name__ == "__main__":
    raise SystemExit(main())
