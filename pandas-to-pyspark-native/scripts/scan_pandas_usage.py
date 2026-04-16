#!/usr/bin/env python3
"""Scan a Python project for pandas-heavy files and common migration hotspots."""

from __future__ import annotations

import argparse
import ast
from collections import Counter, defaultdict
from pathlib import Path
from typing import Iterable


METHOD_NAMES = {
    "apply",
    "assign",
    "drop_duplicates",
    "fillna",
    "groupby",
    "iterrows",
    "itertuples",
    "loc",
    "map",
    "merge",
    "pivot",
    "pivot_table",
    "query",
    "read_csv",
    "read_json",
    "read_parquet",
    "rename",
    "reset_index",
    "sort_values",
    "to_csv",
    "to_dict",
    "to_parquet",
}


def iter_python_files(root: Path) -> Iterable[Path]:
    for path in root.rglob("*.py"):
        if any(part.startswith(".") for part in path.parts):
            continue
        yield path


class PandasVisitor(ast.NodeVisitor):
    def __init__(self) -> None:
        self.aliases: set[str] = set()
        self.hits: Counter[str] = Counter()
        self.lines: dict[str, set[int]] = defaultdict(set)

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            if alias.name == "pandas":
                self.aliases.add(alias.asname or "pandas")
                self.hits["import pandas"] += 1
                self.lines["import pandas"].add(node.lineno)
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if node.module == "pandas":
            self.hits["from pandas import ..."] += 1
            self.lines["from pandas import ..."].add(node.lineno)
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        if isinstance(node.value, ast.Name) and node.value.id in self.aliases:
            if node.attr in METHOD_NAMES:
                key = f"pd.{node.attr}"
                self.hits[key] += 1
                self.lines[key].add(node.lineno)
        elif node.attr in METHOD_NAMES:
            self.hits[node.attr] += 1
            self.lines[node.attr].add(node.lineno)
        self.generic_visit(node)


def scan_file(path: Path) -> tuple[Counter[str], dict[str, set[int]]]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    visitor = PandasVisitor()
    visitor.visit(tree)
    return visitor.hits, visitor.lines


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("target", nargs="?", default=".", help="project directory to scan")
    args = parser.parse_args()

    root = Path(args.target).resolve()
    file_count = 0
    files_with_hits = 0
    totals: Counter[str] = Counter()

    for path in sorted(iter_python_files(root)):
        file_count += 1
        try:
            hits, lines = scan_file(path)
        except (SyntaxError, UnicodeDecodeError) as exc:
            print(f"[skip] {path}: {exc}")
            continue

        if not hits:
            continue

        files_with_hits += 1
        rel = path.relative_to(root)
        print(f"\n## {rel}")
        for name, count in hits.most_common():
            line_list = ", ".join(str(n) for n in sorted(lines[name])[:8])
            print(f"- {name}: {count} hit(s) at line(s) {line_list}")
            totals[name] += count

    print("\n== Summary ==")
    print(f"Scanned files: {file_count}")
    print(f"Files with pandas-like usage: {files_with_hits}")
    if totals:
        for name, count in totals.most_common():
            print(f"- {name}: {count}")
    else:
        print("No pandas-like usage detected.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
