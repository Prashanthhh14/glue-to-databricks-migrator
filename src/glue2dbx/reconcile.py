"""Prove a migrated pipeline produces the same data as the original.

Runs three layers of checks between a *source* DataFrame (the Glue output) and
a *target* DataFrame (the Databricks output):

1. Row counts
2. Schema: missing / extra columns and type differences
3. Content: an order-independent checksum per column, null counts, and
   (when key columns are given) a row-level diff of missing, extra and
   changed rows

Everything is computed with Spark aggregates, so it scales to large tables
without collecting data to the driver.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

NULL_TOKEN = "\u0000<null>"


@dataclass
class ColumnCheck:
    column: str
    source_nulls: int
    target_nulls: int
    source_checksum: str
    target_checksum: str

    @property
    def passed(self) -> bool:
        return self.source_nulls == self.target_nulls and self.source_checksum == self.target_checksum


@dataclass
class ReconResult:
    source_count: int
    target_count: int
    missing_columns: list[str] = field(default_factory=list)
    extra_columns: list[str] = field(default_factory=list)
    type_mismatches: list[tuple[str, str, str]] = field(default_factory=list)
    column_checks: list[ColumnCheck] = field(default_factory=list)
    keys: list[str] = field(default_factory=list)
    missing_in_target: int = 0
    extra_in_target: int = 0
    changed_rows: int = 0
    sample_changed_keys: list[dict] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return (
            self.source_count == self.target_count
            and not self.missing_columns
            and not self.extra_columns
            and not self.type_mismatches
            and all(c.passed for c in self.column_checks)
            and self.missing_in_target == 0
            and self.extra_in_target == 0
            and self.changed_rows == 0
        )

    def to_markdown(self, title: str = "Reconciliation report") -> str:
        icon = lambda ok: "PASS" if ok else "FAIL"  # noqa: E731
        lines = [
            f"## {title}: {icon(self.passed)}",
            "",
            "| Check | Source | Target | Result |",
            "|---|---|---|---|",
            f"| Row count | {self.source_count:,} | {self.target_count:,} | {icon(self.source_count == self.target_count)} |",
            f"| Missing columns | | {', '.join(self.missing_columns) or '-'} | {icon(not self.missing_columns)} |",
            f"| Extra columns | | {', '.join(self.extra_columns) or '-'} | {icon(not self.extra_columns)} |",
        ]
        for col, src, tgt in self.type_mismatches:
            lines.append(f"| Type of `{col}` | {src} | {tgt} | FAIL |")
        for check in self.column_checks:
            lines.append(
                f"| `{check.column}` nulls / checksum | {check.source_nulls} / {check.source_checksum[:12]} "
                f"| {check.target_nulls} / {check.target_checksum[:12]} | {icon(check.passed)} |"
            )
        if self.keys:
            lines += [
                f"| Rows missing in target (by {', '.join(self.keys)}) | | {self.missing_in_target:,} | {icon(self.missing_in_target == 0)} |",
                f"| Unexpected rows in target | | {self.extra_in_target:,} | {icon(self.extra_in_target == 0)} |",
                f"| Rows with changed values | | {self.changed_rows:,} | {icon(self.changed_rows == 0)} |",
            ]
        if self.sample_changed_keys:
            lines += ["", "Sample keys with changed values:", ""]
            lines += [f"- `{row}`" for row in self.sample_changed_keys]
        return "\n".join(lines) + "\n"


def _as_text(column: str):
    return F.coalesce(F.col(f"`{column}`").cast("string"), F.lit(NULL_TOKEN))


def _profile(df: DataFrame, columns: list[str]) -> dict:
    """One pass: row count, plus null count and order-independent checksum per column."""
    aggs = [F.count(F.lit(1)).alias("__rows")]
    for i, col in enumerate(columns):
        aggs.append(F.sum(F.col(f"`{col}`").isNull().cast("long")).alias(f"n{i}"))
        aggs.append(F.sum(F.xxhash64(_as_text(col)).cast("decimal(38,0)")).alias(f"h{i}"))
    return df.agg(*aggs).first().asDict()


def _row_hash(columns: list[str]):
    return F.xxhash64(*[_as_text(c) for c in columns]) if columns else F.lit(0)


def reconcile(
    source: DataFrame,
    target: DataFrame,
    keys: list[str] | None = None,
    exclude: list[str] | None = None,
    sample_size: int = 5,
) -> ReconResult:
    """Compare source vs target and return a ReconResult (check `.passed`)."""
    exclude = set(exclude or [])
    keys = list(keys or [])
    src_types = {f.name: f.dataType.simpleString() for f in source.schema.fields if f.name not in exclude}
    tgt_types = {f.name: f.dataType.simpleString() for f in target.schema.fields if f.name not in exclude}

    common = [c for c in src_types if c in tgt_types]
    result = ReconResult(
        source_count=0,
        target_count=0,
        missing_columns=[c for c in src_types if c not in tgt_types],
        extra_columns=[c for c in tgt_types if c not in src_types],
        type_mismatches=[(c, src_types[c], tgt_types[c]) for c in common if src_types[c] != tgt_types[c]],
        keys=keys,
    )

    src_profile, tgt_profile = _profile(source, common), _profile(target, common)
    result.source_count, result.target_count = src_profile["__rows"], tgt_profile["__rows"]
    for i, col in enumerate(common):
        result.column_checks.append(
            ColumnCheck(
                column=col,
                source_nulls=int(src_profile[f"n{i}"] or 0),
                target_nulls=int(tgt_profile[f"n{i}"] or 0),
                source_checksum=str(src_profile[f"h{i}"] or 0),
                target_checksum=str(tgt_profile[f"h{i}"] or 0),
            )
        )

    if keys:
        missing_keys = [k for k in keys if k not in common]
        if missing_keys:
            raise ValueError(f"Key columns not present in both frames: {missing_keys}")
        values = [c for c in common if c not in keys]
        src = source.select(*[F.col(f"`{k}`") for k in keys], _row_hash(values).alias("__h"))
        tgt = target.select(*[F.col(f"`{k}`") for k in keys], _row_hash(values).alias("__h"))
        result.missing_in_target = src.join(tgt, on=keys, how="left_anti").count()
        result.extra_in_target = tgt.join(src, on=keys, how="left_anti").count()
        changed = src.alias("s").join(tgt.alias("t"), on=keys, how="inner").where(F.col("s.__h") != F.col("t.__h"))
        result.changed_rows = changed.count()
        if result.changed_rows:
            result.sample_changed_keys = [r.asDict() for r in changed.select(*keys).limit(sample_size).collect()]
    return result
