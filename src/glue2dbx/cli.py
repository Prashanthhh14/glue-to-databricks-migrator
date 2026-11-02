"""Command-line interface: `glue2dbx assess | convert | reconcile`."""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import click

from .converter import ConvertOptions, convert_source
from .report import JobAssessment, assessment_report, inventory_csv, job_report


def _glue_scripts(source: Path) -> list[Path]:
    if source.is_file():
        return [source]
    return sorted(p for p in source.rglob("*.py") if p.is_file())


def _assess(paths: list[Path], options: ConvertOptions) -> list[tuple[Path, JobAssessment]]:
    jobs = []
    for path in paths:
        text = path.read_text(encoding="utf-8")
        opts = ConvertOptions(options.catalog, options.target_format, options.path_map, path.name)
        result = convert_source(text, opts)
        loc = sum(1 for line in text.splitlines() if line.strip() and not line.strip().startswith("#"))
        jobs.append((path, JobAssessment(path.stem, loc, result)))
    return jobs


def _parse_path_map(values: tuple[str, ...]) -> tuple[tuple[str, str], ...]:
    pairs = []
    for value in values:
        if "=" not in value:
            raise click.BadParameter(f"expected OLD=NEW, got {value!r}", param_hint="--path-map")
        old, new = value.split("=", 1)
        pairs.append((old, new))
    return tuple(pairs)


def _format(path: Path) -> None:
    ruff = shutil.which("ruff")
    if ruff:
        subprocess.run([ruff, "format", "--quiet", str(path)], check=False)


common_options = [
    click.option("--catalog", default="main", show_default=True, help="Unity Catalog name for Glue catalog tables."),
    click.option("--target-format", default="delta", show_default=True, help="Format for rewritten sinks."),
    click.option(
        "--path-map",
        multiple=True,
        metavar="OLD=NEW",
        help="Rewrite path prefixes, e.g. s3://bucket/=/Volumes/main/raw/",
    ),
]


def with_common(func):
    for option in reversed(common_options):
        func = option(func)
    return func


@click.group()
@click.version_option(package_name="glue2dbx")
def main() -> None:
    """Assess, convert and reconcile AWS Glue jobs moving to Databricks."""


@main.command()
@click.argument("source", type=click.Path(exists=True, path_type=Path))
@click.option("--report", type=click.Path(path_type=Path), help="Write the Markdown assessment here.")
@click.option("--csv", "csv_path", type=click.Path(path_type=Path), help="Write a CSV inventory here.")
@with_common
def assess(source: Path, report: Path | None, csv_path: Path | None, catalog, target_format, path_map) -> None:
    """Scan Glue jobs and tier them by migration effort (nothing is written unless asked)."""
    options = ConvertOptions(catalog, target_format, _parse_path_map(path_map))
    jobs = [job for _, job in _assess(_glue_scripts(source), options)]
    text = assessment_report(jobs)
    if report:
        report.parent.mkdir(parents=True, exist_ok=True)
        report.write_text(text, encoding="utf-8")
        click.echo(f"Assessment written to {report}")
    else:
        click.echo(text)
    if csv_path:
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        csv_path.write_text(inventory_csv(jobs), encoding="utf-8")
        click.echo(f"Inventory written to {csv_path}")


@main.command()
@click.argument("source", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--out", "out_dir", required=True, type=click.Path(path_type=Path), help="Folder for converted jobs and reports."
)
@click.option("--no-format", is_flag=True, help="Skip formatting output with ruff.")
@with_common
def convert(source: Path, out_dir: Path, no_format: bool, catalog, target_format, path_map) -> None:
    """Convert Glue jobs into Databricks PySpark, with a report per job."""
    options = ConvertOptions(catalog, target_format, _parse_path_map(path_map))
    jobs = _assess(_glue_scripts(source), options)
    (out_dir / "reports").mkdir(parents=True, exist_ok=True)
    for path, job in jobs:
        target = out_dir / path.name
        target.write_text(job.result.code, encoding="utf-8")
        if not no_format:
            _format(target)
        (out_dir / "reports" / f"{path.stem}.md").write_text(job_report(job), encoding="utf-8")
        click.echo(f"[tier {job.tier}] {path.name} -> {target}")
    (out_dir / "reports" / "ASSESSMENT.md").write_text(assessment_report([j for _, j in jobs]), encoding="utf-8")
    click.echo(f"Converted {len(jobs)} job(s). Reports in {out_dir / 'reports'}")


@main.command()
@click.option("--source", "source_path", required=True, help="Path or table name of the original (Glue) output.")
@click.option("--target", "target_path", required=True, help="Path or table name of the migrated (Databricks) output.")
@click.option("--source-format", default="parquet", show_default=True, help="parquet, delta, csv, json, or 'table'.")
@click.option("--target-format", default="delta", show_default=True, help="parquet, delta, csv, json, or 'table'.")
@click.option("--keys", default="", help="Comma-separated key columns for a row-level diff.")
@click.option("--exclude", default="", help="Comma-separated columns to ignore (e.g. load timestamps).")
@click.option("--report", type=click.Path(path_type=Path), help="Write the Markdown result here.")
def reconcile(source_path, target_path, source_format, target_format, keys, exclude, report) -> None:
    """Check that migrated output matches the original. Exits 1 on any mismatch."""
    from pyspark.sql import SparkSession

    from .reconcile import reconcile as run_reconcile

    spark = SparkSession.builder.appName("glue2dbx-reconcile").getOrCreate()

    def load(path: str, fmt: str):
        if fmt == "table":
            return spark.table(path)
        reader = spark.read.format(fmt)
        if fmt == "csv":
            reader = reader.option("header", True).option("inferSchema", True)
        return reader.load(path)

    split = lambda s: [x.strip() for x in s.split(",") if x.strip()]  # noqa: E731
    result = run_reconcile(
        load(source_path, source_format), load(target_path, target_format), split(keys), split(exclude)
    )
    text = result.to_markdown()
    if report:
        report.parent.mkdir(parents=True, exist_ok=True)
        report.write_text(text, encoding="utf-8")
    click.echo(text)
    sys.exit(0 if result.passed else 1)


if __name__ == "__main__":
    main()
