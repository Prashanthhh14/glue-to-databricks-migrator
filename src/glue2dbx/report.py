"""Turn conversion findings into migration reports (Markdown and CSV)."""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass

from .converter import AUTO, MANUAL, REVIEW, ConversionResult

TIER_LABELS = {
    1: "Tier 1 - lift and shift",
    2: "Tier 2 - convert and review",
    3: "Tier 3 - re-engineer",
}
STATUS_LABELS = {AUTO: "auto", REVIEW: "review", MANUAL: "MANUAL"}


@dataclass
class JobAssessment:
    name: str
    lines_of_code: int
    result: ConversionResult

    @property
    def tier(self) -> int:
        return self.result.tier


def job_report(job: JobAssessment) -> str:
    r = job.result
    lines = [
        f"# Migration report: `{job.name}`",
        "",
        f"**{TIER_LABELS[job.tier]}** | {job.lines_of_code} lines | "
        f"{r.count(AUTO)} auto, {r.count(REVIEW)} review, {r.count(MANUAL)} manual",
        "",
        "| Glue line | Construct | Status | Note |",
        "|---|---|---|---|",
    ]
    for f in r.findings:
        lines.append(f"| {f.line} | `{f.construct}` | {STATUS_LABELS[f.status]} | {f.note} |")
    return "\n".join(lines) + "\n"


def assessment_report(jobs: list[JobAssessment]) -> str:
    total = len(jobs)
    by_tier = {t: [j for j in jobs if j.tier == t] for t in (1, 2, 3)}
    constructs = sum(len(j.result.findings) for j in jobs)
    automated = sum(j.result.count(AUTO) + j.result.count(REVIEW) for j in jobs)
    rate = (automated / constructs * 100) if constructs else 100.0

    lines = [
        "# Glue to Databricks migration assessment",
        "",
        f"- **Jobs scanned:** {total}",
        f"- **Glue constructs found:** {constructs} ({rate:.0f}% converted automatically)",
    ]
    for tier, tier_jobs in by_tier.items():
        lines.append(f"- **{TIER_LABELS[tier]}:** {len(tier_jobs)}")
    lines += [
        "",
        "Suggested wave plan: migrate Tier 1 first to prove the pattern, then Tier 2 with a reviewer, "
        "and schedule Tier 3 as small re-engineering stories.",
        "",
        "| Job | Lines | Auto | Review | Manual | Tier |",
        "|---|---|---|---|---|---|",
    ]
    for j in sorted(jobs, key=lambda j: (j.tier, j.name)):
        r = j.result
        lines.append(
            f"| `{j.name}` | {j.lines_of_code} | {r.count(AUTO)} | {r.count(REVIEW)} | {r.count(MANUAL)} | {j.tier} |"
        )

    open_items = [(j.name, f) for j in jobs for f in j.result.findings if f.status in (REVIEW, MANUAL)]
    if open_items:
        lines += [
            "",
            "## Items needing a human",
            "",
            "| Job | Glue line | Construct | Status | What to do |",
            "|---|---|---|---|---|",
        ]
        for name, f in open_items:
            lines.append(f"| `{name}` | {f.line} | `{f.construct}` | {STATUS_LABELS[f.status]} | {f.note} |")
    return "\n".join(lines) + "\n"


def inventory_csv(jobs: list[JobAssessment]) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["job", "lines_of_code", "tier", "auto", "review", "manual"])
    for j in sorted(jobs, key=lambda j: j.name):
        r = j.result
        writer.writerow([j.name, j.lines_of_code, j.tier, r.count(AUTO), r.count(REVIEW), r.count(MANUAL)])
    return buf.getvalue()
