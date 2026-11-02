import ast
from pathlib import Path

import pytest

from glue2dbx import ConvertOptions, convert_source
from glue2dbx.converter import AUTO, MANUAL, REVIEW

EXAMPLES = Path(__file__).parent.parent / "examples" / "glue_jobs"

BOILERPLATE = """
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
"""


def convert(body: str, **opts):
    return convert_source(BOILERPLATE + body + "\njob.commit()\n", ConvertOptions(**opts))


def statuses(result):
    return {f.construct: f.status for f in result.findings}


@pytest.mark.parametrize("path", sorted(EXAMPLES.glob("*.py")), ids=lambda p: p.stem)
def test_every_example_converts_to_valid_glue_free_python(path):
    result = convert_source(path.read_text())
    ast.parse(result.code)  # output must be valid Python
    assert "awsglue" not in result.code
    assert "GlueContext" not in result.code
    assert "job.commit" not in result.code
    assert result.code.startswith("# Databricks notebook source")


def test_boilerplate_is_removed_and_spark_session_added():
    result = convert("df = spark.range(3)")
    assert "spark = SparkSession.builder.getOrCreate()" in result.code
    assert "SparkContext()" not in result.code
    assert "get_job_args(['JOB_NAME'])" in result.code
    assert result.tier == 1


def test_from_catalog_uses_unity_catalog_and_pushdown():
    result = convert(
        'df = glueContext.create_dynamic_frame.from_catalog(database="sales", table_name="orders", '
        "push_down_predicate=\"dt = '2026-01-01'\")",
        catalog="prod",
    )
    assert "spark.table('prod.sales.orders').where(\"dt = '2026-01-01'\")" in result.code


def test_from_catalog_with_dynamic_names_builds_fstring():
    result = convert('df = glueContext.create_dynamic_frame.from_catalog(database=args["db"], table_name="t")')
    assert "spark.table(f\"main.{args['db']}.{'t'}\")" in result.code


def test_s3_csv_read_maps_options_and_paths():
    result = convert(
        "df = glueContext.create_dynamic_frame.from_options(connection_type='s3', "
        "connection_options={'paths': ['s3://bucket/raw/x/']}, format='csv', "
        "format_options={'withHeader': True, 'separator': '|'})",
        path_map=(("s3://bucket/", "/Volumes/main/landing/"),),
    )
    assert ".option('header', True).option('sep', '|')" in result.code
    assert ".load('/Volumes/main/landing/raw/x/')" in result.code


def test_unknown_format_option_is_flagged_manual():
    result = convert(
        "df = glueContext.create_dynamic_frame.from_options(connection_type='s3', "
        "connection_options={'paths': ['s3://b/']}, format='csv', format_options={'optimizePerformance': True})"
    )
    assert result.tier == 3
    assert "raise NotImplementedError('glue2dbx:" in result.code


def test_apply_mapping_becomes_select_with_casts():
    result = convert(
        'out = ApplyMapping.apply(frame=df, mappings=[("id", "string", "id", "long"), ("amt", "string", "amount", "double")])'
    )
    assert (
        "df.select(F.col('id').cast('bigint').alias('id'), F.col('amt').cast('double').alias('amount'))" in result.code
    )


@pytest.mark.parametrize(
    ("lam", "expected"),
    [
        ('lambda r: r["x"] > 5', "F.col('x') > 5"),
        ('lambda r: r["x"] is not None', "F.col('x').isNotNull()"),
        ('lambda r: r["s"] in ["A", "B"]', "F.col('s').isin(['A', 'B'])"),
        ('lambda r: r["s"] not in ("A",)', "~F.col('s').isin(['A'])"),
        ('lambda r: not r["flag"] or r["n"] <= -1', "~F.col('flag') | (F.col('n') <= -1)"),
        ('lambda r: r["code"].startswith("US")', "F.col('code').startswith('US')"),
        ('lambda r: 0 < r["x"] < 10', "(0 < F.col('x')) & (F.col('x') < 10)"),
    ],
)
def test_filter_lambdas_translate_to_column_expressions(lam, expected):
    result = convert(f"out = Filter.apply(frame=df, f={lam})")
    assert statuses(result)["Filter.apply"] == AUTO
    assert ast.unparse(ast.parse(expected)) in result.code


def test_filter_with_named_function_is_manual_and_fails_fast():
    result = convert("def keep(r):\n    return r['x'] > 1\nout = Filter.apply(frame=df, f=keep)")
    assert statuses(result)["Filter.apply"] == MANUAL
    lines = result.code.splitlines()
    raise_at = next(i for i, line in enumerate(lines) if line.startswith("raise NotImplementedError"))
    assert lines[raise_at - 1].startswith("# TODO(glue2dbx)")
    assert lines[raise_at + 1].startswith("out = Filter.apply")


def test_join_is_flagged_for_review():
    result = convert('j = Join.apply(a, b, ["id"], ["account_id"])')
    assert "a.join(b, on=[a['id'] == b['account_id']], how='inner')" in result.code
    assert statuses(result)["Join.apply"] == REVIEW
    assert result.tier == 2


def test_join_on_same_key_names_uses_name_list():
    result = convert('j = Join.apply(frame1=a, frame2=b, keys1=["id"], keys2=["id"])')
    assert "a.join(b, on=['id'], how='inner')" in result.code


def test_resolve_choice_cast_and_unsupported_actions():
    ok = convert('d = ResolveChoice.apply(frame=df, specs=[("qty", "cast:long")])')
    assert "df.withColumn('qty', F.col('qty').cast('bigint'))" in ok.code
    bad = convert('d = ResolveChoice.apply(frame=df, specs=[("qty", "make_struct")])')
    assert statuses(bad)["ResolveChoice.apply"] == MANUAL


def test_rename_drop_select_and_dynamicframe_roundtrip():
    result = convert(
        "a = RenameField.apply(frame=df, old_name='x', new_name='y')\n"
        "b = DropFields.apply(frame=a, paths=['z'])\n"
        "c = SelectFields.apply(frame=b, paths=cols)\n"
        "d = DynamicFrame.fromDF(c.toDF().limit(5), glueContext, 'd')"
    )
    assert "a = df.withColumnRenamed('x', 'y')" in result.code
    assert "b = a.drop('z')" in result.code
    assert "c = b.select(*cols)" in result.code
    assert "d = c.limit(5)" in result.code


def test_writes_become_delta():
    result = convert(
        "glueContext.write_dynamic_frame.from_options(frame=df, connection_type='s3', "
        "connection_options={'path': 's3://b/out/', 'partitionKeys': ['dt']}, format='parquet')\n"
        "glueContext.write_dynamic_frame.from_catalog(frame=df, database='gold', table_name='facts')"
    )
    assert "df.write.format('delta').mode('append').partitionBy('dt').save('s3://b/out/')" in result.code
    assert "df.write.format('delta').mode('append').saveAsTable('main.gold.facts')" in result.code


def test_jdbc_credentials_move_to_secrets():
    result = convert(
        "df = glueContext.create_dynamic_frame.from_options(connection_type='postgresql', "
        "connection_options={'url': 'jdbc:postgresql://h/db', 'dbtable': 't', 'user': 'u', 'password': 'p'})"
    )
    assert "dbutils.secrets.get(scope='glue2dbx', key='password')" in result.code
    assert "'p'" not in result.code
    assert result.tier == 2


def test_unsupported_sources_and_transforms_are_manual():
    result = convert(
        "k = glueContext.create_dynamic_frame.from_options(connection_type='kinesis', connection_options={'streamARN': 'x'})\n"
        "r = Relationalize.apply(frame=k, staging_path='s3://t/', name='root')\n"
        "glueContext.purge_s3_path('s3://t/')"
    )
    manual = [f.construct for f in result.findings if f.status == MANUAL]
    assert manual == ["from_options(kinesis)", "Relationalize.apply", "glueContext.purge_s3_path"]
    assert result.code.count("raise NotImplementedError") == 3


def test_manual_raise_lands_inside_nested_blocks():
    result = convert("for t in names:\n    x = Relationalize.apply(frame=df, staging_path='s3://t/', name=t)")
    ast.parse(result.code)
    assert "    raise NotImplementedError('glue2dbx: Relationalize.apply" in result.code


def test_user_apply_methods_are_left_alone():
    result = convert("out = pandas_frame.apply(len)")
    assert "out = pandas_frame.apply(len)" in result.code
    assert result.tier == 1
