"""Rewrite AWS Glue PySpark scripts into Databricks-ready PySpark.

The converter parses a Glue job into a Python AST, swaps every Glue-specific
construct (GlueContext, DynamicFrame reads/writes, built-in transforms) for
its plain Spark / Delta Lake equivalent, and records a finding for each one.

Findings have three statuses:

* ``auto``   - converted with equivalent semantics, nothing to check
* ``review`` - converted, but a human should confirm a semantic detail
* ``manual`` - no safe automatic rewrite; the generated script raises
               ``NotImplementedError`` at that point so it can never run
               with silently wrong results
"""

from __future__ import annotations

import ast
import copy
from dataclasses import dataclass, field

AUTO, REVIEW, MANUAL = "auto", "review", "manual"

# Glue transforms with no safe 1:1 rewrite, and what to do instead.
MANUAL_TRANSFORMS = {
    "Relationalize": "Flattens nested data into several tables. Rewrite with explode() and struct field selects.",
    "Map": "Runs a Python function per record. Rewrite as column expressions (avoid Python UDFs).",
    "FlatMap": "Runs a Python function per record. Rewrite as column expressions or explode().",
    "Unbox": "Parses a string column into a struct. Rewrite with from_json() and an explicit schema.",
    "SplitFields": "Returns a DynamicFrameCollection. Rewrite as two select() calls.",
    "SplitRows": "Returns a DynamicFrameCollection. Rewrite as two filter() calls.",
    "Spigot": "Samples records to S3 for debugging. Drop it or use df.limit(n).write.",
    "ErrorsAsDynamicFrame": "Glue error records have no Spark equivalent. Add explicit data-quality checks.",
}

GLUE_TO_SPARK_TYPES = {"long": "bigint", "short": "smallint", "byte": "tinyint"}

CSV_OPTION_MAP = {
    "withHeader": "header",
    "separator": "sep",
    "quoteChar": "quote",
    "escaper": "escape",
    "multiLine": "multiLine",
}
JSON_OPTION_MAP = {"multiline": "multiLine", "multiLine": "multiLine"}

JDBC_TYPES = {"jdbc", "mysql", "postgresql", "oracle", "sqlserver", "redshift"}

HELPERS = {
    "drop_null_columns": '''
def drop_null_columns(df):
    """Drop columns whose values are all null (what Glue's DropNullFields does)."""
    counts = df.select([F.count(F.col(f"`{c}`")).alias(c) for c in df.columns]).first()
    return df.drop(*[c for c in df.columns if counts[c] == 0])
''',
    "get_job_args": '''
def get_job_args(names):
    """Read job parameters from Databricks widgets, falling back to --NAME value CLI args."""
    args = {}
    for name in names:
        try:
            args[name] = dbutils.widgets.get(name)  # noqa: F821 - provided by Databricks
        except Exception:
            args[name] = _cli_arg(name)
    return args


def _cli_arg(name):
    flag = f"--{name}"
    argv = sys.argv[1:]
    for i, token in enumerate(argv):
        if token == flag and i + 1 < len(argv):
            return argv[i + 1]
        if token.startswith(flag + "="):
            return token.split("=", 1)[1]
    return os.environ.get(name)
''',
}

MANUAL_MARKER = "glue2dbx:"


@dataclass(frozen=True)
class ConvertOptions:
    """Settings for one conversion run."""

    catalog: str = "main"
    target_format: str = "delta"
    path_map: tuple[tuple[str, str], ...] = ()
    source_name: str = "<string>"


@dataclass
class Finding:
    construct: str
    line: int
    status: str
    note: str


@dataclass
class ConversionResult:
    code: str
    findings: list[Finding] = field(default_factory=list)

    def count(self, status: str) -> int:
        return sum(1 for f in self.findings if f.status == status)

    @property
    def tier(self) -> int:
        """1 = lift and shift, 2 = convert and review, 3 = needs re-engineering."""
        if self.count(MANUAL):
            return 3
        if self.count(REVIEW):
            return 2
        return 1


class _Untranslatable(Exception):
    pass


# --------------------------------------------------------------------------- helpers


def _dotted(node: ast.AST) -> str:
    parts: list[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    elif isinstance(node, ast.Call):
        parts.append(_dotted(node.func) + "()")
    else:
        parts.append("?")
    return ".".join(reversed(parts))


def _arg(call: ast.Call, name: str, pos: int | None = None) -> ast.expr | None:
    for kw in call.keywords:
        if kw.arg == name:
            return kw.value
    if pos is not None and len(call.args) > pos:
        return call.args[pos]
    return None


def _literal(node: ast.AST | None):
    if node is None:
        raise _Untranslatable
    try:
        return ast.literal_eval(node)
    except (ValueError, TypeError, SyntaxError, MemoryError, RecursionError) as exc:
        raise _Untranslatable from exc


def _dict_items(node: ast.AST | None) -> dict[str, ast.expr] | None:
    if not isinstance(node, ast.Dict):
        return None
    items: dict[str, ast.expr] = {}
    for key, value in zip(node.keys, node.values, strict=True):
        if not (isinstance(key, ast.Constant) and isinstance(key.value, str)):
            return None
        items[key.value] = value
    return items


def _expr(src: str) -> ast.expr:
    return ast.parse(src, mode="eval").body


def _method(obj: ast.expr, name: str, args=(), keywords=()) -> ast.Call:
    return ast.Call(
        func=ast.Attribute(value=obj, attr=name, ctx=ast.Load()),
        args=list(args),
        keywords=list(keywords),
    )


def _const(value) -> ast.Constant:
    return ast.Constant(value=value)


def _spark_type(glue_type: str) -> str:
    return GLUE_TO_SPARK_TYPES.get(glue_type, glue_type)


class _LambdaToColumn:
    """Translate a simple Glue Filter lambda into a Spark Column expression."""

    CMP = {ast.Eq: "==", ast.NotEq: "!=", ast.Lt: "<", ast.LtE: "<=", ast.Gt: ">", ast.GtE: ">="}
    BIN = {ast.Add: "+", ast.Sub: "-", ast.Mult: "*", ast.Div: "/", ast.Mod: "%"}

    def __init__(self, argname: str):
        self.argname = argname

    def convert(self, node: ast.AST) -> str:
        if isinstance(node, ast.Subscript) and isinstance(node.value, ast.Name):
            if node.value.id == self.argname:
                key = node.slice
                if isinstance(key, ast.Constant) and isinstance(key.value, str):
                    return f"F.col({key.value!r})"
                raise _Untranslatable
            return ast.unparse(node)
        if isinstance(node, ast.Constant):
            return repr(node.value)
        if isinstance(node, ast.Name):
            if node.id == self.argname:
                raise _Untranslatable
            return node.id
        if isinstance(node, ast.Compare):
            return self._compare(node)
        if isinstance(node, ast.BoolOp):
            joiner = " & " if isinstance(node.op, ast.And) else " | "
            return joiner.join(f"({self.convert(v)})" for v in node.values)
        if isinstance(node, ast.UnaryOp):
            if isinstance(node.op, ast.Not):
                return f"~({self.convert(node.operand)})"
            if isinstance(node.op, ast.USub):
                return f"-({self.convert(node.operand)})"
        if isinstance(node, ast.BinOp) and type(node.op) in self.BIN:
            return f"({self.convert(node.left)} {self.BIN[type(node.op)]} {self.convert(node.right)})"
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in {"startswith", "endswith"}
            and len(node.args) == 1
            and not node.keywords
        ):
            return f"{self.convert(node.func.value)}.{node.func.attr}({self.convert(node.args[0])})"
        raise _Untranslatable

    def _compare(self, node: ast.Compare) -> str:
        parts = []
        left = node.left
        for op, right in zip(node.ops, node.comparators, strict=True):
            lhs = self.convert(left)
            if isinstance(op, (ast.Is, ast.IsNot)):
                if not (isinstance(right, ast.Constant) and right.value is None):
                    raise _Untranslatable
                parts.append(f"{lhs}.{'isNull' if isinstance(op, ast.Is) else 'isNotNull'}()")
            elif isinstance(op, (ast.In, ast.NotIn)):
                if not isinstance(right, (ast.List, ast.Tuple, ast.Set)):
                    raise _Untranslatable
                values = ", ".join(self.convert(v) for v in right.elts)
                expr = f"{lhs}.isin([{values}])"
                parts.append(expr if isinstance(op, ast.In) else f"~({expr})")
            elif type(op) in self.CMP:
                parts.append(f"({lhs} {self.CMP[type(op)]} {self.convert(right)})")
            else:
                raise _Untranslatable
            left = right
        return " & ".join(parts) if len(parts) > 1 else parts[0]


# --------------------------------------------------------------------------- transformer


class GlueToDatabricks(ast.NodeTransformer):
    def __init__(self, options: ConvertOptions):
        self.opts = options
        self.findings: list[Finding] = []
        self.helpers: set[str] = set()
        self.needs_logging = False
        self.glue_ctx = {"glueContext", "glue_context", "glue_ctx"}
        self.jobs = {"job"}
        self._emitted: set[int] = set()
        self._seen_once: set[str] = set()

    # ------------------------------------------------------------- bookkeeping
    def _record(self, construct: str, node: ast.AST, status: str, note: str) -> None:
        self.findings.append(Finding(construct, getattr(node, "lineno", 0), status, note))

    def _record_once(self, construct: str, node: ast.AST, note: str) -> None:
        if construct not in self._seen_once:
            self._seen_once.add(construct)
            self._record(construct, node, AUTO, note)

    def _manual(self, construct: str, node: ast.AST, note: str) -> ast.AST:
        self._record(construct, node, MANUAL, note)
        return node

    def visit(self, node):
        """Put a fail-fast `raise` in front of any statement that still needs manual work."""
        if not isinstance(node, ast.stmt):
            return super().visit(node)
        mark = len(self.findings)
        result = super().visit(node)
        pending = [
            (i, f)
            for i, f in enumerate(self.findings[mark:], start=mark)
            if f.status == MANUAL and i not in self._emitted
        ]
        if not pending or result is None:
            return result
        raises = []
        for i, finding in pending:
            self._emitted.add(i)
            msg = (
                f"{MANUAL_MARKER} {finding.construct} (Glue line {finding.line}) needs manual migration. {finding.note}"
            )
            raises.append(
                ast.Raise(
                    exc=ast.Call(func=ast.Name("NotImplementedError", ast.Load()), args=[_const(msg)], keywords=[])
                )
            )
        return raises + (result if isinstance(result, list) else [result])

    # ------------------------------------------------------------- boilerplate
    def visit_ImportFrom(self, node: ast.ImportFrom):
        module = node.module or ""
        if module.startswith("awsglue"):
            self._record_once("awsglue imports", node, "Glue libraries removed; plain PySpark used instead.")
            return None
        if module == "pyspark.context":
            return None
        return node

    def visit_Import(self, node: ast.Import):
        node.names = [a for a in node.names if not a.name.startswith("awsglue")]
        return node if node.names else None

    def visit_Assign(self, node: ast.Assign):
        value = node.value
        names = [t.id for t in node.targets if isinstance(t, ast.Name)]
        if isinstance(value, ast.Call):
            callee = _dotted(value.func)
            if callee.endswith("SparkContext") or callee.endswith("SparkContext.getOrCreate"):
                return None
            if callee.endswith("GlueContext"):
                self.glue_ctx.update(names)
                self._record_once("GlueContext", node, "Replaced by the Databricks SparkSession.")
                return None
            if callee.split(".")[-1] == "Job":
                self.jobs.update(names)
                self._record_once("Job init/commit", node, "Glue Job wrapper removed; Databricks Jobs handle runs.")
                return None
        if (
            isinstance(value, ast.Attribute)
            and value.attr in {"spark_session", "sparkSession"}
            and isinstance(value.value, ast.Name)
            and value.value.id in self.glue_ctx
        ):
            if names == ["spark"]:
                return None
            node.value = ast.Name("spark", ast.Load())
            return node
        return self.generic_visit(node)

    def visit_Expr(self, node: ast.Expr):
        call = node.value
        if (
            isinstance(call, ast.Call)
            and isinstance(call.func, ast.Attribute)
            and isinstance(call.func.value, ast.Name)
            and call.func.value.id in self.jobs
            and call.func.attr in {"init", "commit"}
        ):
            return None
        return self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute):
        if (
            node.attr in {"spark_session", "sparkSession"}
            and isinstance(node.value, ast.Name)
            and node.value.id in self.glue_ctx
        ):
            return ast.Name("spark", ast.Load())
        return self.generic_visit(node)

    # ------------------------------------------------------------- calls
    def visit_Call(self, node: ast.Call):
        self.generic_visit(node)
        name = _dotted(node.func)
        parts = name.split(".")
        try:
            if name.endswith(
                (
                    "create_dynamic_frame.from_catalog",
                    "create_dynamic_frame_from_catalog",
                    "create_data_frame.from_catalog",
                )
            ):
                return self._from_catalog(node)
            if name.endswith(("create_dynamic_frame.from_options", "create_dynamic_frame_from_options")):
                return self._from_options(node)
            if name.endswith(("write_dynamic_frame.from_options", "write_dynamic_frame_from_options")):
                return self._write_options(node)
            if name.endswith(("write_dynamic_frame.from_catalog", "write_dynamic_frame_from_catalog")):
                return self._write_catalog(node)
            if name.endswith("DynamicFrame.fromDF"):
                return node.args[0] if node.args else _arg(node, "dataframe")
            if (
                isinstance(node.func, ast.Attribute)
                and node.func.attr == "toDF"
                and not node.args
                and not node.keywords
            ):
                return node.func.value
            if parts[-1] == "getResolvedOptions":
                return self._resolved_options(node)
            if parts[-1] == "get_logger" and parts[0] in self.glue_ctx:
                self.needs_logging = True
                self._record_once("get_logger", node, "Glue logger replaced with the standard logging module.")
                return _expr('logging.getLogger("glue2dbx")')
            if len(parts) >= 2 and parts[-1] == "apply":
                return self._transform(parts[-2], node)
            if parts[0] in self.glue_ctx:
                return self._manual(
                    name, node, "GlueContext call with no Databricks mapping in glue2dbx. Rewrite by hand."
                )
        except _Untranslatable:
            return self._manual(name, node, "Arguments are not literals glue2dbx can rewrite safely. Convert by hand.")
        return node

    # ------------------------------------------------------------- reads
    def _qualified(self, db: ast.expr, table: ast.expr) -> ast.expr:
        cat = self.opts.catalog
        if isinstance(db, ast.Constant) and isinstance(table, ast.Constant):
            return _const(f"{cat}.{db.value}.{table.value}")
        return _expr(f'f"{cat}.{{{ast.unparse(db)}}}.{{{ast.unparse(table)}}}"')

    def _map_path(self, node: ast.expr) -> ast.expr:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            for old, new in self.opts.path_map:
                if node.value.startswith(old):
                    return _const(new + node.value[len(old) :])
        return node

    def _from_catalog(self, node: ast.Call) -> ast.expr:
        db, table = _arg(node, "database", 0), _arg(node, "table_name", 1)
        if db is None or table is None:
            raise _Untranslatable
        qualified = self._qualified(db, table)
        expr = _method(_expr("spark"), "table", [qualified])
        note = f"Glue Data Catalog table read from Unity Catalog as {ast.unparse(qualified)}."
        predicate = _arg(node, "push_down_predicate")
        if predicate is not None:
            expr = _method(expr, "where", [predicate])
            note += " push_down_predicate became .where() (partition pruning still applies)."
        self._record("create_dynamic_frame.from_catalog", node, AUTO, note)
        return expr

    def _from_options(self, node: ast.Call) -> ast.expr:
        ctype = _literal(_arg(node, "connection_type", 0))
        copts = _dict_items(_arg(node, "connection_options", 1))
        if copts is None:
            raise _Untranslatable
        if ctype == "s3":
            return self._read_s3(node, copts)
        if ctype in JDBC_TYPES:
            return self._read_jdbc(node, copts)
        return self._manual(
            f"from_options({ctype})",
            node,
            f"Source type '{ctype}' needs a hand-written reader (e.g. Kafka/Kinesis connector).",
        )

    def _read_s3(self, node: ast.Call, copts: dict[str, ast.expr]) -> ast.expr:
        paths = copts.get("paths")
        if not isinstance(paths, ast.List) or not paths.elts:
            raise _Untranslatable
        fmt_node = _arg(node, "format", 2) or _const("parquet")
        fmt = fmt_node.value if isinstance(fmt_node, ast.Constant) else None
        reader = _method(_expr("spark.read"), "format", [fmt_node])

        fopts = _dict_items(_arg(node, "format_options", 3)) or {}
        option_map = CSV_OPTION_MAP if fmt == "csv" else JSON_OPTION_MAP if fmt == "json" else {}
        if fmt == "csv" and "withHeader" not in fopts:
            fopts = {"withHeader": _const(False), **fopts}
        for key, value in fopts.items():
            if key not in option_map:
                raise _Untranslatable
            reader = _method(reader, "option", [_const(option_map[key]), value])
        recurse = copts.get("recurse")
        if recurse is not None:
            reader = _method(reader, "option", [_const("recursiveFileLookup"), recurse])

        mapped = [self._map_path(p) for p in paths.elts]
        target = mapped[0] if len(mapped) == 1 else ast.List(elts=mapped, ctx=ast.Load())
        self._record(
            "create_dynamic_frame.from_options(s3)", node, AUTO, f"S3 {fmt or 'file'} read rewritten as spark.read."
        )
        return _method(reader, "load", [target])

    def _read_jdbc(self, node: ast.Call, copts: dict[str, ast.expr]) -> ast.expr:
        if "url" not in copts or "dbtable" not in copts:
            raise _Untranslatable
        reader = _method(_expr("spark.read"), "format", [_const("jdbc")])
        for key in ("url", "dbtable"):
            reader = _method(reader, "option", [_const(key), copts[key]])
        for key in ("user", "password"):
            if key in copts:
                secret = _expr(f'dbutils.secrets.get(scope="glue2dbx", key="{key}")')
                reader = _method(reader, "option", [_const(key), secret])
        self._record(
            "create_dynamic_frame.from_options(jdbc)",
            node,
            REVIEW,
            "JDBC credentials moved to Databricks secret scope 'glue2dbx'. Create the scope and confirm network access.",
        )
        return _method(reader, "load")

    # ------------------------------------------------------------- writes
    def _writer(self, frame: ast.expr) -> ast.expr:
        writer = _method(
            ast.Attribute(value=frame, attr="write", ctx=ast.Load()), "format", [_const(self.opts.target_format)]
        )
        return _method(writer, "mode", [_const("append")])

    def _write_options(self, node: ast.Call) -> ast.expr:
        frame = _arg(node, "frame", 0)
        ctype = _literal(_arg(node, "connection_type", 1))
        copts = _dict_items(_arg(node, "connection_options", 2))
        if frame is None or copts is None:
            raise _Untranslatable
        if ctype != "s3":
            return self._manual(
                f"write_dynamic_frame.from_options({ctype})", node, f"Sink type '{ctype}' needs a hand-written writer."
            )
        path = copts.get("path")
        if path is None:
            raise _Untranslatable
        writer = self._writer(frame)
        keys = copts.get("partitionKeys")
        if isinstance(keys, ast.List) and keys.elts:
            writer = _method(writer, "partitionBy", keys.elts)
        fmt = _arg(node, "format", 3)
        old = fmt.value if isinstance(fmt, ast.Constant) else "file"
        self._record(
            "write_dynamic_frame.from_options(s3)",
            node,
            AUTO,
            f"S3 {old} sink written as {self.opts.target_format} in append mode (same as Glue's default).",
        )
        return _method(writer, "save", [self._map_path(path)])

    def _write_catalog(self, node: ast.Call) -> ast.expr:
        frame = _arg(node, "frame", 0)
        db, table = _arg(node, "database", 1), _arg(node, "table_name", 2)
        if frame is None or db is None or table is None:
            raise _Untranslatable
        qualified = self._qualified(db, table)
        self._record(
            "write_dynamic_frame.from_catalog",
            node,
            AUTO,
            f"Catalog sink written to Delta table {ast.unparse(qualified)}.",
        )
        return _method(self._writer(frame), "saveAsTable", [qualified])

    # ------------------------------------------------------------- job args
    def _resolved_options(self, node: ast.Call) -> ast.expr:
        names = _arg(node, "options", 1)
        if names is None:
            raise _Untranslatable
        self.helpers.add("get_job_args")
        self._record("getResolvedOptions", node, AUTO, "Job parameters read from Databricks widgets / task parameters.")
        return ast.Call(func=ast.Name("get_job_args", ast.Load()), args=[names], keywords=[])

    # ------------------------------------------------------------- transforms
    def _transform(self, cls: str, node: ast.Call) -> ast.expr:
        handler = getattr(self, f"_t_{cls}", None)
        if handler is not None:
            return handler(node)
        if cls in MANUAL_TRANSFORMS:
            return self._manual(f"{cls}.apply", node, MANUAL_TRANSFORMS[cls])
        return node  # not a Glue transform (e.g. a user's own .apply)

    def _frame(self, node: ast.Call) -> ast.expr:
        frame = _arg(node, "frame", 0)
        if frame is None:
            raise _Untranslatable
        return frame

    def _t_ApplyMapping(self, node: ast.Call) -> ast.expr:
        frame = self._frame(node)
        mappings = _literal(_arg(node, "mappings", 1))
        cols = []
        for mapping in mappings:
            if len(mapping) != 4:
                raise _Untranslatable
            src, _src_type, tgt, tgt_type = mapping
            cols.append(_expr(f"F.col({src!r}).cast({_spark_type(tgt_type)!r}).alias({tgt!r})"))
        self._record("ApplyMapping.apply", node, AUTO, f"{len(cols)} column mappings rewritten as select(cast/alias).")
        return _method(frame, "select", cols)

    def _t_SelectFields(self, node: ast.Call) -> ast.expr:
        return self._columns_call(node, "select", "SelectFields.apply")

    def _t_DropFields(self, node: ast.Call) -> ast.expr:
        return self._columns_call(node, "drop", "DropFields.apply")

    def _columns_call(self, node: ast.Call, method: str, construct: str) -> ast.expr:
        frame = self._frame(node)
        paths = _arg(node, "paths", 1)
        if paths is None:
            raise _Untranslatable
        args = paths.elts if isinstance(paths, ast.List) else [ast.Starred(value=paths, ctx=ast.Load())]
        self._record(construct, node, AUTO, f"Rewritten as DataFrame.{method}().")
        return _method(frame, method, args)

    def _t_RenameField(self, node: ast.Call) -> ast.expr:
        frame = self._frame(node)
        old, new = _arg(node, "old_name", 1), _arg(node, "new_name", 2)
        if old is None or new is None:
            raise _Untranslatable
        nested = isinstance(old, ast.Constant) and "." in str(old.value)
        status, note = (
            (REVIEW, "Nested field rename: withColumnRenamed only renames top-level columns.")
            if nested
            else (AUTO, "Rewritten as withColumnRenamed().")
        )
        self._record("RenameField.apply", node, status, note)
        return _method(frame, "withColumnRenamed", [old, new])

    def _t_DropNullFields(self, node: ast.Call) -> ast.expr:
        frame = self._frame(node)
        self.helpers.add("drop_null_columns")
        self._record(
            "DropNullFields.apply", node, AUTO, "Rewritten with drop_null_columns() helper (drops all-null columns)."
        )
        return ast.Call(func=ast.Name("drop_null_columns", ast.Load()), args=[frame], keywords=[])

    def _t_ResolveChoice(self, node: ast.Call) -> ast.expr:
        frame = self._frame(node)
        if _arg(node, "choice") is not None:
            return self._manual(
                "ResolveChoice.apply",
                node,
                "Frame-wide choice= resolution has no direct Spark equivalent. Cast columns explicitly.",
            )
        specs = _literal(_arg(node, "specs", 1))
        expr: ast.expr = frame
        for column, action in specs:
            kind, _, spark_type = action.partition(":")
            if kind not in {"cast", "project"} or not spark_type:
                return self._manual(
                    "ResolveChoice.apply",
                    node,
                    f"Action '{action}' (make_struct/make_cols) needs a hand-written rewrite.",
                )
            expr = _method(
                expr, "withColumn", [_const(column), _expr(f"F.col({column!r}).cast({_spark_type(spark_type)!r})")]
            )
        self._record("ResolveChoice.apply", node, AUTO, f"{len(specs)} cast spec(s) rewritten as withColumn(cast).")
        return expr

    def _t_Join(self, node: ast.Call) -> ast.expr:
        left, right = _arg(node, "frame1", 0), _arg(node, "frame2", 1)
        keys1, keys2 = _literal(_arg(node, "keys1", 2)), _literal(_arg(node, "keys2", 3))
        if left is None or right is None or len(keys1) != len(keys2):
            raise _Untranslatable
        if keys1 == keys2:
            on = ast.List(elts=[_const(k) for k in keys1], ctx=ast.Load())
            note = "Same-name keys joined with on=[...]: Spark keeps one copy of each key column."
        else:
            conds = [
                ast.Compare(
                    left=ast.Subscript(value=copy.deepcopy(left), slice=_const(k1), ctx=ast.Load()),
                    ops=[ast.Eq()],
                    comparators=[ast.Subscript(value=copy.deepcopy(right), slice=_const(k2), ctx=ast.Load())],
                )
                for k1, k2 in zip(keys1, keys2, strict=True)
            ]
            on = ast.List(elts=conds, ctx=ast.Load())
            note = "Inner equi-join rewritten with explicit conditions. Check for duplicate non-key column names."
        self._record("Join.apply", node, REVIEW, note)
        return _method(
            left, "join", [right], [ast.keyword(arg="on", value=on), ast.keyword(arg="how", value=_const("inner"))]
        )

    def _t_Filter(self, node: ast.Call) -> ast.expr:
        frame = self._frame(node)
        func = _arg(node, "f", 1)
        if not isinstance(func, ast.Lambda) or len(func.args.args) != 1:
            return self._manual(
                "Filter.apply",
                node,
                "Filter uses a named Python function. Rewrite the condition as a Column expression.",
            )
        try:
            condition = _LambdaToColumn(func.args.args[0].arg).convert(func.body)
        except _Untranslatable:
            return self._manual(
                "Filter.apply",
                node,
                "Lambda is too complex to translate. Rewrite the condition as a Column expression.",
            )
        self._record(
            "Filter.apply",
            node,
            AUTO,
            "Row lambda translated to a Spark Column filter (runs in the JVM, no Python UDF).",
        )
        return _method(frame, "filter", [_expr(condition)])


# --------------------------------------------------------------------------- public API


def _fill_empty_bodies(tree: ast.AST) -> None:
    for node in ast.walk(tree):
        body = getattr(node, "body", None)
        if isinstance(body, list) and not body:
            node.body = [ast.Pass()]


def _header(transformer: GlueToDatabricks, source_name: str) -> str:
    lines = [
        "# Databricks notebook source",
        f"# Converted from AWS Glue by glue2dbx (source: {source_name}).",
        "# Review the migration report before running this in production.",
    ]
    imports = []
    if transformer.needs_logging:
        imports.append("import logging")
    if "get_job_args" in transformer.helpers:
        imports += ["import os", "import sys"]
    lines += sorted(imports)
    lines += [
        "",
        "from pyspark.sql import SparkSession",
        "from pyspark.sql import functions as F",
        "",
        "spark = SparkSession.builder.getOrCreate()",
    ]
    for helper in sorted(transformer.helpers):
        lines.append(HELPERS[helper].rstrip())
    return "\n".join(lines) + "\n"


def _strip_header_duplicates(tree: ast.Module, transformer: GlueToDatabricks) -> None:
    provided = {"logging"} if transformer.needs_logging else set()
    if "get_job_args" in transformer.helpers:
        provided |= {"os", "sys"}
    kept = []
    for stmt in tree.body:
        if isinstance(stmt, ast.Import):
            stmt.names = [a for a in stmt.names if not (a.name in provided and a.asname is None)]
            if not stmt.names:
                continue
        if isinstance(stmt, ast.ImportFrom) and stmt.module == "pyspark.sql":
            stmt.names = [
                a for a in stmt.names if (a.name, a.asname) not in {("functions", "F"), ("SparkSession", None)}
            ]
            if not stmt.names:
                continue
        kept.append(stmt)
    tree.body = kept


def _comment_manual_steps(code: str) -> str:
    out = []
    for line in code.splitlines():
        stripped = line.lstrip()
        if stripped.startswith("raise NotImplementedError(") and MANUAL_MARKER in stripped:
            indent = line[: len(line) - len(stripped)]
            out.append(f"{indent}# TODO(glue2dbx): manual migration required - see the migration report.")
        out.append(line)
    return "\n".join(out) + "\n"


def convert_source(source: str, options: ConvertOptions | None = None) -> ConversionResult:
    """Convert one Glue script (as text) into Databricks PySpark."""
    options = options or ConvertOptions()
    tree = ast.parse(source)
    transformer = GlueToDatabricks(options)
    docstring = ast.get_docstring(tree)
    if docstring:
        tree.body = tree.body[1:]
    tree = transformer.visit(tree)
    _strip_header_duplicates(tree, transformer)
    _fill_empty_bodies(tree)
    ast.fix_missing_locations(tree)
    body = ast.unparse(tree)
    header = _header(transformer, options.source_name)
    if docstring:
        summary = "\n".join(f"# {line}".rstrip() for line in docstring.splitlines())
        header = header.replace("# Databricks notebook source\n", f"# Databricks notebook source\n{summary}\n", 1)
    code = header + "\n\n" + _comment_manual_steps(body)
    findings = sorted(transformer.findings, key=lambda f: f.line)
    return ConversionResult(code=code, findings=findings)
