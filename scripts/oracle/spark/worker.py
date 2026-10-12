"""Spark worker: loads /work/manifest.json tables as Delta tables, then runs
/work/q/<id>.sql files, writing /work/q/<id>.out.json ({rows}|{error})."""
import datetime, decimal, json, os, re, sys, time, traceback
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

W = "/work"
open(f"{W}/worker.pid", "w").write(str(os.getpid()))
builder = (SparkSession.builder.appName("cg-s12")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.warehouse.dir", "/tmp/cg_wh")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.driver.memory", "8g")
    .config("spark.sql.shuffle.partitions", "4"))
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

def stype(t):
    m = re.fullmatch(r"Nullable\((.*)\)", t) or re.fullmatch(r"LowCardinality\((.*)\)", t)
    if m: return stype(m.group(1))
    m = re.fullmatch(r"Array\((.*)\)", t)
    if m: return f"ARRAY<{stype(m.group(1))}>"
    return {"UInt8": "SMALLINT", "UInt16": "INT", "UInt32": "BIGINT", "UInt64": "BIGINT",
            "Int8": "TINYINT", "Int16": "SMALLINT", "Int32": "INT", "Int64": "BIGINT",
            "Float32": "FLOAT", "Float64": "DOUBLE", "String": "STRING", "Date": "DATE",
            "DateTime": "TIMESTAMP", "Bool": "BOOLEAN"}.get(t, "STRING")

for t in json.load(open(f"{W}/manifest.json")):
    db, name = t["db"], t["table"]
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{db}`")
    # ClickHouse holds booleans as UInt8; a Databricks table would be BOOLEAN.
    def ctype(c):
        if re.match(r"(is|has)_", c["name"]) and re.fullmatch(r"(Nullable\()?UInt8\)?", c["type"]):
            return "BOOLEAN"
        return stype(c["type"])
    ddl = ", ".join(f"`{c['name']}` {stype(c['type'])}" for c in t["columns"])
    df = (spark.read.schema(ddl).option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss[XXX][X]")
          .json(f"{W}/{t['path']}"))
    # JSON 1 / 0 do not read as BOOLEAN: read the numbers, then cast.
    df = df.selectExpr(*[
        f"CAST(`{c['name']}` AS BOOLEAN) AS `{c['name']}`" if ctype(c) == "BOOLEAN" else f"`{c['name']}`"
        for c in t["columns"]])
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(f"`{db}`.`{name}`"))
print("loaded", flush=True)
open(f"{W}/q/READY", "w").write("1")

def conv(v):
    if isinstance(v, decimal.Decimal): return float(v) if v != v.to_integral_value() else int(v)
    if isinstance(v, (datetime.datetime, datetime.date)): return v.isoformat()
    if isinstance(v, (list, tuple)): return [conv(x) for x in v]
    if isinstance(v, dict): return {str(k): conv(x) for k, x in v.items()}
    if hasattr(v, "asDict"): return conv(v.asDict(recursive=True))
    if isinstance(v, (bytes, bytearray)): return v.decode("utf-8", "replace")
    if v.__class__.__name__ == "VariantVal": return json.loads(v.toJson())
    return v

while True:
    jobs = sorted(f for f in os.listdir(f"{W}/q") if f.endswith(".sql"))
    if not jobs:
        time.sleep(0.05); continue
    for f in jobs:
        p = f"{W}/q/{f}"
        sql = open(p).read()
        os.remove(p)
        try:
            df = spark.sql(sql)
            rows = [[conv(x) for x in r] for r in df.collect()]
            out = {"columns": df.columns, "rows": rows}
        except Exception as e:
            out = {"error": str(e).splitlines()[0][:600] if str(e) else repr(e)}
        tmp = p[:-4] + ".tmp"
        json.dump(out, open(tmp, "w"), default=str)
        os.rename(tmp, p[:-4] + ".out.json")
