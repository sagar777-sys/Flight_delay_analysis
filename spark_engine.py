"""Spark (PySpark) engine: reads, cleans and analyses the flight data."""
import glob
import os
import shutil
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import add_rates

NAME = "Apache Spark (PySpark)"
REQUIRED = ["flight_id", "airline", "origin", "destination", "scheduled_departure", "actual_departure",
            "scheduled_arrival", "actual_arrival", "cancelled", "delay_reason", "distance_km"]

_spark = None


def _set_java_home():
    """Finds Java on the host and sets JAVA_HOME so PySpark can launch the JVM."""
    home = os.environ.get("JAVA_HOME")
    if home and os.path.exists(os.path.join(home, "bin", "java")):
        return
    java = shutil.which("java")
    if java:
        os.environ["JAVA_HOME"] = os.path.dirname(os.path.dirname(os.path.realpath(java)))
        return
    for cand in sorted(glob.glob("/usr/lib/jvm/*/bin/java")):
        os.environ["JAVA_HOME"] = os.path.dirname(os.path.dirname(cand))
        os.environ["PATH"] = os.path.dirname(cand) + os.pathsep + os.environ.get("PATH", "")
        return
    raise RuntimeError("Java was not found on this server. Add packages.txt (default-jre-headless) "
                       "to the repo root and redeploy.")


def get_spark():
    global _spark
    if _spark is None:
        _set_java_home()
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        _spark = (
            SparkSession.builder.appName("FlightDelayAnalytics")
            .master("local[2]")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.driver.memory", "512m")
            .config("spark.sql.ansi.enabled", "false")  # bad values become NULL instead of errors
            .getOrCreate()
        )
        _spark.sparkContext.setLogLevel("ERROR")
    return _spark


def load_and_clean(path):
    """Returns (cleaned_spark_df, stats_dict)."""
    spark = get_spark()
    raw = spark.read.option("header", True).csv(path)          # everything as string first
    raw = raw.toDF(*[c.strip().lower() for c in raw.columns])
    missing = [c for c in REQUIRED if c not in raw.columns]
    if missing:
        raise ValueError("CSV is missing columns: " + ", ".join(missing))

    rows_in = raw.count()
    flag = F.lower(F.trim("cancelled"))
    reason = F.trim("delay_reason")
    df = raw.select(
        F.trim("flight_id").alias("flight_id"),
        F.trim("airline").alias("airline"),
        F.upper(F.trim("origin")).alias("origin"),
        F.upper(F.trim("destination")).alias("destination"),
        F.to_timestamp(F.trim("scheduled_departure")).alias("sched_dep"),
        F.to_timestamp(F.trim("actual_departure")).alias("act_dep"),
        F.to_timestamp(F.trim("scheduled_arrival")).alias("sched_arr"),
        F.to_timestamp(F.trim("actual_arrival")).alias("act_arr"),
        F.when(flag.isin("1", "true", "yes"), 1).when(flag.isin("0", "false", "no"), 0).alias("cancelled"),
        F.when(reason.isNull() | (reason == ""), "None").otherwise(F.initcap(reason)).alias("delay_reason"),
        F.trim("distance_km").cast("double").alias("distance_km"),
    )
    df = (df.withColumn("dep_delay", (F.unix_timestamp("act_dep") - F.unix_timestamp("sched_dep")) / 60.0)
            .withColumn("arr_delay", (F.unix_timestamp("act_arr") - F.unix_timestamp("sched_arr")) / 60.0))

    base = (
        F.col("flight_id").isNotNull() & (F.col("flight_id") != "")
        & F.col("airline").isNotNull() & (F.col("airline") != "")
        & F.col("origin").isNotNull() & (F.col("origin") != "")
        & F.col("destination").isNotNull() & (F.col("destination") != "")
        & (F.col("origin") != F.col("destination"))
        & F.col("sched_dep").isNotNull() & F.col("sched_arr").isNotNull()
        & (F.col("sched_arr") > F.col("sched_dep"))
        & F.col("cancelled").isNotNull() & (F.col("distance_km") > 0)
    )
    flown_ok = F.col("dep_delay").between(-90, 1440) & F.col("arr_delay").between(-90, 1440)
    df = df.filter(base & ((F.col("cancelled") == 1) | flown_ok))

    # cancelled flights have no delay
    df = (df.withColumn("dep_delay", F.when(F.col("cancelled") == 0, F.col("dep_delay")))
            .withColumn("arr_delay", F.when(F.col("cancelled") == 0, F.col("arr_delay")))
            .withColumn("date", F.to_date("sched_dep"))
            .withColumn("month", F.date_format("sched_dep", "yyyy-MM"))
            .withColumn("hour", F.hour("sched_dep"))
            .cache())
    rows_out = df.count()
    return df, {"rows_in": rows_in, "rows_out": rows_out, "rows_dropped": rows_in - rows_out}


def get_options(df):
    airlines = sorted(r[0] for r in df.select("airline").distinct().collect())
    origins = sorted(r[0] for r in df.select("origin").distinct().collect())
    b = df.agg(F.min("date"), F.max("date")).collect()[0]
    return airlines, origins, b[0], b[1]


AGGS = [
    F.count("*").alias("flights"),
    F.sum("cancelled").alias("cancelled"),
    F.sum("flown").alias("flown"),
    F.sum("delayed").alias("delayed"),
    F.avg("arr_delay").alias("avg_arr_delay"),
    F.avg("dep_delay").alias("avg_dep_delay"),
]


def analyze(df, start, end, airlines, origins, threshold, min_route):
    f = (df.filter((F.col("date") >= F.lit(start)) & (F.col("date") <= F.lit(end))
                   & F.col("airline").isin(airlines) & F.col("origin").isin(origins))
         .withColumn("flown", (F.col("cancelled") == 0).cast("int"))
         .withColumn("delayed", F.when((F.col("cancelled") == 0) & (F.col("arr_delay") > threshold), 1).otherwise(0)))

    k = f.agg(*AGGS).collect()[0]
    total, canc = int(k["flights"] or 0), int(k["cancelled"] or 0)
    flown, delayed = int(k["flown"] or 0), int(k["delayed"] or 0)
    kpi = {"flights": total, "cancelled": canc, "flown": flown, "delayed": delayed,
           "cancelled_pct": canc / total * 100 if total else 0.0,
           "delayed_pct": delayed / flown * 100 if flown else 0.0,
           "ontime_pct": (flown - delayed) / flown * 100 if flown else 0.0,
           "avg_arr_delay": float(k["avg_arr_delay"] or 0.0),
           "avg_dep_delay": float(k["avg_dep_delay"] or 0.0)}

    by_airline = (add_rates(f.groupBy("airline").agg(*AGGS).toPandas())
                  .sort_values("ontime_pct", ascending=False).reset_index(drop=True))
    by_origin = (add_rates(f.groupBy("origin").agg(*AGGS).toPandas())
                 .sort_values("avg_dep_delay", ascending=False).reset_index(drop=True))
    routes = add_rates(f.groupBy("origin", "destination").agg(*AGGS)
                       .filter(F.col("flights") >= min_route).toPandas())
    routes["route"] = routes["origin"] + " → " + routes["destination"]
    routes = routes.sort_values("avg_arr_delay", ascending=False).reset_index(drop=True)
    by_month = add_rates(f.groupBy("month").agg(*AGGS).toPandas()).sort_values("month").reset_index(drop=True)
    by_hour = add_rates(f.groupBy("hour").agg(*AGGS).toPandas()).sort_values("hour").reset_index(drop=True)
    airline_month = add_rates(f.groupBy("airline", "month").agg(*AGGS).toPandas())

    d = f.filter(F.col("delayed") == 1)
    by_reason = (d.groupBy("delay_reason")
                 .agg(F.count("*").alias("flights"), F.sum("arr_delay").alias("total_delay_min"))
                 .orderBy(F.desc("flights")).toPandas())
    dist = (f.filter(F.col("cancelled") == 0)
            .withColumn("bin", (F.floor(F.greatest(F.lit(-30.0), F.least(F.col("arr_delay"), F.lit(180.0))) / 15) * 15)
                        .cast("int"))
            .groupBy("bin").agg(F.count("*").alias("flights")).orderBy("bin").toPandas())
    worst = (d.orderBy(F.desc("arr_delay")).limit(100)
             .select("flight_id", "date", "airline", "origin", "destination", "arr_delay", "delay_reason").toPandas())
    return {"kpi": kpi, "by_airline": by_airline, "by_origin": by_origin, "routes": routes, "by_month": by_month,
            "by_hour": by_hour, "airline_month": airline_month, "by_reason": by_reason, "dist": dist, "worst": worst}
