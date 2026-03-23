# feature_engineering.py
from pyspark.sql import functions as F, SparkSession
from config import *
from pyspark.sql.window import Window

# ─────────────────────────────────────────────────
# 1. SILVER: aggregate daily → weekly
# ─────────────────────────────────────────────────
def build_silver_climate(spark: SparkSession):
    df = (
    spark.table(BRONZE_TABLE)
    .filter("status = 'SUCCESS'")
    .withColumn("date_parsed", F.to_date(F.col("date"), DATE_FORMAT))
    .withColumn("month_num", F.month("date_parsed"))   
    )

    df_silver = (
        df.groupBy(
            "grid_id", "longitude", "latitude",
            F.date_trunc("week", F.col("date_parsed")).alias("iso_week"),
        )
        .agg(
            # temperature
            F.avg("T2M").alias("temp_mean_C"),
            F.stddev("T2M").alias("temp_std"),
            F.max("T2M").alias("temp_max"),

            # 🌧️ precipitation
            F.sum("PRECTOTCORR").alias("precip_sum_7d"),
            F.max("PRECTOTCORR").alias("precip_max"),
            F.expr("percentile(PRECTOTCORR, 0.9)").alias("precip_p90"),

            # 💧 humidity
            F.avg("RH2M").alias("humidity_mean"),
            F.stddev("RH2M").alias("humidity_std"),

            # month
            F.first("month_num").alias("month_num"),
        )
        .withColumn("temp_sq", F.col("temp_mean_C") ** 2)

        # season encoding
        .withColumn("month_sin", F.sin(2 * 3.14159 * F.col("month_num") / 12))
        .withColumn("month_cos", F.cos(2 * 3.14159 * F.col("month_num") / 12))
    )

    (
        df_silver.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(SILVER_CLIMATE_TABLE)
    )

    count = spark.table(SILVER_CLIMATE_TABLE).count()
    print(f"[silver.climate_features] rows written: {count:,}")
    return count


# ─────────────────────────────────────────────────
# 2. GOLD: add mechanistic vp_temp feature
# ─────────────────────────────────────────────────
def build_gold_climate(spark):
    w = Window.partitionBy("grid_id").orderBy("iso_week")
    # normalization

    df = spark.table(SILVER_CLIMATE_TABLE)
    df = df.withColumn("precip_log", F.log1p(F.col("precip_sum_7d"))) 

    # global stats
    stats = df.select(
        F.mean("temp_mean_C").alias("temp_mean"),
        F.stddev("temp_mean_C").alias("temp_std"),
        F.mean("humidity_mean").alias("hum_mean"),
        F.stddev("humidity_mean").alias("hum_std"),
        F.mean("precip_log").alias("precip_log_mean"),     
        F.stddev("precip_log").alias("precip_log_std"),
    ).collect()[0]

    df_gold = (
        df

        # normalization（data-driven）
        .withColumn("temp_z", (F.col("temp_mean_C") - stats["temp_mean"]) / stats["temp_std"])
        .withColumn("hum_z", (F.col("humidity_mean") - stats["hum_mean"]) / stats["hum_std"])

        # “week mechanistic score”
        .withColumn("temp_score", -F.abs(F.col("temp_z")))
        .withColumn("humidity_score", -F.abs(F.col("hum_z")))

        # ✅ VSI
        .withColumn("precip_z",
            (F.col("precip_log") - stats["precip_log_mean"]) / stats["precip_log_std"])
        .withColumn("vsi",
            F.col("temp_score") + F.col("humidity_score") + F.col("precip_z"))

        # interaction
        .withColumn("temp_x_precip", F.col("temp_mean_C") * F.col("precip_sum_7d"))

        # lag
        .withColumn("precip_lag1", F.lag("precip_sum_7d", 1).over(w))
        .withColumn("temp_lag2", F.lag("temp_mean_C", 2).over(w))
        .withColumn("humidity_lag1", F.lag("humidity_mean", 1).over(w))
    )

    # ───── fillna  ─────
    df_gold = df_gold.fillna({
        "precip_lag1": 0,
        "temp_lag2": 0,
        "humidity_lag1": 0
    })

    # ───── write ─────
    (
        df_gold.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(GOLD_CLIMATE_TABLE)
    )

    count = spark.table(GOLD_CLIMATE_TABLE).count()
    print(f"[gold.climate_features_vp] rows written: {count:,}")
    return count


# ─────────────────────────────────────────────────
# 3. VALIDATE output
# ─────────────────────────────────────────────────
def validate_gold(spark: SparkSession):
    df = spark.table(GOLD_CLIMATE_TABLE)

    print("\n── Schema ──────────────────────────────")
    df.printSchema()

    print("\n── Sample (vp_temp should be in [0,1]) ─")
    df.select("grid_id", "iso_week", "temp_mean_C", "vsi").show(5)

    print("\n── vp_temp range ───────────────────────")
    df.selectExpr(
        "min(vsi)", "max(vsi)", "avg(vsi)",
        "count(*) as total_rows"
    ).show()