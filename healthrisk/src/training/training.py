from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from config import *

COUNTRY_GRID_BOUNDS = {
    "PHILIPPINES":  {"lon": (117, 127), "lat": (5, 20)},
    "VIET NAM":     {"lon": (102, 110), "lat": (8, 24)},
    "INDONESIA":    {"lon": (95, 141),  "lat": (-11, 6)},
    "THAILAND":     {"lon": (97, 106),  "lat": (5, 21)},
    "MALAYSIA":     {"lon": (99, 119),  "lat": (1, 8)},
    "MYANMAR":      {"lon": (92, 102),  "lat": (10, 28)},
    "CAMBODIA":     {"lon": (102, 108), "lat": (10, 15)},
    "LAO PEOPLE'S DEMOCRATIC REPUBLIC": {"lon": (100, 107), "lat": (14, 23)},
}

def build_training_dataset(spark: SparkSession):
    from pyspark.sql.types import StringType

    climate_raw = (spark.table(GOLD_CLIMATE_TABLE)
        .withColumn("year_month", F.date_trunc("month", F.col("iso_week"))))

    # grid ids
    country_dfs = []
    for country, bounds in COUNTRY_GRID_BOUNDS.items():
        lon_min, lon_max = bounds["lon"]
        lat_min, lat_max = bounds["lat"]
        country_df = (climate_raw
            .filter(F.col("longitude").between(lon_min, lon_max))
            .filter(F.col("latitude").between(lat_min, lat_max))
            .withColumn("adm_0_name", F.lit(country)))
        country_dfs.append(country_df)

    climate_with_country = country_dfs[0]
    for df in country_dfs[1:]:
        climate_with_country = climate_with_country.unionByName(df)

    # climate_monthly
    climate_monthly = (climate_with_country
        .groupBy("adm_0_name", "year_month")
        .agg(
            F.avg("temp_mean_C").alias("temp_mean_C"),
            F.avg("humidity_mean").alias("humidity_mean"),
            F.sum("precip_sum_7d").alias("precip_sum_28d"),
            F.avg("vsi").alias("vsi"),
            F.avg("temp_score").alias("temp_score"),
            F.avg("humidity_score").alias("humidity_score"),
            F.avg("precip_z").alias("precip_z"),
            F.avg("month_sin").alias("month_sin"),
            F.avg("month_cos").alias("month_cos"),
        ))

    # country + month  
    dengue_monthly = (spark.table("silver.dengue_features")
        .groupBy("adm_0_name", "year_month")
        .agg(
            F.sum("dengue_total").alias("dengue_total"),
            F.avg("cases_lag_14d").alias("cases_lag_14d"),
            F.avg("cases_roll28").alias("cases_roll28"),
            F.max("outbreak_label").alias("outbreak_label")
        ))

    df = climate_monthly.join(dengue_monthly, 
        on=["adm_0_name", "year_month"], how="inner")

    (df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(GOLD_TRAINING_TABLE))

    count = spark.table(GOLD_TRAINING_TABLE).count()
    print(f"[gold.training_dataset] rows written: {count:,}")
    return count