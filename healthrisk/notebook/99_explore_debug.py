# Databricks notebook source
import requests, io, pandas as pd

OPENDENGUE_URL = "https://raw.githubusercontent.com/OpenDengue/master-repo/main/data/releases/V1.2/Spatial_extract_V1.2.csv"

r = requests.get(OPENDENGUE_URL, timeout=60)
df = pd.read_csv(io.StringIO(r.text))
print(df.columns.tolist())
print(df.head(2))

# COMMAND ----------

from pyspark.sql import functions as F
spark.sql("USE CATALOG healthrisk")

spark.table("bronze.dengue_raw") \
    .filter(F.col("adm_0_name").isin([
        "BRAZIL", "VENEZUELA", "PARAGUAY", 
        "EL SALVADOR", "DOMINICAN REPUBLIC", "HONDURAS"
    ])) \
    .select("adm_0_name", "adm_1_name") \
    .distinct() \
    .orderBy("adm_0_name", "adm_1_name") \
    .show(200, truncate=False)

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
spark.sql("USE CATALOG healthrisk")
dt = DeltaTable.forName(spark, "bronze.tile_status")
dt.delete(F.col("tile_id").rlike("^-"))

# COMMAND ----------

spark.sql("USE CATALOG healthrisk")
from pyspark.sql import functions as F

spark.table("bronze.nasa_weather") \
    .filter(F.col("longitude") < 0) \
    .groupBy("status") \
    .count() \
    .show()

# COMMAND ----------

spark.sql("USE CATALOG healthrisk")
from pyspark.sql import functions as F

print("=== climate ===")
spark.table("gold.climate_features_vp") \
    .filter(F.col("longitude") < 0) \
    .select("grid_id").distinct().limit(5).show(truncate=False)

print("=== dengue ===")
spark.table("silver.dengue_features") \
    .select("grid_id").distinct().limit(5).show(truncate=False)

# COMMAND ----------

spark.sql("USE CATALOG healthrisk")

# 看 bronze 里有没有南美数据
spark.table("bronze.nasa_weather") \
    .filter(F.col("longitude") < 0) \
    .select("longitude", "latitude") \
    .limit(5).show()

# 看总体经度范围
spark.table("bronze.nasa_weather") \
    .filter(F.col("longitude") < 0) \
    .groupBy("status") \
    .count() \
    .show()

spark.table("silver.climate_features") \
    .agg(F.min("longitude"), F.max("longitude")).show()

# COMMAND ----------

spark.sql("USE CATALOG healthrisk")
from pyspark.sql import functions as F

# climate 的 grid_id 样本
print("=== climate grid_ids ===")
spark.table("gold.climate_features_vp") \
    .filter(F.col("longitude") < 0) \
    .select("grid_id", "longitude", "latitude") \
    .limit(5).show(truncate=False)
# dengue 的 grid_id 样本
print("=== dengue grid_ids ===")
spark.table("silver.dengue_features") \
    .select("grid_id", "centroid_lon", "centroid_lat") \
    .limit(10).show(truncate=False)



# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/healthrisk")

spark.sql("SHOW SCHEMAS").show()
spark.sql("SHOW TABLES IN bronze").show()
spark.table("bronze.tile_status").count() 
df = spark.table("bronze.nasa_weather")
from pyspark.sql.functions import col
from pyspark.sql import functions as F

import requests, io, pandas as pd

OPENDENGUE_URL = "https://raw.githubusercontent.com/OpenDengue/master-repo/main/data/releases/V1.2/Spatial_extract_V1.2.csv"

r = requests.get(OPENDENGUE_URL, timeout=60)
df = pd.read_csv(io.StringIO(r.text))
print(df.columns.tolist())
print(df.head(2))

# total counts
df.groupBy("status").count().show()

# NULL check  
from pyspark.sql.functions import col, sum as spark_sum
df.select([
    spark_sum(col(c).isNull().cast("int")).alias(c)
    for c in ["T2M", "PRECTOTCORR", "RH2M"]
]).show()

# time range + grid amount
df.selectExpr("min(date)", "max(date)", "count(distinct grid_id)").show()

# grid points distribution
tmp = df.groupBy("grid_id").count()

tmp = df.groupBy("grid_id").count()

display(tmp)

summary = tmp.selectExpr(
    "min(count) as min_count",
    "max(count) as max_count",
    "avg(count) as avg_count",
    "count(*) as total_grids"
)

display(summary)

df.select("T2M").describe().show()
df.select("PRECTOTCORR").describe().show()
df.selectExpr("min(date)", "max(date)").show()
df.filter(col("grid_id") == 112.0).orderBy("date").show()
df.groupBy("grid_id").agg(
    F.avg("T2M").alias("avg_temp")
).orderBy("avg_temp").show()
