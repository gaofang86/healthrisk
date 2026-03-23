# Databricks notebook source
import sys, importlib
sys.path.insert(0, "/Workspace/healthrisk")

from src.ingestion import dengue_ingestion
from src.features import preprocess
from src.training import training
import config

importlib.reload(dengue_ingestion)
importlib.reload(preprocess)
importlib.reload(training)
importlib.reload(config)

from src.ingestion.dengue_ingestion import ingest_dengue
from src.features.preprocess import build_dengue_features
from src.training.training import build_training_dataset

df_test = spark.read.option("header", "true").csv(
    "/Volumes/workspace_7474658570712100/bronze/raw_data/Spatial_extract_V1_3.csv"
).toPandas()

print(df_test["adm_0_name"].unique())

ingest_dengue(spark)
build_dengue_features(spark)
build_training_dataset(spark)


# COMMAND ----------

print("bronze.dengue_raw:      ", spark.table("bronze.dengue_raw").count())
print("silver.dengue_features: ", spark.table("silver.dengue_features").count())
print("gold.training_dataset:  ", spark.table("gold.training_dataset").count())

# COMMAND ----------

print("=== climate gold grid_id ===")
spark.table("gold.climate_features_vp").select("grid_id").distinct().show(5, truncate=False)

print("=== dengue silver grid_id ===")
spark.table("silver.dengue_features").select("grid_id").distinct().show(5, truncate=False)
spark.table("bronze.dengue_raw").select("adm_0_name", "adm_1_name").distinct().orderBy("adm_0_name").show(100, truncate=False)

print("=== climate gold iso_week ===")
spark.table("gold.climate_features_vp").select("iso_week").distinct().show(5, truncate=False)

print("=== dengue silver iso_week ===")
spark.table("silver.dengue_features").select("year_month").distinct().show(5, truncate=False)

# COMMAND ----------

spark.table("gold.training_dataset").select(
    "adm_0_name", "year_month", "temp_mean_C", "vsi",
    "dengue_total", "outbreak_label"
).show(10, truncate=False)

spark.table("gold.training_dataset").groupBy("outbreak_label").count().show()