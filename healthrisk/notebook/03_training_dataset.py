# Databricks notebook source
spark.sql("USE CATALOG healthrisk")
from pyspark.sql import functions as F

spark.table("gold.climate_features_vp") \
    .filter(F.col("longitude") < 0) \
    .select("grid_id").distinct().limit(5).show(truncate=False)
spark.table("silver.dengue_features") \
    .select("grid_id").distinct().limit(5).show(truncate=False)

# COMMAND ----------

import sys, importlib
spark.sql("USE CATALOG healthrisk")
sys.path.insert(0, "/Workspace/Repos/gfine886@gmail.com/healthrisk/healthrisk")

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
    "/Volumes/healthrisk/bronze/raw_data/Spatial_extract.csv"
).toPandas()

print(df_test["adm_0_name"].unique())

ingest_dengue(spark)
build_dengue_features(spark)
build_training_dataset(spark)


# COMMAND ----------

spark.table("gold.training_dataset") \
    .groupBy("adm_0_name") \
    .count() \
    .orderBy("count", ascending=False) \
    .show(20)

# COMMAND ----------

print("bronze.dengue_raw:      ", spark.table("bronze.dengue_raw").count())
print("silver.dengue_features: ", spark.table("silver.dengue_features").count())
print("gold.training_dataset:  ", spark.table("gold.training_dataset").count())
