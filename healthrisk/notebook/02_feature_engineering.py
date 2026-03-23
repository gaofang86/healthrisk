# Databricks notebook source
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# COMMAND ----------

import importlib
import sys
sys.path.insert(0, "/Workspace/healthrisk")

# import first，then reload
from src.ingestion import climate_ingestion
import config
from src.features import feature_engineering

importlib.reload(climate_ingestion)
importlib.reload(config)
importlib.reload(feature_engineering)

spark.sql("SHOW TABLES IN bronze").show()
from src.features.feature_engineering import build_silver_climate, build_gold_climate, validate_gold