# Databricks notebook source
spark.sql("SHOW CATALOGS").show()

# COMMAND ----------

spark.sql("USE CATALOG healthrisk")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# COMMAND ----------

import os
print(os.getcwd())

import subprocess
result = subprocess.run(['find', '/Workspace', '-name', 'src', '-type', 'd'], capture_output=True, text=True)
print(result.stdout)

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %pip install aiohttp

# COMMAND ----------

import importlib
import sys
sys.path.insert(0, "/Workspace/Repos/gfine886@gmail.com/healthrisk/healthrisk")

# import first，then reload
from src.ingestion import climate_ingestion
import config
from src.features import feature_engineering

importlib.reload(climate_ingestion)
importlib.reload(config)
importlib.reload(feature_engineering)

spark.sql("SHOW TABLES IN bronze").show()
from src.features.feature_engineering import build_silver_climate, build_gold_climate, validate_gold

# COMMAND ----------

import importlib
from src.features import feature_engineering
importlib.reload(feature_engineering)
from src.features.feature_engineering import build_silver_climate, build_gold_climate, validate_gold

build_silver_climate(spark)
build_gold_climate(spark)

# COMMAND ----------

build_silver_climate(spark)
build_gold_climate(spark)
validate_gold(spark)
