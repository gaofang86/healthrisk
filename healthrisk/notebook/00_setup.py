# Databricks notebook source
# MAGIC %pip install aiohttp

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/healthrisk")


# COMMAND ----------

spark.sql("SELECT current_catalog()").show()
spark.sql("SELECT current_schema()").show()
spark.sql("SHOW TABLES IN bronze").show()
def init_schemas(spark):
    for layer in ["bronze", "silver", "gold"]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{layer}`")

init_schemas(spark)
spark.sql("""
CREATE TABLE IF NOT EXISTS bronze.tile_status (
    tile_id STRING,
    status STRING
)
""")

# COMMAND ----------

dirs_needing_init = [
    "src/features",
    "src/Decision", 
    "src/training",
    "src/utils",
]

base = "/Workspace/Users/gfang8617@gmail.com/healthrisk"

for d in dirs_needing_init:
    path = f"{base}/{d}/__init__.py"
    try:
        dbutils.fs.put(f"file:{path}", "", overwrite=False)
        print(f"created: {path}")
    except Exception as e:
        print(f"skip: {path} — {e}")

# COMMAND ----------

import importlib
from src.ingestion import climate_ingestion
importlib.reload(climate_ingestion)

# COMMAND ----------

spark.sql("TRUNCATE TABLE bronze.tile_status")

# COMMAND ----------

# ===== MAIN PIPELINE =====

from src.ingestion import climate_ingestion
from src.ingestion import dengue_ingestion
import config
import asyncio

# checkpoint
try:
    df = spark.table("bronze.tile_status")
    df.display()  
    done_tile_ids = set(
    row.tile_id
    for row in df
        .filter("status = 'DONE'")
        .select("tile_id")
        .collect()
    )
except Exception as e:
    print("ERROR:", e)
    done_tile_ids = set()

# build tiles
all_tiles = climate_ingestion.build_tiles(
    config.LON_MIN,
    config.LON_MAX,
    config.LAT_MIN,
    config.LAT_MAX,
    config.TILE_LON_SIZE,
    config.TILE_LAT_SIZE
)

tiles = [
    (
        f"{tile['lon_min']}_{tile['lon_max']}_{tile['lat_min']}_{tile['lat_max']}",
        tile
    )
    for tile in all_tiles
    if f"{tile['lon_min']}_{tile['lon_max']}_{tile['lat_min']}_{tile['lat_max']}" not in done_tile_ids
]

print(f"[INFO] total: {len(all_tiles)} | remaining: {len(tiles)}")

# run async

import asyncio

if not tiles:
    print("[INFO] All tiles already processed. Nothing to run.")
else:
    results = await climate_ingestion.run_all_tiles(tiles)  

    all_dfs = [r["df"] for r in results]

    if all_dfs:
        import pandas as pd
        df = pd.concat(all_dfs, ignore_index=True)
        spark_df = spark.createDataFrame(df)
        spark_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable("bronze.nasa_weather")


    #write checkpoint
    checkpoint_df = spark.createDataFrame(
    [(tile_id, "DONE") for tile_id, _ in tiles],
    ["tile_id", "status"]
    )

    checkpoint_df.write \
        .mode("append") \
        .saveAsTable("bronze.tile_status")
    