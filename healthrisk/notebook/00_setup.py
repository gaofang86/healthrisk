# Databricks notebook source
# MAGIC %pip install aiohttp

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/gfine886@gmail.com/healthrisk/healthrisk")
import config
import importlib
importlib.reload(config)


# COMMAND ----------

spark.sql("USE CATALOG healthrisk")
spark.sql("SELECT current_catalog()").show()
spark.sql("SELECT current_schema()").show()
def init_schemas(spark):
    for layer in ["bronze", "silver", "gold"]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS healthrisk.`{layer}`")

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

base = "/Workspace/Users/gfine886@gmail.com/healthrisk"

for d in dirs_needing_init:
    path = f"{base}/{d}/__init__.py"
    try:
        dbutils.fs.put(f"file:{path}", "", overwrite=False)
        print(f"created: {path}")
    except Exception as e:
        print(f"skip: {path} — {e}")

# COMMAND ----------

import sys
import importlib
sys.path.insert(0, "/Workspace/Repos/gfine886@gmail.com/healthrisk/healthrisk")
from src.ingestion import climate_ingestion
importlib.reload(climate_ingestion)

# COMMAND ----------

spark.sql("TRUNCATE TABLE bronze.tile_status")

# COMMAND ----------

importlib.reload(config)

# COMMAND ----------

test_tile_id, test_tile = tiles[0]
print(f"Testing tile: {test_tile_id}")
result = await climate_ingestion.process_one_tile(test_tile, test_tile_id)
print(f"Done! Shape: {result['df'].shape}")
print(result['df']['status'].value_counts())
print(result['df'][['lon', 'lat', 'error_message']].head(5))

# COMMAND ----------

print(result['df'][['longitude', 'latitude', 'error_message']].head(5))

# COMMAND ----------

import aiohttp

async def debug_one_point():
    params = {
        "parameters": config.PARAMETERS,
        "community": "AG", 
        "longitude": 100.0,
        "latitude": -8.0,
        "start": config.START_DATE,
        "end": config.END_DATE,
        "format": "JSON"
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(config.NASA_POINT_URL, params=params, timeout=aiohttp.ClientTimeout(total=60)) as resp:
            print(f"Status: {resp.status}")
            text = await resp.text()
            print(text[:500])

await debug_one_point()

# COMMAND ----------

import aiohttp

async def debug_south_america():
    params = {
        "parameters": "T2M,PRECTOTCORR,RH2M",
        "community": "AG",
        "longitude": -82.0,
        "latitude": -56.0,
        "start": "20170101",
        "end": "20241231",
        "format": "JSON"
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(
            "https://power.larc.nasa.gov/api/temporal/daily/point",
            params=params,
            timeout=aiohttp.ClientTimeout(total=180)
        ) as resp:
            print(f"Status: {resp.status}")
            text = await resp.text()
            print(text[:300])

await debug_south_america()

# COMMAND ----------

# ===== MAIN PIPELINE =====

from src.ingestion import climate_ingestion
from src.ingestion import dengue_ingestion
import config
import asyncio

import importlib
importlib.reload(climate_ingestion)

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
# build tiles for ALL regions
all_tiles = []
for region_name, bbox in config.REGIONS.items():
    region_tiles = climate_ingestion.build_tiles(
        bbox["lon_min"], bbox["lon_max"],
        bbox["lat_min"], bbox["lat_max"],
        config.TILE_LON_SIZE,
        config.TILE_LAT_SIZE
    )
    for tile in region_tiles:
        tile["region"] = region_name
    all_tiles.extend(region_tiles)
    print(f"[{region_name}] {len(region_tiles)} tiles")

print(f"[INFO] total tiles across all regions: {len(all_tiles)}")

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
    print(f"[INFO] Queuing {len(tiles)} tiles...")
    results = await climate_ingestion.run_all_tiles(tiles)
    print(results[0]["df"][["status", "error_message"]].value_counts())  

    all_dfs = [r["df"] for r in results]

    if all_dfs:
        import pandas as pd
        df = pd.concat(all_dfs, ignore_index=True)
        df["T2M"] = pd.to_numeric(df["T2M"], errors="coerce")
        df["PRECTOTCORR"] = pd.to_numeric(df["PRECTOTCORR"], errors="coerce")
        df["RH2M"] = pd.to_numeric(df["RH2M"], errors="coerce")
        spark_df = spark.createDataFrame(df)
        spark_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable("bronze.nasa_weather")


    #write checkpoint
    checkpoint_df = spark.createDataFrame(
    [(tile_id, "DONE") for tile_id, _ in tiles],
    ["tile_id", "status"]
    )

    checkpoint_df.write \
        .mode("append") \
        .saveAsTable("bronze.tile_status")
    

# COMMAND ----------

spark.sql("SELECT count(*) FROM bronze.nasa_weather").show()
