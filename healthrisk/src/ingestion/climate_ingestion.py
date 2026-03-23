# ingestion.py

# =========================
# IMPORTS
# =========================
import pandas as pd 
import asyncio
import aiohttp
from datetime import datetime, UTC
from config import *

# =========================
# GLOBAL SEMAPHORE（限流）
# =========================
semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
retry_count = 0
fail_count = 0
# =========================
# 1. REGION TILING
# =========================
def build_tiles(lon_min, lon_max, lat_min, lat_max, tile_lon_size, tile_lat_size):
    tiles = []
    lon = lon_min

    while lon < lon_max:
        next_lon = min(lon + tile_lon_size, lon_max)
        lat = lat_min

        while lat < lat_max:
            next_lat = min(lat + tile_lat_size, lat_max)

            tiles.append({
                "lon_min": round(lon, 4),
                "lon_max": round(next_lon, 4),
                "lat_min": round(lat, 4),
                "lat_max": round(next_lat, 4),
            })

            lat = next_lat
        lon = next_lon

    return tiles


# =========================
# 2. GRID GENERATION
# =========================
def frange(start, stop, step):
    values = []
    x = start

    while x < stop + 1e-9:
        values.append(round(x, 4))
        x += step

    return values


def build_grid_points_for_tile(tile, grid_step):
    lons = frange(tile["lon_min"], tile["lon_max"] - grid_step, grid_step)
    lats = frange(tile["lat_min"], tile["lat_max"] - grid_step, grid_step)

    points = []
    for lon in lons:
        for lat in lats:
            points.append((round(lon, 4), round(lat, 4)))

    return points


# =========================
# 3. ASYNC FETCH
# =========================
async def fetch_point(session, lon, lat):
    global retry_count
    params = {
        "parameters": PARAMETERS,
        "community": "AG",
        "longitude": lon,
        "latitude": lat,
        "start": START_DATE,
        "end": END_DATE,
        "format": "JSON"
    }

    async with semaphore:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with session.get(NASA_POINT_URL, params=params, timeout=REQUEST_TIMEOUT) as resp:

                    if resp.status == 200:
                        data = await resp.json()
                        return {"lon": lon, "lat": lat, "data": data, "error": None}

                    if resp.status in [429, 500, 502, 503, 504]:
                        retry_count += 1

                        wait = RETRY_BACKOFF_BASE ** (attempt - 1)
                        await asyncio.sleep(wait)
                        continue

                    text = await resp.text()
                    return {
                        "lon": lon,
                        "lat": lat,
                        "data": None,
                        "error": f"HTTP {resp.status}: {text[:200]}"
                    }

            except Exception as e:
                retry_count += 1
                wait = RETRY_BACKOFF_BASE ** (attempt - 1)
                await asyncio.sleep(wait)

        retry_count += 1

        return {
            "lon": lon,
            "lat": lat,
            "data": None,
            "error": "max retries exceeded"
        }


async def fetch_tile_points(tile_points):
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENCY)

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_point(session, lon, lat) for lon, lat in tile_points]
        results = await asyncio.gather(*tasks)

    return results


# =========================
# 4. NORMALIZATION
# =========================
def normalize_point_result(result):
    lon = result["lon"]
    lat = result["lat"]

    if result["error"] or result["data"] is None:
        return pd.DataFrame([{
            "date": None,
            "longitude": lon,
            "latitude": lat,
            "grid_id": f"{lon}_{lat}",
            "T2M": None,
            "PRECTOTCORR": None,
            "RH2M": None,
            "ingestion_ts": datetime.now(UTC).isoformat(),
            "status": "FAILED",
            "error_message": result["error"]
        }])

    try:
        param = result["data"]["properties"]["parameter"]

        t2m = param.get("T2M", {})
        prcp = param.get("PRECTOTCORR", {})
        rh2m = param.get("RH2M", {})

        dates = sorted(set(t2m) | set(prcp) | set(rh2m))

        rows = []
        for d in dates:
            rows.append({
                "date": d,
                "longitude": lon,
                "latitude": lat,
                "grid_id": f"{lon}_{lat}",
                "T2M": t2m.get(d),
                "PRECTOTCORR": prcp.get(d),
                "RH2M": rh2m.get(d),
                "ingestion_ts": datetime.now(UTC).isoformat(),
                "status": "SUCCESS",
                "error_message": None
            })

        return pd.DataFrame(rows)

    except Exception as e:
        return pd.DataFrame([{
            "date": None,
            "longitude": lon,
            "latitude": lat,
            "grid_id": f"{lon}_{lat}",
            "T2M": None,
            "PRECTOTCORR": None,
            "RH2M": None,
            "ingestion_ts": datetime.now(UTC).isoformat(),
            "status": "FAILED",
            "error_message": str(e)
        }])


# =========================
# 5. TILE PROCESSING
# =========================
async def process_one_tile(tile, tile_idx):
    tile_points = build_grid_points_for_tile(tile, GRID_STEP)

    results = await fetch_tile_points(tile_points)

    dfs = [normalize_point_result(r) for r in results]
    tile_df = pd.concat(dfs, ignore_index=True)

    tile_df["tile_id"] = tile_idx

    return {
        "tile_id": tile_idx,
        "df": tile_df
    }


# =========================
# 6. MAIN PIPELINE
# =========================
async def run_all_tiles(tiles):
    tasks = [
        process_one_tile(tile, tile_id)
        for tile_id, tile in tiles
    ]

    return await asyncio.gather(*tasks)