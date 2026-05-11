# config.py

NASA_POINT_URL = "https://power.larc.nasa.gov/api/temporal/daily/point"

PARAMETERS = "T2M,PRECTOTCORR,RH2M"
START_DATE = "20170101"
END_DATE = "20241231"

GRID_STEP = 1.0

TILE_LON_SIZE = 5
TILE_LAT_SIZE = 5
MAX_CONCURRENCY = 10
REQUEST_TIMEOUT = 180
MAX_RETRIES = 5
RETRY_BACKOFF_BASE = 3

BRONZE_TABLE = "bronze.nasa_weather"

# ── Multi-region bounding boxes ──────────────────
REGIONS = {
    "SEA": {
        "lon_min": 100,
        "lon_max": 125,
        "lat_min": -8,
        "lat_max": 20,
    },
    "SOUTH_AMERICA": {
        "lon_min": -82,
        "lon_max": -34,
        "lat_min": -56,
        "lat_max":  13,
    },
}

# Legacy single-region aliases (kept so existing code doesn't break)
LON_MIN = REGIONS["SEA"]["lon_min"]
LON_MAX = REGIONS["SEA"]["lon_max"]
LAT_MIN = REGIONS["SEA"]["lat_min"]
LAT_MAX = REGIONS["SEA"]["lat_max"]

# ── Feature Engineering ──────────────────────────

SILVER_CLIMATE_TABLE = "silver.climate_features"
GOLD_CLIMATE_TABLE   = "gold.climate_features_vp"
GOLD_TRAINING_TABLE  = "gold.training_dataset"

DATE_FORMAT = "yyyyMMdd"

OUTBREAK_LAG_WEEKS = 2    # cases_lag_14d
OUTBREAK_ROLL_WEEKS = 4   # cases_roll28