# config.py

NASA_POINT_URL = "https://power.larc.nasa.gov/api/temporal/daily/point"

PARAMETERS = "T2M,PRECTOTCORR,RH2M"
START_DATE = "20170101"
END_DATE = "20260228"

GRID_STEP = 1.0
LON_MIN = 100
LON_MAX = 125
LAT_MIN = -8
LAT_MAX = 20

TILE_LON_SIZE = 5
TILE_LAT_SIZE = 5
MAX_CONCURRENCY = 10
REQUEST_TIMEOUT = 60
MAX_RETRIES = 4
RETRY_BACKOFF_BASE = 2

BRONZE_TABLE = "bronze.nasa_weather"

# ── Feature Engineering ──────────────────────────

SILVER_CLIMATE_TABLE = "silver.climate_features"
GOLD_CLIMATE_TABLE   = "gold.climate_features_vp"
GOLD_TRAINING_TABLE  = "gold.training_dataset"

DATE_FORMAT = "yyyyMMdd"

OUTBREAK_LAG_WEEKS = 2    # cases_lag_14d
OUTBREAK_ROLL_WEEKS = 4   # cases_roll28