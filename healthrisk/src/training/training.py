from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from config import *

COUNTRY_GRID_BOUNDS = {
    "PHILIPPINES":  {"lon": (117, 127), "lat": (5, 20)},
    "VIET NAM":     {"lon": (102, 110), "lat": (8, 24)},
    "INDONESIA":    {"lon": (95, 141),  "lat": (-11, 6)},
    "THAILAND":     {"lon": (97, 106),  "lat": (5, 21)},
    "MALAYSIA":     {"lon": (99, 119),  "lat": (1, 8)},
    "MYANMAR":      {"lon": (92, 102),  "lat": (10, 28)},
    "CAMBODIA":     {"lon": (102, 108), "lat": (10, 15)},
    "LAO PEOPLE'S DEMOCRATIC REPUBLIC": {"lon": (100, 107), "lat": (14, 23)},

    "BRAZIL":               {"lon": (-74, -34), "lat": (-34, 6)},
    "COLOMBIA":             {"lon": (-79, -66), "lat": (-5, 13)},
    "PERU":                 {"lon": (-82, -68), "lat": (-19, 1)},
    "BOLIVIA":              {"lon": (-70, -57), "lat": (-23, -9)},
    "ECUADOR":              {"lon": (-81, -75), "lat": (-5, 2)},
    "VENEZUELA":            {"lon": (-74, -60), "lat": (1, 13)},
    "PARAGUAY":             {"lon": (-63, -54), "lat": (-28, -19)},
    "ARGENTINA":            {"lon": (-66, -53), "lat": (-40, -22)},
    "MEXICO":               {"lon": (-118, -86), "lat": (14, 33)},
    "COSTA RICA":           {"lon": (-86, -82), "lat": (8, 12)},
    "HONDURAS":             {"lon": (-90, -83), "lat": (13, 16)},
    "NICARAGUA":            {"lon": (-88, -83), "lat": (11, 15)},
    "PANAMA":               {"lon": (-83, -77), "lat": (7, 10)},
    "GUATEMALA":            {"lon": (-93, -88), "lat": (13, 18)},
    "EL SALVADOR":          {"lon": (-91, -87), "lat": (13, 15)},
    "DOMINICAN REPUBLIC":   {"lon": (-75, -68), "lat": (17, 20)},
}

def build_training_dataset(spark: SparkSession):
    
    climate = spark.table(GOLD_CLIMATE_TABLE)
    dengue = spark.table("silver.dengue_features")

    df = climate.join(dengue, on=["grid_id", "iso_week"], how="inner")

    (df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(GOLD_TRAINING_TABLE))

    count = spark.table(GOLD_TRAINING_TABLE).count()
    print(f"[gold.training_dataset] rows written: {count:,}")
    return count