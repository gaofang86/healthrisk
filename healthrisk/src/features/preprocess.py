from pyspark.sql import functions as F, SparkSession
from pyspark.sql.window import Window
from config import *

PROVINCE_CENTROIDS = {
    # indonesia
    "DKI JAKARTA": (-6.21, 106.85),
    "JAWA BARAT": (-6.90, 107.61),
    "JAWA TIMUR": (-7.54, 112.24),
    "JAWA TENGAH": (-7.15, 110.14),
    "SUMATERA UTARA": (2.07, 99.33),
    "SUMATERA SELATAN": (-3.32, 104.91),
    "SUMATERA BARAT": (-0.74, 100.24),
    "BALI": (-8.34, 115.09),
    "KALIMANTAN TIMUR": (0.54, 116.42),
    "KALIMANTAN BARAT": (0.13, 109.34),
    "KALIMANTAN SELATAN": (-3.09, 115.28),
    "KALIMANTAN TENGAH": (-1.68, 113.38),
    "SULAWESI SELATAN": (-3.67, 119.97),
    "SULAWESI UTARA": (1.49, 124.84),
    "SULAWESI TENGAH": (-1.43, 121.44),
    "SULAWESI TENGGARA": (-4.14, 122.17),
    "SULAWESI BARAT": (-2.84, 119.23),
    "BANTEN": (-6.41, 106.02),
    "LAMPUNG": (-4.56, 105.41),
    "RIAU": (0.29, 101.71),
    "JAMBI": (-1.61, 103.61),
    "BENGKULU": (-3.79, 102.26),
    "ACEH": (4.70, 96.75),
    "PAPUA": (-4.27, 138.08),
    "PAPUA BARAT": (-1.34, 133.17),
    "MALUKU": (-3.24, 130.14),
    "MALUKU UTARA": (1.57, 127.81),
    "GORONTALO": (0.55, 123.06),
    "KEPULAUAN RIAU": (3.94, 108.14),
    "BANGKA BELITUNG": (-2.74, 106.44),
    "DI YOGYAKARTA": (-7.80, 110.36),
    "DAERAH ISTIMEWA YOGYAKARTA": (-7.80, 110.36),
    "D.I YOGYA": (-7.80, 110.36),
    "NUSA TENGGARA BARAT": (-8.65, 117.36),
    "NUSA TENGGARA TIMUR": (-8.66, 121.08),
    "NUSATENGGARA BARAT": (-8.65, 117.36),
    "NUSATENGGARA TIMUR": (-8.66, 121.08),
    "KALIMANTAN UTARA": (3.07, 116.04),
    "NANGGROE ACEH DARUSSALAM": (4.70, 96.75),
    "KEPULAUAN-RIAU": (3.94, 108.14),
    "KEPULAUAN BANGKA BELITUNG": (-2.74, 106.44),
    "BABEL": (-2.74, 106.44),
    "SUMATERA SELATA": (-3.32, 104.91),
    "KALIMANTAN SELATA": (-3.09, 115.28),
    "SULAWESI SELATA": (-3.67, 119.97),
    "PAPUA BARAT DAYA": (-1.34, 131.50),
    "PAPUA SELATAN": (-6.08, 140.62),
    "PAPUA TENGAH": (-3.99, 136.38),
    "PAPUA PEGUNUNGAN": (-4.02, 138.95),
    # malaysia
    "SELANGOR": (3.07, 101.52),
    "KUALA LUMPUR": (3.14, 101.69),
    "W.P. KUALA LUMPUR": (3.14, 101.69),
    "JOHOR": (1.86, 103.76),
    "PULAU PINANG": (5.41, 100.33),
    "SABAH": (5.98, 116.07),
    "SARAWAK": (1.55, 110.36),
    "PERAK": (4.59, 101.09),
    "PAHANG": (3.81, 103.33),
    "KELANTAN": (5.74, 102.03),
    "TERENGGANU": (5.31, 103.14),
    "NEGERI SEMBILAN": (2.73, 102.24),
    "MELAKA": (2.19, 102.25),
    "KEDAH": (6.12, 100.37),
    "PERLIS": (6.44, 100.19),
    "LABUAN": (5.28, 115.24),
    "W.P. LABUAN": (5.28, 115.24),
    # myanmar
    "YANGON": (16.87, 96.19),
    "MANDALAY": (21.98, 96.08),
    "SAGAING": (23.11, 95.75),
    "BAGO (WEST)": (17.33, 96.48),
    "BAGO (EAST)": (17.33, 96.48),
    "BAGO (W": (17.33, 96.48),
    "BAGO (E)": (17.33, 96.48),
    "MAGWAY": (20.15, 94.93),
    "AYEYARWADY": (16.91, 95.22),
    "AYAYARWADDY": (16.91, 95.22),
    "SHAN (NORTH)": (22.04, 98.13),
    "SHAN (SOUTH)": (19.77, 97.40),
    "SHAN (EAST)": (21.30, 100.62),
    "SHAN (N)": (22.04, 98.13),
    "SHAN (S)": (19.77, 97.40),
    "KACHIN": (25.85, 97.44),
    "KAYAH": (19.25, 97.24),
    "KAYIN": (17.14, 97.64),
    "MON": (16.30, 97.72),
    "RAKHINE": (20.10, 93.00),
    "CHIN": (22.01, 93.58),
    "TANINTHARYI": (13.06, 98.66),
    "NAY PYI TAW": (19.76, 96.07),
    "NAYPYITAW": (19.76, 96.07),
    "MONGAR": (27.27, 91.24),
    # vietnam
    "REGION IV-A (CALABARZON)": (14.10, 121.08),
    "REGION III (CENTRAL LUZON)": (15.48, 120.71),
    "REGION X (NORTHERN MINDANAO)": (8.02, 124.68),
    "REGION VIII (EASTERN VISAYAS)": (11.25, 125.00),
}

COUNTRY_CENTROIDS = {
    "CAMBODIA": (11.55, 104.92),
    "INDONESIA": (-2.5, 118.0),
    "LAO PEOPLE'S DEMOCRATIC REPUBLIC": (17.97, 102.63),
    "MALAYSIA": (4.21, 108.0),
    "MYANMAR": (19.74, 95.96),
    "PHILIPPINES": (12.88, 121.77),
    "THAILAND": (15.87, 100.99),
    "VIET NAM": (16.64, 106.30),
}

def snap_to_grid(lat, lon, step=1.0):
    import math
    snapped_lat = math.floor(lat / step) * step
    snapped_lon = math.floor(lon / step) * step
    lat_str = str(int(snapped_lat)) if snapped_lat == int(snapped_lat) else str(snapped_lat)
    lon_str = str(int(snapped_lon)) if snapped_lon == int(snapped_lon) else str(snapped_lon)
    return f"{lon_str}_{lat_str}"

def build_dengue_features(spark: SparkSession):
    df = spark.table("bronze.dengue_raw")

    df = (df
        .withColumn("year_month", F.date_trunc("month", F.col("calendar_start_date")))
        .filter(F.col("year_month") >= "2017-01-01")
    )

    # province lookup
    province_rows = [
        (province, float(lat), float(lon), snap_to_grid(lat, lon))
        for province, (lat, lon) in PROVINCE_CENTROIDS.items()
    ]
    province_df = spark.createDataFrame(
        province_rows, ["adm_1_name", "centroid_lat", "centroid_lon", "grid_id"]
    )

    # contry lookup
    country_rows = [
        (country, float(lat), float(lon), snap_to_grid(lat, lon))
        for country, (lat, lon) in COUNTRY_CENTROIDS.items()
    ]
    country_df = spark.createDataFrame(
        country_rows, ["adm_0_name", "centroid_lat", "centroid_lon", "grid_id"]
    )

    # join by province
    df_province = (df.filter(F.col("adm_1_name") != "NA")
        .join(F.broadcast(province_df), on="adm_1_name", how="inner"))

    # NA join by country
    df_country = (df.filter(F.col("adm_1_name") == "NA")
        .join(F.broadcast(country_df), on="adm_0_name", how="inner"))

    df = df_province.unionByName(df_country, allowMissingColumns=True)

    df = (df
        .filter(F.col("dengue_total") >= 0)
        .filter(F.col("dengue_total") < 500000)
        .dropDuplicates(["grid_id", "year_month"])
    )
    w = Window.partitionBy("grid_id").orderBy("year_month")
    df = (df
        .withColumn("cases_lag_14d", F.lag("dengue_total", OUTBREAK_LAG_WEEKS).over(w))
        .withColumn("cases_roll28", F.avg("dengue_total").over(w.rowsBetween(-OUTBREAK_ROLL_WEEKS, -1)))
        .withColumn("outbreak_label",
            (F.col("dengue_total") > F.col("cases_roll28") * 1.5).cast("int")))

    df = df.fillna({"cases_lag_14d": 0, "cases_roll28": 0, "outbreak_label": 0})

    (df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("silver.dengue_features"))

    count = spark.table("silver.dengue_features").count()
    print(f"[silver.dengue_features] rows written: {count:,}")
    return count