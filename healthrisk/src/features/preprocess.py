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

    # ── ARGENTINA ──
    "BUENOS AIRES":        (-36.68, -60.56),
    "CATAMARCA":           (-28.47, -65.78),
    "CHACO":               (-27.43, -61.16),
    "CHUBUT":              (-43.29, -65.11),
    "CORDOBA":             (-31.40, -64.18),
    "CORRIENTES":          (-29.42, -58.82),
    "ENTRE RIOS":          (-31.78, -60.50),
    "FORMOSA":             (-24.89, -59.79),
    "JUJUY":               (-23.21, -65.30),
    "LA PAMPA":            (-36.62, -64.29),
    "LA RIOJA":            (-29.41, -66.86),
    "MENDOZA":             (-34.83, -68.84),
    "MISIONES":            (-27.37, -55.95),
    "NEUQUEN":             (-38.95, -68.07),
    "RIO NEGRO":           (-40.82, -63.02),
    "SALTA":               (-24.80, -65.42),
    "SAN JUAN":            (-30.87, -68.89),
    "SAN LUIS":            (-33.30, -66.34),
    "SANTA CRUZ":          (-51.64, -69.25),
    "SANTA FE":            (-30.71, -60.95),
    "SANTIAGO DEL ESTERO": (-27.78, -64.27),
    "TIERRA DEL FUEGO":    (-54.43, -67.62),
    "TUCUMAN":             (-26.82, -65.22),

    # ── BOLIVIA ──
    "BENI":       (-14.48, -65.10),
    "CHUQUISACA": (-20.02, -64.36),
    "COCHABAMBA": (-17.39, -65.76),
    "LA PAZ":     (-16.50, -68.15),
    "ORURO":      (-18.47, -67.11),
    "PANDO":      (-11.03, -67.62),
    "POTOSI":     (-19.98, -65.79),
    "SANTA CRUZ": (-17.80, -63.17),
    "TARIJA":     (-21.53, -63.73),

    # ── COLOMBIA ──
    "AMAZONAS":        (-1.44, -71.57),
    "ANTIOQUIA":       (7.20, -75.34),
    "ARAUCA":          (6.54, -71.00),
    "ATLANTICO":       (10.69, -74.87),
    "BOLIVAR":         (8.67, -74.03),
    "BOYACA":          (5.45, -73.36),
    "CALDAS":          (5.30, -75.27),
    "CAQUETA":         (1.01, -73.79),
    "CASANARE":        (5.76, -71.57),
    "CAUCA":           (2.53, -76.62),
    "CESAR":           (9.33, -73.54),
    "CHOCO":           (5.69, -76.66),
    "CUNDINAMARCA":    (4.60, -74.08),
    "GUAINIA":         (2.58, -68.53),
    "GUAJIRA":         (11.35, -72.52),
    "GUAVIARE":        (2.57, -72.65),
    "HUILA":           (2.54, -75.53),
    "MAGDALENA":       (10.41, -74.41),
    "META":            (3.99, -73.13),
    "NARINO":          (1.29, -77.36),
    "NORTE SANTANDER": (7.94, -72.50),
    "PUTUMAYO":        (0.44, -75.52),
    "QUINDIO":         (4.46, -75.67),
    "RISARALDA":       (5.31, -75.99),
    "SAN ANDRES":      (12.53, -81.72),
    "SANTANDER":       (6.64, -73.65),
    "SUCRE":           (9.30, -75.40),
    "TOLIMA":          (4.09, -75.15),
    "VALLE":           (3.80, -76.51),
    "VAUPES":          (0.86, -70.81),
    "VICHADA":         (4.42, -69.28),

    # ── COSTA RICA ──
    "ALAJUELA":    (10.39, -84.44),
    "CARTAGO":     (9.86,  -83.92),
    "GUANACASTE":  (10.54, -85.35),
    "HEREDIA":     (10.47, -84.02),
    "LIMON":       (9.99,  -83.03),
    "PUNTARENAS":  (9.98,  -84.83),
    "SAN JOSE":    (9.93,  -84.08),

    # ── ECUADOR ──
    "AZUAY":            (-2.90, -78.99),
    "BOLIVAR":          (-1.59, -79.00),
    "CANAR":            (-2.56, -78.94),
    "CARCHI":           (0.50,  -77.92),
    "CHIMBORAZO":       (-1.67, -78.65),
    "COTOPAXI":         (-0.93, -78.62),
    "EL ORO":           (-3.26, -79.96),
    "ESMERALDAS":       (0.96,  -79.65),
    "GALAPAGOS":        (-0.95, -90.97),
    "GUAYAS":           (-1.83, -79.52),
    "IMBABURA":         (0.35,  -78.12),
    "LOJA":             (-4.00, -79.20),
    "LOS RIOS":         (-1.02, -79.46),
    "MANABI":           (-1.05, -80.45),
    "MORONA SANTIAGO":  (-2.30, -78.11),
    "NAPO":             (-0.99, -77.81),
    "ORELLANA":         (-0.46, -76.99),
    "PASTAZA":          (-1.49, -77.30),
    "PICHINCHA":        (-0.18, -78.47),
    "SUCUMBIOS":        (0.09,  -76.89),
    "TUNGURAHUA":       (-1.26, -78.57),
    "ZAMORA CHINCHIPE": (-4.07, -78.95),

    # ── GUATEMALA ──
    "ALTA VERAPAZ":           (15.71, -90.22),
    "BAJA VERAPAZ":           (15.12, -90.37),
    "CHIMALTENANGO":          (14.66, -90.82),
    "CHIQUIMULA":             (14.80, -89.55),
    "EL PROGRESO":            (14.94, -89.87),
    "EL QUICHE":              (15.49, -91.15),
    "ESCUINTLA":              (14.31, -90.79),
    "GUATEMALA CENTRAL":      (14.64, -90.51),
    "GUATEMALA NOR OCCIDENTE":(14.70, -90.60),
    "GUATEMALA NOR ORIENTE":  (14.70, -90.45),
    "GUATEMALA NOROCCIDENTE": (14.70, -90.60),
    "GUATEMALA NORORIENTE":   (14.70, -90.45),
    "GUATEMALA SUR":          (14.55, -90.55),
    "HUEHUETENANGO":          (15.32, -91.47),
    "IXCAN":                  (15.88, -91.07),
    "IXIL":                   (15.52, -91.18),
    "IZABAL":                 (15.50, -88.86),
    "JALAPA":                 (14.63, -89.99),
    "JUTIAPA":                (14.29, -89.90),
    "PETEN NORTE":            (17.25, -90.03),
    "PETEN SUR OCCIDENTAL":   (16.60, -90.18),
    "PETEN SUR ORIENTAL":     (16.60, -89.80),
    "PETEN SUROCCIDENTAL":    (16.60, -90.18),
    "PETEN SURORIENTAL":      (16.60, -89.80),
    "QUETZALTENANGO":         (14.83, -91.52),
    "RETALHULEU":             (14.53, -91.69),
    "SACATEPEQUEZ":           (14.56, -90.73),
    "SAN MARCOS":             (15.03, -91.80),
    "SANTA ROSA":             (14.22, -90.30),
    "SOLOLA":                 (14.77, -91.18),
    "SUCHITEPEQUEZ":          (14.42, -91.40),
    "TOTONICAPAN":            (15.00, -91.36),
    "ZACAPA":                 (14.97, -89.53),

    # ── MEXICO ──
    "AGUASCALIENTES":    (21.88, -102.29),
    "BAJA CALIFORNIA":   (30.84, -115.28),
    "BAJA CALIFORNIA SUR":(25.97, -111.66),
    "CAMPECHE":          (19.83, -90.53),
    "CHIAPAS":           (16.76, -93.11),
    "CHIHUAHUA":         (28.63, -106.07),
    "COAHUILA":          (27.06, -101.71),
    "COLIMA":            (19.24, -103.72),
    "DISTRITO FEDERAL":  (19.43, -99.13),
    "DURANGO":           (24.53, -104.66),
    "GUANAJUATO":        (20.92, -101.05),
    "GUERRERO":          (17.56, -100.08),
    "HIDALGO":           (20.49, -98.99),
    "JALISCO":           (20.66, -103.35),
    "MEXICO":            (19.36, -99.66),
    "MICHOACAN":         (19.57, -101.74),
    "MORELOS":           (18.68, -99.10),
    "NAYARIT":           (21.75, -104.85),
    "NUEVO LEON":        (25.59, -99.99),
    "OAXACA":            (17.07, -96.72),
    "PUEBLA":            (19.04, -98.21),
    "QUERETARO":         (20.59, -100.39),
    "QUINTANA ROO":      (19.18, -88.48),
    "SAN LUIS POTOSI":   (22.16, -100.98),
    "SINALOA":           (25.19, -107.65),
    "SONORA":            (29.29, -110.31),
    "TABASCO":           (17.99, -92.93),
    "TAMAULIPAS":        (24.27, -98.84),
    "TLAXCALA":          (19.32, -98.24),
    "VERACRUZ":          (19.18, -96.14),
    "YUCATAN":           (20.97, -89.62),
    "ZACATECAS":         (23.02, -102.57),

    # ── NICARAGUA ──
    "BILWI":                              (14.04, -83.39),
    "BOACO":                              (12.47, -85.66),
    "CARAZO":                             (11.73, -86.22),
    "CHINANDEGA":                         (12.63, -87.13),
    "CHONTALES":                          (11.94, -85.19),
    "ESTELI":                             (13.08, -86.36),
    "GRANADA":                            (11.93, -85.96),
    "JINOTEGA":                           (13.09, -85.99),
    "LEON":                               (12.44, -86.88),
    "MADRIZ":                             (13.47, -86.46),
    "MANAGUA":                            (12.15, -86.28),
    "MASAYA":                             (11.97, -86.10),
    "MATAGALPA":                          (12.91, -85.52),
    "NUEVA SEGOVIA":                      (13.74, -86.11),
    "REGION AUTONOMA DEL ATLANTICO SUR":  (12.00, -84.00),
    "RIO SAN JUAN":                       (11.09, -84.77),
    "RIVAS":                              (11.44, -85.84),
    "ZELAYA CENTRAL":                     (13.00, -85.00),

    # ── PANAMA ──
    "BOCAS DEL TORO": (9.40,  -82.44),
    "CHIRIQUI":       (8.57,  -82.35),
    "COCLE":          (8.63,  -80.36),
    "COLON":          (9.36,  -79.90),
    "DARIEN":         (7.73,  -77.72),
    "HERRERA":        (7.78,  -80.72),
    "KUNA YALA":      (9.20,  -78.30),
    "LOS SANTOS":     (7.61,  -80.42),
    "NGOBE BUGLE":    (8.66,  -81.77),
    "PANAMA":         (8.99,  -79.52),
    "VERAGUAS":       (8.01,  -81.07),

    # ── PERU ──
    "AMAZONAS":   (-5.28,  -78.10),
    "ANCASH":     (-9.53,  -77.53),
    "AREQUIPA":   (-16.41, -71.54),
    "AYACUCHO":   (-13.16, -74.22),
    "CAJAMARCA":  (-7.16,  -78.51),
    "CALLAO":     (-12.06, -77.13),
    "CUSCO":      (-13.53, -71.97),
    "HUANUCO":    (-9.93,  -76.24),
    "ICA":        (-14.07, -75.73),
    "JUNIN":      (-11.16, -75.01),
    "LA LIBERTAD":(-8.12,  -78.49),
    "LAMBAYEQUE": (-6.70,  -79.91),
    "LIMA":       (-12.05, -77.03),

    
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

    "BRAZIL":             (-14.24, -51.93),
    "COLOMBIA":           (4.57,   -74.30),
    "PERU":               (-9.19,  -75.02),
    "BOLIVIA":            (-16.29, -63.59),
    "ECUADOR":            (-1.83,  -78.18),
    "VENEZUELA":          (6.42,   -66.59),
    "PARAGUAY":           (-23.44, -58.44),
    "ARGENTINA":          (-34.61, -58.44),
    "MEXICO":             (23.63,  -102.55),
    "COSTA RICA":         (9.75,   -83.75),
    "HONDURAS":           (15.20,  -86.24),
    "NICARAGUA":          (12.87,  -85.21),
    "PANAMA":             (8.99,   -79.52),
    "GUATEMALA":          (15.78,  -90.23),
    "EL SALVADOR":        (13.79,  -88.90),
    "DOMINICAN REPUBLIC": (18.74,  -70.16),
}

def snap_to_grid(lat, lon, step=1.0):
    import math
    snapped_lat = float(math.floor(lat / step) * step)
    snapped_lon = float(math.floor(lon / step) * step)
    return f"{snapped_lon}_{snapped_lat}"

def build_dengue_features(spark: SparkSession):
    df = spark.table("bronze.dengue_raw")

    df = (df
        .withColumn("iso_week", F.date_trunc("week", F.col("calendar_start_date")))
        .filter(F.col("iso_week") >= "2017-01-01")
    )

    # province lookup
    province_rows = [
        (province, float(lat), float(lon), snap_to_grid(lat, lon))
        for province, (lat, lon) in PROVINCE_CENTROIDS.items()
    ]
    province_df = spark.createDataFrame(
        province_rows, ["adm_1_name", "centroid_lat", "centroid_lon", "grid_id"]
    )

    # country lookup
    country_rows = [
        (country, float(lat), float(lon), snap_to_grid(lat, lon))
        for country, (lat, lon) in COUNTRY_CENTROIDS.items()
    ]
    country_df = spark.createDataFrame(
        country_rows, ["adm_0_name", "centroid_lat", "centroid_lon", "grid_id"]
    )

    # join by province first
    df_province = (df.filter(F.col("adm_1_name") != "NA")
        .join(F.broadcast(province_df), on="adm_1_name", how="inner"))

    # fallback to country
    df_country = (df.filter(F.col("adm_1_name") == "NA")
        .join(F.broadcast(country_df), on="adm_0_name", how="inner"))

    df = df_province.unionByName(df_country, allowMissingColumns=True)

    df = df.filter(F.col("dengue_total") >= 0).filter(F.col("dengue_total") < 500000)

    # ──  aggregation instead of dropDuplicates ──
    # grid_id
    df = (df
        .groupBy("grid_id", "iso_week", "adm_0_name")
        .agg(
            F.sum("dengue_total").alias("dengue_total"),
            F.first("centroid_lat").alias("centroid_lat"),
            F.first("centroid_lon").alias("centroid_lon"),
        )
    )

    # ── upsampling week ──
   
    df = df.withColumn("dengue_weekly", F.col("dengue_total") / 4.0)

    # lag/roll（iso_week->  year_month）
    w = Window.partitionBy("grid_id").orderBy("iso_week")
    df = (df
        .withColumn("cases_lag_14d", F.lag("dengue_weekly", OUTBREAK_LAG_WEEKS).over(w))
        .withColumn("cases_lag_7d",   F.lag("dengue_weekly", 1).over(w))   # 新增：1周前
        .withColumn("cases_lag_21d",  F.lag("dengue_weekly", 3).over(w))   # 新增：3周前
        .withColumn("cases_roll28", F.avg("dengue_weekly").over(w.rowsBetween(-OUTBREAK_ROLL_WEEKS, -1)))
        .withColumn("cases_roll14",   F.avg("dengue_weekly").over(w.rowsBetween(-2, -1)))
        .withColumn("outbreak_label",
            (F.col("dengue_weekly") > F.col("cases_roll28") * 1.5).cast("int"))
        .withColumn("outbreak_lag1",  F.lag("outbreak_label", 1).over(w))
    )

    df = df.fillna({"cases_lag_14d": 0, "cases_roll28": 0, "outbreak_label": 0,"cases_lag_7d": 0,"cases_lag_21d": 0,"cases_roll14": 0,"outbreak_lag1": 0, })

    (df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("silver.dengue_features"))

    count = spark.table("silver.dengue_features").count()
    print(f"[silver.dengue_features] rows written: {count:,}")
    return count