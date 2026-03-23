import pandas as pd
from pyspark.sql import SparkSession

SEA_COUNTRIES = [
    "PHILIPPINES", "VIET NAM", "INDONESIA", "THAILAND", "MALAYSIA",
    "MYANMAR", "CAMBODIA", "LAO PEOPLE'S DEMOCRATIC REPUBLIC"
]

def ingest_dengue(spark: SparkSession):
    df = spark.read.option("header", "true").csv(
        "/Volumes/workspace_7474658570712100/bronze/raw_data/Spatial_extract_V1_3.csv"
    ).toPandas()
    
    print("columns:", df.columns.tolist())
    
    df = df[df["adm_0_name"].isin(SEA_COUNTRIES)].copy()
    df["calendar_start_date"] = pd.to_datetime(df["calendar_start_date"])
    df = df[df["calendar_start_date"] >= "2017-01-01"]
    df = df[["adm_0_name", "adm_1_name", "calendar_start_date", "dengue_total"]].dropna()

    sdf = spark.createDataFrame(df)
    (sdf.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("bronze.dengue_raw"))
    
    count = sdf.count()
    print(f"[bronze.dengue_raw] rows written: {count:,}")
    return count