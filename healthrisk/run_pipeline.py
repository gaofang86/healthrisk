# run_pipeline.py

from src.ingestion.climate_ingestion import run as run_climate_ingestion
from src.ingestion.dengue_ingestion import run as run_dengue_ingestion

# 后面逐步加
# from src.features.feature_engineering import run as run_feature_engineering


def main():
    print("Starting pipeline...")

    run_climate_ingestion()
    run_dengue_ingestion()

    # run_feature_engineering()
    # run_training()
    # run_policy()

    print("Pipeline finished.")


if __name__ == "__main__":
    main()