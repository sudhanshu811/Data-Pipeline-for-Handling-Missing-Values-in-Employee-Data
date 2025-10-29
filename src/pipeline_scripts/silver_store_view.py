import os
import pandas as pd
from src.utils.imputation_models import ImputationModels

def transform_bronze_to_silver_with_metadata(bronze_file: str, silver_dir: str) -> str:
    df = pd.read_parquet(bronze_file)

    imputer = ImputationModels()
    df_silver = imputer.fit_and_impute(df)

    os.makedirs(silver_dir, exist_ok=True)
    silver_file = os.path.join(silver_dir, os.path.basename(bronze_file).replace(".parquet", "_silver.parquet"))
    df_silver.to_parquet(silver_file, index=False)
    return silver_file
