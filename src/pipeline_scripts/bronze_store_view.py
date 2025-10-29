import os
import pandas as pd

def extract_and_store_bronze(raw_file_path: str, bronze_dir: str) -> str:
    df = pd.read_csv(raw_file_path)

    # Drop rows with missing ID
    if 'id' in df.columns:
        df = df[df['id'].notna()]

    # Fill missing names with #####
    if 'name' in df.columns:
        df['name'] = df['name'].fillna("N/A")

    os.makedirs(bronze_dir, exist_ok=True)
    bronze_file = os.path.join(bronze_dir, os.path.basename(raw_file_path).replace(".csv", ".parquet"))
    df.to_parquet(bronze_file, index=False)
    return bronze_file
