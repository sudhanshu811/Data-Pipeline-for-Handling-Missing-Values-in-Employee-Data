import os
import pandas as pd
from src.pipeline_scripts.bronze_store_view import extract_and_store_bronze


def test_bronze_creation(tmp_path):
    raw_csv = tmp_path / "sample.csv"
    raw_csv.write_text("id,name,age,salary,department,number_of_years_worked\n1,John,29,50000,IT,5\n2,,34,65000,HR,8")

    bronze_file = extract_and_store_bronze(str(raw_csv), str(tmp_path))
    df = pd.read_parquet(bronze_file)
    assert "####" in df["name"].values
    assert -1 not in df["age"].values
