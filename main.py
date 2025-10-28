import argparse
from etl.extract import extract_data
from etl.transform import transform_data
from etl.validate import col_rename, coll_del
from etl.load import load_df
import os

def run_pipeline(raw_file: str, db_path: str):
    print("=== START ETL PIPELINE ===\n")

    temp_dir = os.path.join(os.path.dirname(raw_file), "intermediate")
    os.makedirs(temp_dir, exist_ok=True)

    # Extract
    print("--- Extract ---")
    df = extract_data(file_path=raw_file)
    df.to_parquet(os.path.join(temp_dir, "01_extract.parquet"), engine="pyarrow")

    # Transform
    print("--- Transform ---")
    df = transform_data(df)
    df.to_parquet(os.path.join(temp_dir, "02_transform.parquet"), engine="pyarrow")

    # Validate
    print("--- Validate ---")
    df = col_rename(df)
    df = coll_del(df)
    df.to_parquet(os.path.join(temp_dir, "03_validate.parquet"), engine="pyarrow")

    # Load
    print("--- Load ---")
    load_df(df, db_path=db_path)

    print("\n=== ETL PIPELINE FINISHED ===")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run ETL pipeline")
    parser.add_argument("--raw_file", required=True, help="Path to raw TSV file")
    parser.add_argument("--db_path", required=True, help="Path to SQLite creds.db file")
    args = parser.parse_args()

    run_pipeline(raw_file=args.raw_file, db_path=args.db_path)
