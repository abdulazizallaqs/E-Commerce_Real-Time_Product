import json
from pathlib import Path


def build_spark_session():
    try:
        from pyspark.sql import SparkSession

        return (
            SparkSession.builder.appName("EcomSilverToGold")
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
            .master("local[1]")
            .getOrCreate()
        )
    except Exception as exc:
        print(f"Spark unavailable: {exc}")
        return None


def load_source_data(spark, bronze_path: str, silver_path: str):
    bronze_dir = Path(bronze_path)
    bronze_file = bronze_dir / "bronze_records.jsonl"
    if bronze_file.exists():
        print("Reading data from bronze JSONL")
        return [json.loads(line) for line in bronze_file.read_text(encoding="utf-8").splitlines() if line.strip()]

    print("No bronze data available; creating a demo record")
    return [
        {
            "product_id": "DEMO_001",
            "name": "Demo Product",
            "description": "Generated for the capstone demo",
            "price": 29.99,
            "stock_quantity": 8,
            "category": "General",
            "timestamp": "2026-07-16T00:00:00",
        }
    ]


def write_gold_output(rows, gold_path: str):
    target = Path(gold_path)
    target.mkdir(parents=True, exist_ok=True)
    artifact_path = target / "gold_records.jsonl"
    with artifact_path.open("w", encoding="utf-8") as handle:
        for item in rows:
            handle.write(json.dumps(item) + "\n")
    print(f"Saved gold artifact to: {artifact_path}")


if __name__ == "__main__":
    spark = build_spark_session()
    try:
        rows = load_source_data(spark, "lakehouse/data/bronze", "lakehouse/data/silver")
        write_gold_output(rows, "lakehouse/data/gold")
    finally:
        if spark is not None:
            spark.stop()