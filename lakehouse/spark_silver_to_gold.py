import json
from pathlib import Path


def build_spark_session():
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

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


def build_schema(spark):
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
    return StructType([
        StructField("product_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("stock_quantity", IntegerType(), True),
        StructField("category", StringType(), True),
        StructField("timestamp", StringType(), True),
    ])


def load_source_data(spark, bronze_path: str, silver_path: str):
    bronze_dir = Path(bronze_path)
    bronze_file = bronze_dir / "bronze_records.jsonl"
    if bronze_file.exists():
        print("Reading data from bronze JSONL")
        rows = [json.loads(line) for line in bronze_file.read_text(encoding="utf-8").splitlines() if line.strip()]
        if spark is not None:
            try:
                schema = build_schema(spark)
                df = spark.createDataFrame([(item.get("product_id"), item.get("name"), item.get("description"), item.get("price"), item.get("stock_quantity"), item.get("category"), item.get("timestamp")) for item in rows], schema=schema)
                silver_path_value = Path(silver_path)
                silver_path_value.mkdir(parents=True, exist_ok=True)
                df.dropDuplicates(["product_id"]).write.format("delta").mode("overwrite").option("mergeSchema", "false").save(str(silver_path_value))
                print(f"Saved silver Delta data to: {silver_path_value}")
            except Exception as exc:
                print(f"Silver write skipped: {exc}")
        return rows

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


def write_gold_output(rows, gold_path: str, spark=None):
    target = Path(gold_path)
    target.mkdir(parents=True, exist_ok=True)
    artifact_path = target / "gold_records.jsonl"
    with artifact_path.open("w", encoding="utf-8") as handle:
        for item in rows:
            handle.write(json.dumps(item) + "\n")
    print(f"Saved gold artifact to: {artifact_path}")
    if spark is not None:
        try:
            from delta.tables import DeltaTable
            schema = build_schema(spark)
            df = spark.createDataFrame([(item.get("product_id"), item.get("name"), item.get("description"), item.get("price"), item.get("stock_quantity"), item.get("category"), item.get("timestamp")) for item in rows], schema=schema)
            target_path = str(target)
            if not Path(target_path + "/_delta_log").exists():
                df.write.format("delta").mode("overwrite").option("mergeSchema", "false").save(target_path)
            else:
                delta_table = DeltaTable.forPath(spark, target_path)
                delta_table.alias("t").merge(df.alias("s"), "t.product_id = s.product_id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            print(f"Saved gold Delta data to: {target_path}")
        except Exception as exc:
            print(f"Gold merge skipped: {exc}")


if __name__ == "__main__":
    spark = build_spark_session()
    try:
        rows = load_source_data(spark, "lakehouse/data/bronze", "lakehouse/data/silver")
        write_gold_output(rows, "lakehouse/data/gold", spark=spark)
    finally:
        if spark is not None:
            spark.stop()