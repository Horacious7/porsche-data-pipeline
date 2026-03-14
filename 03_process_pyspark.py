import logging
import os
from typing import Tuple

from dotenv import load_dotenv
from azure.core.exceptions import (
    ClientAuthenticationError,
    HttpResponseError,
    ResourceNotFoundError,
)
from azure.storage.blob import BlobServiceClient
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, current_timestamp, lit, sum as spark_sum
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType


STORAGE_ACCOUNT_NAME = "datalakeporscheho"
BRONZE_CONTAINER = "1-bronze"
SILVER_CONTAINER = "2-silver"
GOLD_CONTAINER = "3-gold"
SILVER_PREFIX = "sales/"
GOLD_PREFIX = "model_metrics/"

BRONZE_PATH = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/"
BRONZE_JSON_GLOB_PATH = f"{BRONZE_PATH}*.json"
SILVER_PATH = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{SILVER_PREFIX}"
GOLD_PATH = f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{GOLD_PREFIX}"


def configure_logging() -> None:
    """Configure app-wide logging with timestamps and severity levels."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    logging.getLogger("azure").setLevel(logging.WARNING)
    logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)


def extract_account_key(connection_string: str) -> str:
    """Extract the storage account key from a standard Azure connection string."""
    for segment in connection_string.split(";"):
        if segment.startswith("AccountKey="):
            return segment.split("=", 1)[1]
    raise ValueError("Connection string does not contain an AccountKey segment.")


def get_connection_details() -> Tuple[str, str]:
    """Load and validate Azure credentials from environment variables."""
    load_dotenv()
    connection_string = os.getenv("AZURE_CONNECTION_STRING")
    if not connection_string:
        raise EnvironmentError("AZURE_CONNECTION_STRING is missing from the environment.")

    account_key = extract_account_key(connection_string)
    return connection_string, account_key


def create_spark_session(account_name: str, account_key: str) -> SparkSession:
    """Initialize SparkSession and configure direct ADLS Gen2 access."""
    builder = SparkSession.builder.appName("PorscheSalesMedallionPipeline")

    # Allow explicit package injection for runtimes that require external Azure jars.
    spark_jars_packages = os.getenv("SPARK_JARS_PACKAGES")
    if spark_jars_packages:
        builder = builder.config("spark.jars.packages", spark_jars_packages)

    spark = builder.getOrCreate()

    dfs_host = f"{account_name}.dfs.core.windows.net"
    spark.conf.set(f"fs.azure.account.auth.type.{dfs_host}", "SharedKey")
    spark.conf.set(f"fs.azure.account.key.{dfs_host}", account_key)

    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set(f"fs.azure.account.auth.type.{dfs_host}", "SharedKey")
    hadoop_conf.set(f"fs.azure.account.key.{dfs_host}", account_key)

    # validate_abfs_connector(spark)
    spark.sparkContext.setLogLevel("WARN")
    return spark


def validate_abfs_connector(spark: SparkSession) -> None:
    """Ensure the Spark runtime has ABFS filesystem classes available."""
    class_names = [
        "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem",
        "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem",
    ]
    jvm = spark.sparkContext._jvm

    for class_name in class_names:
        try:
            jvm.java.lang.Class.forName(class_name)
            logging.info("Detected ABFS connector class: %s", class_name)
            return
        except Exception:
            continue

    raise RuntimeError(
        "Spark runtime is missing ABFS filesystem classes. "
        "Run this script on a Spark server runtime with Azure connectors preinstalled "
        "(for example Databricks/Synapse), or provide compatible connector packages via "
        "SPARK_JARS_PACKAGES."
    )


def bronze_has_json_files(connection_string: str) -> bool:
    """Return True when Bronze contains at least one JSON blob."""
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(BRONZE_CONTAINER)

        for blob in container_client.list_blobs():
            if blob.name.lower().endswith(".json"):
                return True

        logging.info("Bronze container is reachable but contains no JSON blobs.")
        return False
    except ResourceNotFoundError:
        logging.error(
            "Bronze container '1-bronze' was not found in storage account '%s'.",
            STORAGE_ACCOUNT_NAME,
        )
        raise
    except (ClientAuthenticationError, HttpResponseError) as exc:
        logging.error(
            "Unable to access Bronze container due to authentication/authorization error: %s",
            exc,
        )
        raise
    except Exception:
        logging.exception("Unexpected error while checking Bronze container contents.")
        raise


def read_bronze_json(spark: SparkSession) -> DataFrame:
    """Read Bronze JSON files with multiline support and corrupt-record diagnostics."""
    bronze_df = (
        spark.read.option("multiLine", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(BRONZE_JSON_GLOB_PATH)
    )

    columns = set(bronze_df.columns)
    if "_corrupt_record" in columns:
        corrupt_count = bronze_df.filter("_corrupt_record IS NOT NULL").count()
        if corrupt_count > 0:
            logging.warning("Detected %s malformed Bronze JSON records.", corrupt_count)

    required_columns = {"sale_id", "vin_number", "model_name", "price"}
    if not required_columns.intersection(columns):
        raise ValueError(
            "Bronze JSON structure is invalid. Parsed columns do not contain expected fields "
            "(sale_id, vin_number, model_name, price). "
            "If files are pretty-printed JSON, ensure multiline parsing is enabled."
        )

    if "_corrupt_record" in columns:
        bronze_df = bronze_df.filter("_corrupt_record IS NULL").drop("_corrupt_record")

    if "price" in bronze_df.columns:
        bronze_df = bronze_df.withColumn("price", col("price").cast("double"))
    else:
        bronze_df = bronze_df.withColumn("price", lit(None).cast("double"))

    return bronze_df


def process_bronze_to_silver(spark: SparkSession) -> DataFrame:
    """Read Bronze JSON data, clean it, and write Silver Parquet data."""
    try:
        logging.info("Reading Bronze JSON files from %s", BRONZE_JSON_GLOB_PATH)
        bronze_df = read_bronze_json(spark)

        input_count = bronze_df.count()
        logging.info("Loaded %s records from Bronze.", input_count)

        if input_count == 0:
            logging.warning("Bronze input is empty. Writing empty Silver dataset.")
            empty_silver_df = bronze_df.withColumn("processed_timestamp", current_timestamp())
            empty_silver_df.write.mode("overwrite").parquet(SILVER_PATH)
            return empty_silver_df

        valid_price_df = bronze_df.filter(col("price") > 0)
        removed_by_price = input_count - valid_price_df.count()

        valid_fields_df = valid_price_df.filter(
            "vin_number IS NOT NULL AND TRIM(vin_number) <> '' "
            "AND sale_id IS NOT NULL AND TRIM(sale_id) <> '' "
            "AND model_name IS NOT NULL AND TRIM(model_name) <> ''"
        )
        removed_by_required_fields = valid_price_df.count() - valid_fields_df.count()

        pre_dedup_count = valid_fields_df.count()
        cleaned_df = valid_fields_df.dropDuplicates(["vin_number"]).withColumn(
            "processed_timestamp", current_timestamp()
        )
        removed_as_duplicates = pre_dedup_count - cleaned_df.count()

        cleaned_count = cleaned_df.count()
        logging.info(
            "Cleaning results | removed_by_price=%s | removed_by_required_fields=%s | removed_as_duplicates=%s | final_count=%s",
            removed_by_price,
            removed_by_required_fields,
            removed_as_duplicates,
            cleaned_count,
        )

        logging.info("Writing cleaned data to Silver path: %s", SILVER_PATH)
        cleaned_df.write.mode("overwrite").parquet(SILVER_PATH)
        return cleaned_df
    except Exception:
        logging.exception("Bronze to Silver processing failed.")
        raise


def process_silver_to_gold(spark: SparkSession) -> DataFrame:
    """Read Silver Parquet data, aggregate metrics, and write Gold Parquet data."""
    try:
        logging.info("Reading Silver Parquet files from %s", SILVER_PATH)
        silver_df = spark.read.parquet(SILVER_PATH)

        silver_count = silver_df.count()
        logging.info("Loaded %s records from Silver.", silver_count)

        if silver_count == 0:
            logging.warning("Silver input is empty. Writing empty Gold dataset.")
            empty_gold_schema = StructType(
                [
                    StructField("model_name", StringType(), True),
                    StructField("total_revenue", DoubleType(), True),
                    StructField("cars_sold", LongType(), True),
                ]
            )
            empty_gold_df = spark.createDataFrame([], empty_gold_schema)
            empty_gold_df.write.mode("overwrite").parquet(GOLD_PATH)
            return empty_gold_df

        valid_silver_df = silver_df.filter(
            "model_name IS NOT NULL AND TRIM(model_name) <> '' "
            "AND price IS NOT NULL AND sale_id IS NOT NULL"
        )
        removed_before_aggregation = silver_count - valid_silver_df.count()
        if removed_before_aggregation > 0:
            logging.warning(
                "Removed %s invalid Silver records before Gold aggregation.",
                removed_before_aggregation,
            )

        gold_df = valid_silver_df.groupBy("model_name").agg(
            spark_sum("price").alias("total_revenue"),
            count("sale_id").alias("cars_sold"),
        )

        gold_count = gold_df.count()
        logging.info("Prepared %s aggregated rows for Gold.", gold_count)

        logging.info("Writing aggregated data to Gold path: %s", GOLD_PATH)
        gold_df.write.mode("overwrite").parquet(GOLD_PATH)
        return gold_df
    except Exception:
        logging.exception("Silver to Gold processing failed.")
        raise


def main() -> None:
    """Run the full Bronze -> Silver -> Gold processing pipeline."""
    configure_logging()
    spark = None

    try:
        connection_string, account_key = get_connection_details()
        spark = create_spark_session(STORAGE_ACCOUNT_NAME, account_key)
        logging.info("Spark session created successfully.")

        if not bronze_has_json_files(connection_string):
            logging.warning(
                "No JSON files found in Bronze container '%s'. Nothing to process. "
                "Run 02_upload_bronze.py first, then rerun this script.",
                BRONZE_CONTAINER,
            )
            return

        process_bronze_to_silver(spark)
        process_silver_to_gold(spark)

        logging.info("Pipeline finished successfully.")
    except Exception:
        logging.exception("Pipeline failed due to an unexpected error.")
        raise
    finally:
        if spark is not None:
            spark.stop()
            logging.info("Spark session stopped.")


if __name__ == "__main__":
    main()

