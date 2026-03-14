import io
import os
import logging
from typing import List

import pandas as pd
import pyarrow.parquet as pq
import streamlit as st
from azure.core.exceptions import ClientAuthenticationError, HttpResponseError, ResourceNotFoundError
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv


GOLD_CONTAINER = "3-gold"
SILVER_CONTAINER = "2-silver"


def configure_page() -> None:
    """Configure Streamlit page metadata and light styling."""
    st.set_page_config(page_title="Porsche Sales Dashboard", layout="wide")
    st.title("Porsche Sales Dashboard")
    st.caption("Medallion pipeline insights from Silver and Gold containers")

    st.markdown(
        """
        <style>
        .stMetric {
            /* Use a darker translucent card so text stays readable in dark theme. */
            background-color: rgba(17, 24, 39, 0.75);
            border: 1px solid rgba(148, 163, 184, 0.35);
            border-radius: 10px;
            padding: 10px;
        }
        .stMetric label,
        .stMetric [data-testid="stMetricValue"],
        .stMetric [data-testid="stMetricDelta"] {
            color: #f3f4f6 !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def configure_logging() -> None:
    """Configure logs for dashboard data access diagnostics."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    logging.getLogger("azure").setLevel(logging.WARNING)
    logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)


def get_connection_string() -> str:
    """Load the Azure connection string from environment variables."""
    load_dotenv()
    connection_string = os.getenv("AZURE_CONNECTION_STRING", "")
    if not connection_string:
        raise EnvironmentError("AZURE_CONNECTION_STRING is missing. Add it in .env or environment variables.")
    return connection_string


def _load_parquet_blob_frames(blob_service_client: BlobServiceClient, container_name: str) -> List[pd.DataFrame]:
    """Download every parquet blob from a container and return one frame per file."""
    container_client = blob_service_client.get_container_client(container_name)
    frames: List[pd.DataFrame] = []

    for blob in container_client.list_blobs():
        if not blob.name.endswith(".parquet"):
            continue

        blob_bytes = container_client.download_blob(blob.name).readall()
        table = pq.read_table(io.BytesIO(blob_bytes))
        frames.append(table.to_pandas())

    return frames


@st.cache_data(ttl=300, show_spinner=False)
def load_parquet_container(connection_string: str, container_name: str) -> pd.DataFrame:
    """Load and merge all parquet files from one Azure Blob container."""
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        frames = _load_parquet_blob_frames(blob_service_client, container_name)
    except ResourceNotFoundError:
        logging.warning("Container '%s' was not found.", container_name)
        return pd.DataFrame()
    except (ClientAuthenticationError, HttpResponseError) as exc:
        logging.error("Failed to access container '%s' due to Azure auth/service error: %s", container_name, exc)
        raise
    except Exception:
        logging.exception("Unexpected error while loading container '%s'.", container_name)
        raise

    if not frames:
        logging.info("No parquet files found in container '%s'.", container_name)
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def build_gold_from_silver(silver_df: pd.DataFrame) -> pd.DataFrame:
    """Create Gold-like aggregates from Silver when Gold is not available yet."""
    if silver_df.empty:
        return pd.DataFrame(columns=["model_name", "total_revenue", "cars_sold"])

    required_columns = {"model_name", "price", "sale_id"}
    if not required_columns.issubset(silver_df.columns):
        return pd.DataFrame(columns=["model_name", "total_revenue", "cars_sold"])

    valid_df = silver_df.dropna(subset=["model_name", "price", "sale_id"])
    valid_df = valid_df[valid_df["model_name"].astype(str).str.strip() != ""]

    if valid_df.empty:
        return pd.DataFrame(columns=["model_name", "total_revenue", "cars_sold"])

    gold_df = (
        valid_df.groupby("model_name", as_index=False)
        .agg(total_revenue=("price", "sum"), cars_sold=("sale_id", "count"))
        .sort_values("total_revenue", ascending=False)
    )
    return gold_df


def render_kpis(gold_df: pd.DataFrame, silver_df: pd.DataFrame) -> None:
    """Render high-level KPI cards."""
    total_revenue = float(gold_df["total_revenue"].sum()) if "total_revenue" in gold_df.columns else 0.0
    cars_sold = int(gold_df["cars_sold"].sum()) if "cars_sold" in gold_df.columns else 0
    models_count = int(gold_df["model_name"].nunique()) if "model_name" in gold_df.columns else 0

    electric_share = 0.0
    if not silver_df.empty and "is_electric" in silver_df.columns:
        electric_series = silver_df["is_electric"].dropna().astype(bool)
        if not electric_series.empty:
            electric_share = float(electric_series.mean() * 100)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total revenue", f"EUR {total_revenue:,.0f}")
    col2.metric("Cars sold", f"{cars_sold:,}")
    col3.metric("Distinct models sold", f"{models_count}")
    col4.metric("Electric share", f"{electric_share:.1f}%")


def render_charts(gold_df: pd.DataFrame, silver_df: pd.DataFrame) -> None:
    """Render model, trend, and segmentation charts."""
    left, right = st.columns(2)

    with left:
        st.subheader("Revenue by model")
        if not gold_df.empty and {"model_name", "total_revenue"}.issubset(gold_df.columns):
            revenue_chart = gold_df.set_index("model_name")[["total_revenue"]].sort_values(
                "total_revenue", ascending=False
            )
            st.bar_chart(revenue_chart)
        else:
            st.info("No Gold revenue data available yet.")

    with right:
        st.subheader("Cars sold by model")
        if not gold_df.empty and {"model_name", "cars_sold"}.issubset(gold_df.columns):
            cars_chart = gold_df.set_index("model_name")[["cars_sold"]].sort_values(
                "cars_sold", ascending=False
            )
            st.bar_chart(cars_chart)
        else:
            st.info("No Gold sales count data available yet.")

    left, right = st.columns(2)

    with left:
        st.subheader("Daily revenue trend")
        if not silver_df.empty and {"sale_timestamp", "price"}.issubset(silver_df.columns):
            trend_df = silver_df.copy()
            trend_df["sale_timestamp"] = pd.to_datetime(trend_df["sale_timestamp"], errors="coerce")
            trend_df = trend_df.dropna(subset=["sale_timestamp", "price"])
            if trend_df.empty:
                st.info("No valid timestamped Silver data available.")
            else:
                trend_df["sale_date"] = trend_df["sale_timestamp"].dt.date
                daily = trend_df.groupby("sale_date", as_index=False)["price"].sum().rename(
                    columns={"price": "daily_revenue"}
                )
                st.line_chart(daily.set_index("sale_date")[["daily_revenue"]])
        else:
            st.info("No Silver trend data available yet.")

    with right:
        st.subheader("Sales by country")
        if not silver_df.empty and {"country", "sale_id"}.issubset(silver_df.columns):
            country_df = (
                silver_df.dropna(subset=["country", "sale_id"])
                .groupby("country", as_index=False)["sale_id"]
                .count()
                .rename(columns={"sale_id": "sales"})
                .sort_values("sales", ascending=False)
            )
            if country_df.empty:
                st.info("No valid country data available.")
            else:
                st.bar_chart(country_df.set_index("country")[["sales"]])
        else:
            st.info("No Silver country data available yet.")


def main() -> None:
    """Run the dashboard app."""
    configure_logging()


    configure_page()

    try:
        connection_string = get_connection_string()
    except Exception as exc:
        st.error(str(exc))
        st.stop()

    st.info("Loading Gold and Silver datasets from Azure Blob Storage...")
    try:
        gold_df = load_parquet_container(connection_string, GOLD_CONTAINER)
        silver_df = load_parquet_container(connection_string, SILVER_CONTAINER)
    except (ClientAuthenticationError, HttpResponseError) as exc:
        st.error(f"Azure access error while loading dashboard data: {exc}")
        st.stop()
    except Exception as exc:
        st.error(f"Unexpected error while loading dashboard data: {exc}")
        st.stop()

    if gold_df.empty and silver_df.empty:
        st.warning(
            "No data found in Silver or Gold. Generate data, upload to Bronze, and run the PySpark pipeline first."
        )
        st.code(
            "python 01_generate_data.py\npython 02_upload_bronze.py\npython 03_process_pyspark.py",
            language="powershell",
        )
        return

    if gold_df.empty and not silver_df.empty:
        st.info("Gold data was not found. Building temporary aggregates from Silver for visualization.")
        gold_df = build_gold_from_silver(silver_df)

    render_kpis(gold_df, silver_df)
    render_charts(gold_df, silver_df)

    with st.expander("Preview data"):
        st.write("Gold preview")
        st.dataframe(gold_df.head(20), width="stretch")
        st.write("Silver preview")
        st.dataframe(silver_df.head(20), width="stretch")


if __name__ == "__main__":
    main()

