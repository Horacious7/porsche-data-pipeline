# Porsche Sales Medallion Pipeline

End-to-end Porsche sales analytics pipeline built with Python, PySpark, Azure Blob Storage, and Streamlit.
The project follows a Medallion architecture:

- `Bronze`: raw JSON sales events (`1-bronze`)
- `Silver`: cleaned and deduplicated parquet (`2-silver`)
- `Gold`: aggregated business metrics (`3-gold`)

## Project Components

- `01_generate_data.py` - Generates mock Porsche sales JSON files locally.
- `02_upload_bronze.py` - Uploads generated JSON files into Azure Bronze.
- `03_process_pyspark.py` - Runs Bronze -> Silver -> Gold transformations with logging and data-quality checks.
- `04_dashboard_streamlit.py` - Displays KPI cards and charts from Silver/Gold parquet data.

## Data Pipeline Logic

### Bronze -> Silver

- Downloads JSON files from `1-bronze` to a local working folder (`pipeline_work/bronze_json`) and reads them with Spark (multiline-safe parsing).
- Removes invalid records (`price <= 0`).
- Drops rows with null/blank `vin_number`, `sale_id`, and `model_name`.
- Deduplicates by `vin_number`.
- Adds `processed_timestamp`.
- Writes parquet locally (`pipeline_work/silver_parquet`) and uploads to `2-silver/sales/` with overwrite semantics.

### Silver -> Gold

- Reads Silver parquet from the local staging folder.
- Aggregates by `model_name`:
  - `total_revenue = SUM(price)`
  - `cars_sold = COUNT(sale_id)`
- Writes Gold parquet locally (`pipeline_work/gold_parquet`) and uploads to `3-gold/model_metrics/`.

## Setup

### 1) Prerequisites

- Python 3.10+
- Java 8/11/17 (required by Spark)
- Azure Storage account with containers:
  - `1-bronze`
  - `2-silver`
  - `3-gold`

### 2) Install dependencies

```powershell
pip install -r requirements.txt
```

### 3) Configure credentials

Copy `.env.example` to `.env` and set a valid connection string:

```powershell
Copy-Item .env.example .env
```

`AZURE_CONNECTION_STRING` must point to account `datalakeporscheho` (or update script constants accordingly).

## Run

### Generate and upload data

```powershell
python 01_generate_data.py
python 02_upload_bronze.py
```

### Process with Spark

```powershell
python 03_process_pyspark.py
```

### Launch dashboard

```powershell
streamlit run 04_dashboard_streamlit.py
```

## Operational Behavior and Error Handling

- Consistent timestamped logging is used across scripts.
- `03_process_pyspark.py` checks Bronze availability before processing.
- If Bronze has no JSON files, pipeline exits gracefully with actionable logs.
- Azure auth/resource errors are classified and logged explicitly for faster troubleshooting.
- Local staging avoids unstable direct Spark commit/rename behavior on some Windows + ADLS Gen2 setups.
- Dashboard gracefully handles missing containers/data and shows guidance in-app.

## 🛠️ AI-Driven Engineering & Collaboration

This project was developed using modern AI pair-programming practices to accelerate delivery and improve engineering quality.

- A conversational AI assistant (Gemini) was used for high-level system design, Medallion architecture planning, and data engineering learning support.
- An advanced coding agent (GPT-5.3-Codex) was used for implementation-heavy work, including PySpark transformations, debugging, and production-style error-handling refinements.

The result is a practical example of using AI as a productivity multiplier: faster iteration, clearer architecture decisions, and stronger implementation consistency.

## Security Notice: AI & Secret Management

When using AI coding assistants (Copilot, Cursor, GPT-based tools), protect cloud credentials with strict handling rules:

- Never paste connection strings, API keys, or other secrets into AI chat prompts.
- Close `.env` tabs after updating secrets; some assistants can use open editor context.
- If a key or connection string is exposed, rotate it immediately in Azure Portal (Regenerate Access Keys).

## ⚠️ Crucial Prerequisites for Windows Users

Running PySpark locally on Windows typically requires Hadoop native binaries.
Without them, Spark jobs may fail at runtime with filesystem/native dependency errors.

### Required setup

1. Download `winutils.exe` and `hadoop.dll` from a trusted Hadoop 3.3+ compatible repository.
2. Place both files in:
   - `C:\hadoop\bin\`
3. Set system environment variable:
   - `HADOOP_HOME=C:\hadoop`
4. Add this entry to your system `Path`:
   - `%HADOOP_HOME%\bin`
5. Restart IDE and terminal sessions to reload environment variables.

## Quick Troubleshooting

- `AZURE_CONNECTION_STRING is missing`: create/update `.env` and restart terminal.
- `Bronze container was not found`: verify container name and storage account.
- `authentication/authorization error`: verify connection string validity and permissions.
- Dashboard shows no data: run `01_generate_data.py`, `02_upload_bronze.py`, then `03_process_pyspark.py`.

