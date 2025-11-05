# Data Swamp Filter Pipeline

## Overview

This project defines a data pipeline that processes Google Analytics-like data stored in Parquet files. It performs the following steps:

1.  **Extracts** data from three specified Parquet files (expected to be hosted in a Google Cloud Storage bucket, although the script currently uses local filesystem access patterns).
2.  **Loads** the raw data into a local DuckDB database file (`data_swamp.duckdb`).
3.  **Transforms** the raw data using dbt (Data Build Tool) models to create structured fact tables (`src_sessions_fct`, `src_events_fct`).

The pipeline uses the `dlt` library for data loading and orchestration and `polars` for efficient data manipulation within the Python script. The transformations are defined in SQL within a dbt project.

## Prerequisites

Before you begin, ensure you have the following installed:

* **Python:** Version 3.8 or higher.
* **pip:** Python package installer.
* **Git:** (Optional) If cloning from a repository.
* **(Potentially) Google Cloud SDK (`gcloud`):** Required if your Parquet files are in GCS and you need to authenticate.

## Setup

1.  **Get the Code:**
    * Clone the repository or download the code files into a local directory.

2.  **Create a Virtual Environment (Recommended):**
    ```bash
    python -m venv .venv
    source .venv/bin/activate  # On Windows use `.venv\Scripts\activate`
    ```

3.  **Install Python Dependencies:**
    * Install the required Python libraries listed in the script:
        ```bash
        pip install dlt[duckdb] polars rich dbt-core dbt-duckdb
        ```
        * `dlt[duckdb]`: Installs the Data Load Tool library with DuckDB support.
        * `polars`: For data manipulation.
        * `rich`: For enhanced logging output.
        * `dbt-core` & `dbt-duckdb`: Required for running dbt transformations against DuckDB. `dlt` might install these, but explicit installation ensures they are present.

4.  **Configure File Access (IMPORTANT):**
    * The Python script `filter_data_swamp_pipeline.py` uses `dlt.sources.filesystem()` to discover input files.
    * **If your Parquet files are local:** Place the three Parquet files in a location where the script can find them (e.g., the project root or a subdirectory). The script seems designed to iterate through files found by `filesystem()`. You might need to adjust the script if it doesn't automatically pick up your specific files.
    * **If your Parquet files are in GCS:**
        * You need to ensure the environment running the script has access to your GCS bucket. The easiest way is often to authenticate using the Google Cloud SDK:
            ```bash
            gcloud auth application-default login
            ```
        * Modify the `filter_data_swamp_pipeline.py` script to point directly to your GCS file paths. Instead of relying on `filesystem()` to discover files, you might need to explicitly define the file paths (e.g., `gs://your-bucket-name/path/to/file1.parquet`). You could pass these paths as arguments or environment variables to the script. The `polars` library (`pl.scan_parquet`) used in the script supports reading directly from GCS URIs if authentication is correctly configured.

5.  **Install dbt Dependencies:**
    * Navigate to the dbt project directory (`data_swamp_models`) and install the dbt package dependencies defined in `dependencies.yml`:
        ```bash
        cd data_swamp_models
        dbt deps
        cd ..
        ```

## Running the Pipeline

1.  **Execute the Python Script:**
    * From the root directory of the project (where `filter_data_swamp_pipeline.py` is located), run:
        ```bash
        python filter_data_swamp_pipeline.py
        ```

2.  **What Happens:**
    * The script will start, logging its progress to the console (using `rich`) and to a file named `pipeline.log`.
    * It will attempt to find and process your Parquet files one by one.
    * For each file:
        * Data is extracted in chunks using `polars`.
        * Basic transformation (processing 'hits' column) occurs.
        * Data is loaded into the `source_data` schema in the `data_swamp_models/data_swamp.duckdb` file. If the file doesn't exist, it will be created.
    * After processing each file, the script runs the dbt models defined in the `data_swamp_models/models/sources` directory. These models transform the raw data (`load`, `load__hits`) into `src_events_fct` and `src_sessions_fct` tables within the same DuckDB file, using the `gordon_bombay` profile defined in `profiles.yml`.
    * You will see output indicating the status of the dbt model runs.

3.  **Outputs:**
    * **`data_swamp_models/data_swamp.duckdb`:** The DuckDB database file containing the raw (`source_data.load`, `source_data.load__hits`) and transformed (`source_data.src_events_fct`, `source_data.src_sessions_fct`) data. You can connect to this file using a DuckDB client or Python library to query the data.
    * **`pipeline.log`:** A log file containing detailed information about the pipeline execution, including any errors.

## Project Structure