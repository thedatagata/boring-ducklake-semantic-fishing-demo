# GA Customer Revenue Data Processing Pipeline

## Overview

This script processes the `train_v2.csv` file from the [Google Analytics Customer Revenue Prediction Kaggle competition](https://www.kaggle.com/competitions/ga-customer-revenue-prediction/code). It reads the large CSV file, groups the data by month, and saves each month's data as a separate Parquet file. This makes the data easier to manage and analyze, especially for large datasets.

The script uses the `dlt` (data load tool) library to handle the data extraction and loading process and `polars` for efficient data manipulation.

## Prerequisites

Before running this script, you'll need:

1.  **Python:** Ensure you have Python installed on your system.
2.  **Required Python Libraries:** Install the necessary libraries by running:
    ```bash
    pip install dlt[filesystem] polars rich
    ```
3.  **Input Data:** Download the `train_v2.csv` file from the [Kaggle competition page](https://www.kaggle.com/competitions/ga-customer-revenue-prediction/data).
4.  **GCP Bucket:**
    * You need a Google Cloud Platform (GCP) bucket.
    * Upload the `train_v2.csv` file to a folder within this bucket.
    * Create another empty folder within the same bucket where the output Parquet files will be saved. The script uses `dlt`'s filesystem destination, which defaults to a local `.dlt` directory structure, but it can be configured to point to a GCP bucket. *You may need to adjust the `dlt.pipeline` destination configuration in the script if you specifically want the output directly in a GCS bucket folder.*

## How it Works

1.  **Initialization:** The script sets up logging and initializes a `dlt` pipeline configured to use the filesystem as a destination (by default, it writes locally).
2.  **File Discovery:** It scans for the input CSV file(s) using `dlt`'s filesystem source.
3.  **Schema Reading & Month Identification:** For each CSV file found, it reads the data using `polars`, identifies the unique months present in the `date` column, and determines how many months need processing.
4.  **Monthly Extraction:** The script iterates through the first few months found in the data (specifically the first 3 months as currently coded).
5.  **Data Processing:** For each month, it filters the rows belonging to that month using `polars`.
6.  **Output Generation:** It uses the `dlt` pipeline to write the filtered data for each month into a separate Parquet file. The table (and resulting file) is named following the pattern `ga_sessions_YYYYMM` (e.g., `ga_sessions_201608`).

## Setup and Running the Script

1.  **Download Data:** Get the `train_v2.csv` file from Kaggle.
2.  **Upload to GCP:** Upload `train_v2.csv` to your chosen input folder in your GCP bucket.
3.  **Configure Script (If needed):**
    * **Input Path:** The script currently uses `dlt.sources.filesystem` which might need configuration to point to your specific GCP bucket and input folder if it's not running in an environment already configured for GCS access (like a GCE VM or using Application Default Credentials). You might need to specify the `bucket_url` for `src_fs`.
    * **Output Path:** Similarly, the `dlt.pipeline` destination `dest_fs()` defaults to local output. To write directly to your GCP output folder, you'll need to configure the `filesystem` destination with your `bucket_url`. Refer to the `dlt` documentation for `filesystem` configuration.
    * **Months to Process:** Currently, the script processes only the first 3 months found (`months[0:3]`). Modify this slice if you want to process all months or a different range.
4.  **Run:** Execute the Python script from your terminal:
    ```bash
    python fill_data_swamp_pipeline.py
    ```
5.  **Monitor Output:** The script will print progress messages to the console, indicating which file and months it's processing.

## Output

After the script finishes, you will find:

* Parquet files in the configured output location (either locally in `.dlt/pipelines/fill_data_swamp/analytics/tables/` or in your specified GCP bucket folder if configured).
* Each file will be named `ga_sessions_YYYYMM.parquet`, containing the data for that specific year and month.

This structured output makes it much easier to load and query data for specific time periods using tools that support the Parquet format.