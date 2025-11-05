import dlt
from dlt.sources.filesystem import filesystem 
import polars as pl
import duckdb
import logging
from rich.console import Console
from rich.logging import RichHandler
import warnings
import json 
import ast
from typing import Dict, Iterator, Optional
from dlt.helpers.dbt import create_runner
import os
from pathlib import Path
import tempfile

console = Console()

# Configure logging and silence warnings
for logger in ['botocore', 'boto3', 'urllib3', 's3transfer', 'fsspec', 'aiobotocore']:
    logging.getLogger(logger).setLevel(logging.WARNING)
warnings.filterwarnings('ignore', message='.*checksum.*')
warnings.filterwarnings('ignore', message='.*delimiter.*')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        RichHandler(console=console, rich_tracebacks=True),
        logging.FileHandler('pipeline.log')
    ]
)

logger = logging.getLogger(__name__)

# Get absolute paths for important locations
SCRIPT_DIR = Path(__file__).parent.absolute()
PROJECT_ROOT = SCRIPT_DIR.parent
DBT_PROJECT_PATH = SCRIPT_DIR / "data_swamp_models"

# Create local DuckDB pipeline
pipeline = dlt.pipeline(
    pipeline_name="filter_data_swamp",
    destination="duckdb",
    dataset_name="source_data",
    progress="log"
)

def execute_pipeline(file_object):
    """Execute the data pipeline for a given file object."""
    logger.info("Using local DuckDB for data loading and transformation")

    # Rest of your pipeline code stays the same
    @dlt.resource(max_table_nesting=3, write_disposition="append")
    def extract():
        """Extract stage: Reads data in chunks to manage memory."""
        try:
            logger.info(f"Starting data extraction from: {file_object['file_url']}")
            
            # Scan first to get total row count
            total_rows = pl.scan_parquet(file_object['file_url']).select(pl.count()).collect().item()
            logger.info(f"Total rows to process: {total_rows}")
            
            # Process in chunks of 100k rows
            for chunk_start in range(0, total_rows, 100_000):
                df = pl.scan_parquet(
                    file_object['file_url'],
                    parallel="row_groups",
                    low_memory=True,
                    use_statistics=True,
                    cache=False
                ).slice(chunk_start, 100_000).collect()
                
                logger.info(f"Processing chunk of {df.height} rows")
                yield df
                
        except Exception as e:
            logger.error(f"Extract error: {e}")
            raise

    @dlt.transformer(data_from=extract, parallelized=True)
    def transform(df: pl.DataFrame) -> Iterator[Dict]:
        def process_hits(hits):
            try:
                return ast.literal_eval(hits)
            except Exception as e:
                logger.error(f"Error processing hits: {e}")
                return None

        dates = df.select('date').unique().to_series().sort()
        logger.info(f"Processing {len(dates)} unique dates")
        
        # Process one date at a time instead of all at once
        for date in dates:
            date_df = df.filter(pl.col('date') == date)
            hits_chunk = date_df.select(['visit_id', 'full_visitor_id', 'hits']).to_pandas()
            hits_chunk['hits'] = hits_chunk['hits'].apply(process_hits)
            hits_chunk = hits_chunk.dropna(subset=['hits'])
                
            sessions_chunk = date_df.select([
                'visit_id', 'full_visitor_id', 'visit_number', 'visit_start_time', 'date',
                'device', 'geo_network', 'totals', 'traffic_source'
            ])
            chunk_df = sessions_chunk.join(pl.DataFrame(hits_chunk), on=['visit_id','full_visitor_id'], how='inner')
            yield chunk_df

    @dlt.transformer(data_from=transform)
    def load(df: pl.DataFrame) -> Iterator[Dict]:
        try:
            yield df.to_dicts()
        except Exception as e:
            logger.error(f"Load error: {e}")
            raise

    pipeline_info = pipeline.run(load)
    
    logger.info("Running dbt models...")

    return pipeline_info

def setup_ducklake_database():
    """Ensure DuckLake database exists in MotherDuck."""
    logger.info("Checking/creating DuckLake database in MotherDuck...")
    
    try:
        # Get MotherDuck credentials from dlt config
        import dlt.common.configuration.specs as specs
        from dlt.common.configuration import resolve_configuration
        
        # Get MotherDuck token from secrets
        creds = resolve_configuration(
            specs.ConnectionStringCredentials(),
            sections=("destination", "motherduck", "credentials")
        )
        
        token = creds.password
        
        # Connect to MotherDuck
        con = duckdb.connect(f"md:?motherduck_token={token}")
        
        # Check if ducklake_analytics database exists
        databases = con.execute("SHOW DATABASES").df()
        
        if 'ducklake_analytics' not in databases['name'].values:
            logger.info("Creating DuckLake database: ducklake_analytics")
            con.execute("""
                CREATE DATABASE ducklake_analytics (
                    TYPE DUCKLAKE,
                    DATA_PATH 'gs://boring-dlt-duckdb-demo/ducklake/'
                )
            """)
            logger.info("✅ DuckLake database created successfully!")
        else:
            logger.info("✅ DuckLake database already exists")
        
        con.close()
        
    except Exception as e:
        logger.warning(f"Could not setup DuckLake database: {e}")
        logger.warning("Pipeline will attempt to create it automatically during export")

def export_to_ducklake():
    """Export src_sessions_fct from local DuckDB to DuckLake (MotherDuck)."""
    logger.info("="*80)
    logger.info("Starting DuckLake export...")
    logger.info("="*80)
    
    # Path to local DuckDB database created by dlt
    local_db_path = SCRIPT_DIR / "filter_data_swamp.duckdb"
    
    if not local_db_path.exists():
        logger.error(f"Local DuckDB database not found at {local_db_path}")
        return
    
    # Create a dlt resource from the local DuckDB table
    @dlt.resource(name="src_sessions_fct", write_disposition="replace")
    def load_sessions():
        """Load src_sessions_fct from local DuckDB."""
        conn = duckdb.connect(str(local_db_path), read_only=True)
        
        # Check if table exists
        try:
            row_count = conn.execute("SELECT COUNT(*) FROM source_data.src_sessions_fct").fetchone()[0]
            logger.info(f"Found {row_count:,} rows in src_sessions_fct")
        except Exception as e:
            logger.error(f"Table source_data.src_sessions_fct not found: {e}")
            conn.close()
            return
        
        # Read data in chunks
        chunk_size = 10000
        offset = 0
        
        while offset < row_count:
            logger.info(f"Reading rows {offset:,} to {min(offset + chunk_size, row_count):,}")
            df = conn.execute(f"""
                SELECT * FROM source_data.src_sessions_fct 
                ORDER BY session_start_time
                LIMIT {chunk_size} OFFSET {offset}
            """).df()
            
            yield df.to_dict('records')
            offset += chunk_size
        
        conn.close()
    
    # Create pipeline to MotherDuck/DuckLake
    logger.info("Creating MotherDuck/DuckLake pipeline...")
    ducklake_pipeline = dlt.pipeline(
        pipeline_name="export_to_ducklake",
        destination="motherduck",
        dataset_name="ducklake_analytics",  # This is the database name
        progress="log"
    )
    
    # Run the export
    logger.info("Exporting src_sessions_fct to DuckLake...")
    info = ducklake_pipeline.run(load_sessions())
    
    logger.info("✅ DuckLake export completed!")
    logger.info(f"Table: md:ducklake_analytics.main.src_sessions_fct")
    logger.info(f"Data stored in: gs://boring-dlt-duckdb-demo/ducklake/")
    
    return info

if __name__ == '__main__':
    logger.info("Starting data pipeline...")
    logger.info(f"Destination: Local DuckDB")
    logger.info(f"DBT project path: {DBT_PROJECT_PATH}")
    
    for file_object in filesystem():
        try:
            logger.info(f"Processing file: {file_object['file_url']}")
            info = execute_pipeline(file_object)
            logger.info(f"File processed: {info}")
        except Exception as e:
            logger.error(f"Failed to process file {file_object['file_url']}: {e}")
            continue
    
    # Run dbt transformations
    logger.info("="*80)
    logger.info("Running dbt transformations...")
    logger.info("="*80)
    
    dbt = dlt.dbt.package(
        pipeline, 
        str(DBT_PROJECT_PATH)
    )
    models = dbt.run_all() 
    for m in models:
        logger.info(
            f"Model {m.model_name} materialized" +
            f" in {m.time}" +
            f" with status {m.status}" +
            f" and message {m.message}"
        )
    
    # Setup DuckLake database in MotherDuck
    try:
        setup_ducklake_database()
    except Exception as e:
        logger.warning(f"DuckLake setup warning: {e}")
    
    # Export transformed data to DuckLake
    try:
        export_to_ducklake()
    except Exception as e:
        logger.error(f"Failed to export to DuckLake: {e}")
        logger.error("The data is still available in local DuckDB at filter_data_swamp.duckdb")
        raise
    
    logger.info("="*80)
    logger.info("✅ Complete pipeline finished successfully!")
    logger.info("="*80)


