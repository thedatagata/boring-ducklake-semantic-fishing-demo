#!/usr/bin/env python
"""Load src_sessions_fct to local DuckLake with SQLite catalog."""

import dlt
import duckdb
import logging
from rich.console import Console
from rich.logging import RichHandler
from pathlib import Path

console = Console()
logging.basicConfig(level=logging.INFO, handlers=[RichHandler(console=console)])
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.absolute()
LOCAL_DB_PATH = SCRIPT_DIR / "filter_data_swamp.duckdb"

@dlt.resource(name="src_sessions_fct", write_disposition="append")
def load_sessions():
    """Load src_sessions_fct from local DuckDB."""
    conn = duckdb.connect(str(LOCAL_DB_PATH), read_only=True)
    
    try:
        row_count = conn.execute("SELECT COUNT(*) FROM source_data.src_sessions_fct").fetchone()[0]
        logger.info(f"Found {row_count:,} rows")
        
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
    finally:
        conn.close()

if __name__ == "__main__":
    logger.info("Loading to local DuckLake...")
    
    # Configure ducklake explicitly
    pipeline = dlt.pipeline(
        pipeline_name="local_ducklake",
        destination="ducklake",
        dataset_name="analytics",
        progress="log",
        dev_mode=False
    )
    
    try:
        info = pipeline.run(load_sessions())
        logger.info("✅ Load complete")
        
        # Verify the data
        with pipeline.sql_client() as client:
            result = client.execute_sql("SELECT COUNT(*) as cnt FROM analytics.src_sessions_fct")
            count = list(result)[0][0]
            logger.info(f"Verified {count} rows in DuckLake")
            
    except Exception as e:
        logger.error(f"Load failed: {e}")
        raise