#!/usr/bin/env python
"""Sync local DuckLake data to MotherDuck."""

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
LAKE_CATALOG = SCRIPT_DIR / "lake_catalog.sqlite"

@dlt.resource(name="src_sessions_fct", write_disposition="append")
def read_from_ducklake():
    """Read from local DuckLake."""
    conn = duckdb.connect(":memory:")
    conn.execute(f"ATTACH '{LAKE_CATALOG}' AS lake_cat (TYPE DUCKLAKE)")
    
    row_count = conn.execute("SELECT COUNT(*) FROM lake_cat.main.src_sessions_fct").fetchone()[0]
    logger.info(f"Found {row_count:,} rows in local DuckLake")
    
    chunk_size = 10000
    offset = 0
    
    while offset < row_count:
        logger.info(f"Reading rows {offset:,} to {min(offset + chunk_size, row_count):,}")
        df = conn.execute(f"""
            SELECT * FROM lake_cat.main.src_sessions_fct 
            LIMIT {chunk_size} OFFSET {offset}
        """).df()
        
        yield df.to_dict('records')
        offset += chunk_size
    
    conn.close()

if __name__ == "__main__":
    logger.info("Syncing to MotherDuck...")
    
    pipeline = dlt.pipeline(
        pipeline_name="sync_to_motherduck",
        destination="motherduck",
        dataset_name="main",
        progress="log"
    )
    
    info = pipeline.run(read_from_ducklake())
    logger.info("✅ Synced to MotherDuck")