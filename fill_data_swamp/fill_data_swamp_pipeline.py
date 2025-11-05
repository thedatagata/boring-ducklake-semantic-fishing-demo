import dlt
from dlt.sources.filesystem import filesystem as src_fs
from dlt.destinations import filesystem as dest_fs 
import os
import polars as pl
import logging
from rich.console import Console
from rich.logging import RichHandler
import warnings
from typing import Dict, Iterator, List

console = Console()
import polars as pl


for logger in ['botocore', 'boto3', 'urllib3', 's3transfer', 'fsspec', 'aiobotocore']:
    logging.getLogger(logger).setLevel(logging.WARNING)
warnings.filterwarnings('ignore', message='.*checksum.*')
warnings.filterwarnings('ignore', message='.*delimiter.*')

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler(console=console, rich_tracebacks=True)]
)

def process_data(df: pl.DataFrame) -> Iterator[List[Dict]]:
    days = df.select("date").unique().get_column("date").sort().to_list()
    for day in days:
        yield df.filter(pl.col("date") == day).to_dicts()


@dlt.resource(
    write_disposition="replace",
    primary_key=['visitId', 'fullVisitorId']
)
def extract(month: str, ga_scan: pl.LazyFrame):
    """Extract data for a specific month."""
    console.log(f"[yellow]Processing month: {month}")
    
    df = ga_scan.filter(pl.col("date").str.slice(0, 6) == month).collect(streaming=True)
    console.log(f"[green]Found {len(df):,} rows for month {month}")
    yield from process_data(df)

if __name__ == '__main__':
    console.log("[bold cyan]Starting GA data pipeline...")
    
    pipeline = dlt.pipeline(
        pipeline_name="fill_data_swamp",
        destination=dest_fs(),
        dataset_name="analytics"
    )
    
    for file_object in src_fs():
        file_path = file_object['file_url']
        console.log(f"[bold yellow]Processing file: {file_path}")
        
        ga_scan = pl.scan_csv(
            file_path,
            infer_schema=False,
            ignore_errors=True,
            low_memory=True,
            encoding='utf8-lossy'
        )
        
        months = (ga_scan.select(pl.col("date").str.slice(0, 6).unique().alias("month"))
                        .collect()
                        .get_column("month")
                        .sort()
                        .to_list()[-6:])  # Get only the last 6 months
        
        console.log(f"[blue]Found {len(months)} months to process")
        
        for month in months:
            info = pipeline.run(
                extract(month, ga_scan),
                table_name=f"ga_sessions_{month}",
                loader_file_format="parquet"
            )
            console.log(f"[purple]Loaded table ga_sessions_{month}")
        
        console.log(f"[yellow]File processing complete")
    
    console.log(f"[bold green]Pipeline complete")