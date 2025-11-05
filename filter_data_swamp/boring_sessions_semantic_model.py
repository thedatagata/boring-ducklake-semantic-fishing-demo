"""Semantic model for sessions fact table from DuckLake."""

import ibis
from boring_semantic_layer import SemanticModel, DimensionSpec, MeasureSpec
from pathlib import Path

# Connect to local DuckDB
SCRIPT_DIR = Path(__file__).parent.absolute()
LOCAL_DB_PATH = SCRIPT_DIR / "filter_data_swamp.duckdb"

con = ibis.duckdb.connect(str(LOCAL_DB_PATH))
sessions_tbl = con.table("src_sessions_fct", database="source_data")

# Define semantic model with descriptions for MCP
sessions_sm = SemanticModel(
    name="sessions",
    table=sessions_tbl,
    description="Google Analytics session data with user behavior, device info, and traffic sources",
    
    # Time dimension for time-series queries
    time_dimension="session_start_time",
    smallest_time_grain="TIME_GRAIN_DAY",
    
    dimensions={
        # User & Session
        "user_id": DimensionSpec(
            expr=lambda t: t.user_id,
            description="Unique identifier for the visitor"
        ),
        "session_id": DimensionSpec(
            expr=lambda t: t.session_id,
            description="Unique session identifier"
        ),
        "session_number": DimensionSpec(
            expr=lambda t: t.session_number,
            description="Sequential number of this session for the user"
        ),
        
        # Device
        "device_browser": DimensionSpec(
            expr=lambda t: t.session_device__browser,
            description="Browser used in the session"
        ),
        "device_os": DimensionSpec(
            expr=lambda t: t.session_device__os,
            description="Operating system of the device"
        ),
        "device_category": DimensionSpec(
            expr=lambda t: t.session_device__device_category,
            description="Type of device (desktop, mobile, tablet)"
        ),
        "is_mobile": DimensionSpec(
            expr=lambda t: t.session_device__is_mobile,
            description="Whether the session was on a mobile device"
        ),
        
        # Geography
        "continent": DimensionSpec(
            expr=lambda t: t.session_geo__continent,
            description="Continent where the session originated"
        ),
        "country": DimensionSpec(
            expr=lambda t: t.session_geo__country,
            description="Country where the session originated"
        ),
        
        # Traffic Source
        "traffic_source": DimensionSpec(
            expr=lambda t: t.session_traffic_source__source,
            description="Traffic source (google, direct, etc.)"
        ),
        "traffic_medium": DimensionSpec(
            expr=lambda t: t.session_traffic_source__medium,
            description="Traffic medium (organic, cpc, referral, etc.)"
        ),
        "campaign": DimensionSpec(
            expr=lambda t: t.session_traffic_source__campaign,
            description="Marketing campaign name"
        ),
    },
    
    measures={
        "session_count": MeasureSpec(
            expr=lambda t: t.count(),
            description="Total number of sessions"
        ),
        "user_count": MeasureSpec(
            expr=lambda t: t.user_id.nunique(),
            description="Number of unique users"
        ),
        "total_pageviews": MeasureSpec(
            expr=lambda t: t.session_totals__pageviews.sum(),
            description="Total pageviews across all sessions"
        ),
        "avg_pageviews": MeasureSpec(
            expr=lambda t: t.session_totals__pageviews.mean(),
            description="Average pageviews per session"
        ),
        "total_time_on_site": MeasureSpec(
            expr=lambda t: t.session_totals__time_on_site.sum(),
            description="Total time spent on site in seconds"
        ),
        "avg_time_on_site": MeasureSpec(
            expr=lambda t: t.session_totals__time_on_site.mean(),
            description="Average time on site per session in seconds"
        ),
        "total_revenue": MeasureSpec(
            expr=lambda t: t.session_totals__transaction_revenue.sum(),
            description="Total transaction revenue in micros (divide by 1,000,000 for dollars)"
        ),
        "new_users": MeasureSpec(
            expr=lambda t: t.session_totals__new_visits.sum(),
            description="Number of new user sessions"
        ),
    }
)