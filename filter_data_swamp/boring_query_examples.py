#!/usr/bin/env python
"""Example queries and visualizations using the sessions semantic model."""

from sessions_semantic_model import sessions_sm

# Example 1: Sessions by device category
print("=" * 80)
print("Sessions by Device Category")
print("=" * 80)
device_query = sessions_sm.query(
    dimensions=["device_category"],
    measures=["session_count", "avg_pageviews", "avg_time_on_site"],
    order_by=[("session_count", "desc")]
)
print(device_query.execute())

# Create visualization (requires boring-semantic-layer[viz-altair])
try:
    chart = device_query.chart(spec={
        "title": "Sessions by Device Category",
        "mark": {"type": "bar", "color": "steelblue"}
    })
    chart.save("sessions_by_device.png")
    print("\n✅ Chart saved to sessions_by_device.png")
except Exception as e:
    print(f"\n⚠️  Visualization requires: pip install 'boring-semantic-layer[viz-altair]'")

# Example 2: Traffic source analysis
print("\n" + "=" * 80)
print("Top Traffic Sources")
print("=" * 80)
traffic_query = sessions_sm.query(
    dimensions=["traffic_source", "traffic_medium"],
    measures=["session_count", "user_count", "total_pageviews"],
    order_by=[("session_count", "desc")],
    limit=10
)
print(traffic_query.execute())

# Example 3: Time series - sessions over time
print("\n" + "=" * 80)
print("Sessions Over Time (First Month)")
print("=" * 80)
time_query = sessions_sm.query(
    dimensions=["session_start_time"],
    measures=["session_count", "new_users"],
    time_grain="TIME_GRAIN_DAY",
    order_by=[("session_start_time", "asc")],
    limit=30
)
print(time_query.execute())

try:
    time_chart = time_query.chart(spec={
        "title": "Daily Sessions Trend",
        "mark": "line"
    })
    time_chart.save("sessions_time_series.png")
    print("\n✅ Time series chart saved to sessions_time_series.png")
except Exception as e:
    print(f"\n⚠️  Visualization requires: pip install 'boring-semantic-layer[viz-altair]'")

# Example 4: Geographic analysis
print("\n" + "=" * 80)
print("Sessions by Country (Top 10)")
print("=" * 80)
geo_query = sessions_sm.query(
    dimensions=["country"],
    measures=["session_count", "total_revenue"],
    filters=[
        {"field": "country", "operator": "is not null", "value": None}
    ],
    order_by=[("session_count", "desc")],
    limit=10
)
print(geo_query.execute())

# Example 5: Revenue analysis by device and source
print("\n" + "=" * 80)
print("Revenue by Device Category and Traffic Source")
print("=" * 80)
revenue_query = sessions_sm.query(
    dimensions=["device_category", "traffic_source"],
    measures=["session_count", "total_revenue"],
    filters=[
        {"field": "total_revenue", "operator": ">", "value": 0}
    ],
    order_by=[("total_revenue", "desc")],
    limit=15
)
print(revenue_query.execute())

print("\n" + "=" * 80)
print("✅ Examples complete! See the output above.")
print("=" * 80)