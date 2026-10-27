"""Airline Flight Delay Analytics - Apache Spark (PySpark) + Streamlit dashboard."""
import hashlib
import os
import shutil
import tempfile

import pandas as pd
import plotly.express as px
import streamlit as st

# ---- pick the engine: Spark if it can start (needs Java), else pandas fallback ----
ENGINE_NOTE = ""
try:
    import spark_engine as engine
    engine.get_spark()
except Exception as e:  # Java missing, low memory, etc.
    import pandas_engine as engine
    ENGINE_NOTE = (f"Spark could not start here ({type(e).__name__}); running the pandas fallback. "
                   f"Java on server: {shutil.which('java') or 'NOT FOUND'}. Details: {str(e)[:400]}")

st.set_page_config(page_title="Flight Delay Analytics", page_icon="✈️", layout="wide")
HERE = os.path.dirname(os.path.abspath(__file__))
SAMPLE = os.path.join(HERE, "flights.csv")


@st.cache_resource(show_spinner="Processing flights with Spark...")
def load(path, key):
    return engine.load_and_clean(path)


# ---------------- sidebar ----------------
st.sidebar.title("✈️ Controls")
up = st.sidebar.file_uploader("Upload flights CSV", type="csv")
if up is not None:
    content = up.getvalue()
    key = hashlib.md5(content).hexdigest()
    path = os.path.join(tempfile.gettempdir(), f"upload_{key}.csv")
    with open(path, "wb") as fh:
        fh.write(content)
    source = f"Uploaded: {up.name}"
else:
    path, key, source = SAMPLE, "sample", "Sample data (generated, fictional airlines, not real flight records)"

try:
    df, stats = load(path, key)
except Exception as e:
    st.error(f"Could not read this file: {e}")
    st.stop()
if stats["rows_out"] == 0:
    st.error("No valid flights left after cleaning. Check your CSV columns and values.")
    st.stop()

airlines, origins, dmin, dmax = engine.get_options(df)
date_range = st.sidebar.date_input("Date range", value=(dmin, dmax), min_value=dmin, max_value=dmax)
sel_airlines = st.sidebar.multiselect("Airline", airlines, default=airlines)
sel_origins = st.sidebar.multiselect("Departure airport", origins, default=origins)
threshold = st.sidebar.slider("Flight is delayed if it arrives more than (minutes) late", 5, 60, 15, step=5)
min_route = st.sidebar.slider("Minimum flights for a route to be ranked", 5, 100, 20, step=5)

if isinstance(date_range, (tuple, list)) and len(date_range) == 2:
    start, end = date_range
else:
    start = end = date_range[0] if isinstance(date_range, (tuple, list)) else date_range
if not sel_airlines or not sel_origins:
    st.warning("Select at least one airline and one departure airport.")
    st.stop()

res = engine.analyze(df, start, end, sel_airlines, sel_origins, threshold, min_route)
k = res["kpi"]

# ---------------- header ----------------
st.title("✈️ Airline Flight Delay Analytics")
st.caption(f"Processing engine: **{engine.NAME}** · Source: {source}")
if ENGINE_NOTE:
    st.warning(ENGINE_NOTE)
st.caption(f"Cleaning: {stats['rows_in']:,} rows read, {stats['rows_dropped']:,} invalid flights removed, "
           f"{stats['rows_out']:,} kept.")
if k["flights"] == 0:
    st.info("No flights match the current filters.")
    st.stop()

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total flights", f"{k['flights']:,}")
c2.metric("On-time", f"{k['ontime_pct']:.1f}%")
c3.metric("Delayed", f"{k['delayed_pct']:.1f}%", f"{k['delayed']:,} flights", delta_color="off")
c4.metric("Cancelled", f"{k['cancelled_pct']:.1f}%", f"{k['cancelled']:,} flights", delta_color="off")
c5.metric("Avg arrival delay", f"{k['avg_arr_delay']:.1f} min", f"avg departure delay {k['avg_dep_delay']:.1f} min", delta_color="off")

# ---------------- key insights (written from the results) ----------------
ins = []
al = res["by_airline"]
if len(al) > 1:
    ins.append(f"Best on-time record: **{al.iloc[0]['airline']}** ({al.iloc[0]['ontime_pct']}%). "
               f"Worst: **{al.iloc[-1]['airline']}** ({al.iloc[-1]['ontime_pct']}%).")
og = res["by_origin"]
if len(og) > 1:
    ins.append(f"Airport with the highest average departure delay: **{og.iloc[0]['origin']}** ({og.iloc[0]['avg_dep_delay']} min).")
bm = res["by_month"]
if len(bm) > 1:
    w = bm.sort_values("delayed_pct", ascending=False).iloc[0]
    ins.append(f"Month with the highest share of delayed flights: **{w['month']}** ({w['delayed_pct']}%).")
rs = res["by_reason"]
if not rs.empty:
    ins.append(f"Most common delay cause: **{rs.iloc[0]['delay_reason']}** ({int(rs.iloc[0]['flights']):,} delayed flights).")
if ins:
    st.info("**Key insights**\n\n" + "\n\n".join("• " + i for i in ins))

t1, t2, t3, t4, t5 = st.tabs(["Overview", "Airlines", "Airports & routes", "Delay causes", "Worst delays"])

# ---------------- overview ----------------
with t1:
    st.plotly_chart(px.line(res["by_month"], x="month", y="delayed_pct", markers=True,
                            title="Delayed flights by month (%)", labels={"month": "Month", "delayed_pct": "% delayed"}))
    a, b = st.columns(2)
    a.plotly_chart(px.line(res["by_hour"], x="hour", y="avg_dep_delay", markers=True,
                           title="Average departure delay by scheduled hour (min)",
                           labels={"hour": "Scheduled departure hour", "avg_dep_delay": "Avg delay (min)"}))
    d = res["dist"].copy()
    b.plotly_chart(px.bar(d, x="bin", y="flights", title="Arrival delay distribution (15-min buckets, 180 = 180+)",
                          labels={"bin": "Arrival delay (min)", "flights": "Flights"}))

# ---------------- airlines ----------------
with t2:
    st.plotly_chart(px.bar(res["by_airline"], x="airline", y="ontime_pct", title="On-time performance by airline (%)",
                           labels={"airline": "Airline", "ontime_pct": "On-time %"}))
    pivot = res["airline_month"].pivot_table(index="airline", columns="month", values="avg_arr_delay")
    st.plotly_chart(px.imshow(pivot, aspect="auto", title="Average arrival delay (min): airline x month",
                              labels={"x": "Month", "y": "Airline", "color": "Avg delay (min)"}))
    st.dataframe(res["by_airline"][["airline", "flights", "ontime_pct", "delayed_pct", "cancelled_pct",
                                    "avg_arr_delay", "avg_dep_delay"]], hide_index=True)

# ---------------- airports & routes ----------------
with t3:
    a, b = st.columns(2)
    a.plotly_chart(px.bar(res["by_origin"], x="origin", y="avg_dep_delay", title="Average departure delay by airport (min)",
                          labels={"origin": "Airport", "avg_dep_delay": "Avg delay (min)"}))
    worst_routes = res["routes"].head(10)
    if worst_routes.empty:
        b.info("No routes have enough flights. Lower the minimum flights per route in the sidebar.")
    else:
        b.plotly_chart(px.bar(worst_routes, x="avg_arr_delay", y="route", orientation="h",
                              title=f"10 most delayed routes (min {min_route} flights)",
                              labels={"route": "Route", "avg_arr_delay": "Avg arrival delay (min)"}
                              ).update_yaxes(autorange="reversed"))
    st.subheader("Airport summary")
    st.dataframe(res["by_origin"][["origin", "flights", "avg_dep_delay", "delayed_pct", "cancelled_pct"]], hide_index=True)
    st.subheader("Route summary")
    st.dataframe(res["routes"][["route", "flights", "avg_arr_delay", "delayed_pct", "cancelled_pct"]], hide_index=True)

# ---------------- delay causes ----------------
with t4:
    if rs.empty:
        st.info("No delayed flights in this selection.")
    else:
        a, b = st.columns(2)
        a.plotly_chart(px.pie(rs, names="delay_reason", values="flights", hole=0.45, title="Delayed flights by cause"))
        rs2 = rs.assign(total_delay_hours=(rs["total_delay_min"] / 60).round(0))
        b.plotly_chart(px.bar(rs2, x="delay_reason", y="total_delay_hours", title="Total delay time by cause (hours)",
                              labels={"delay_reason": "Cause", "total_delay_hours": "Hours"}))
        st.dataframe(rs2, hide_index=True)

# ---------------- worst delays ----------------
with t5:
    st.subheader("100 longest delays")
    wd = res["worst"].copy()
    if wd.empty:
        st.success("No delayed flights in this selection.")
    else:
        wd["route"] = wd["origin"] + " → " + wd["destination"]
        wd["arr_delay"] = wd["arr_delay"].round(0).astype(int)
        st.dataframe(wd[["flight_id", "date", "airline", "route", "arr_delay", "delay_reason"]], hide_index=True)

# ---------------- export ----------------
st.divider()
st.subheader("Download reports")
d1, d2, d3 = st.columns(3)
d1.download_button("⬇️ Airline summary (CSV)", res["by_airline"].to_csv(index=False).encode(), "airline_summary.csv", "text/csv")
d2.download_button("⬇️ Airport summary (CSV)", res["by_origin"].to_csv(index=False).encode(), "airport_summary.csv", "text/csv")
d3.download_button("⬇️ Route summary (CSV)", res["routes"].to_csv(index=False).encode(), "route_summary.csv", "text/csv")
