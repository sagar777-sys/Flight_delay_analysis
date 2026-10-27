"""Pandas fallback engine - used ONLY if Spark/Java cannot start on the host."""
import numpy as np
import pandas as pd

from common import add_rates

NAME = "Pandas (fallback - Spark unavailable)"
REQUIRED = ["flight_id", "airline", "origin", "destination", "scheduled_departure", "actual_departure",
            "scheduled_arrival", "actual_arrival", "cancelled", "delay_reason", "distance_km"]
FLAG = {"1": 1, "true": 1, "yes": 1, "0": 0, "false": 0, "no": 0}


def load_and_clean(path):
    raw = pd.read_csv(path, dtype=str)
    raw.columns = [c.strip().lower() for c in raw.columns]
    missing = [c for c in REQUIRED if c not in raw.columns]
    if missing:
        raise ValueError("CSV is missing columns: " + ", ".join(missing))
    rows_in = len(raw)
    s = lambda c: raw[c].fillna("").str.strip()
    df = pd.DataFrame({
        "flight_id": s("flight_id"),
        "airline": s("airline"),
        "origin": s("origin").str.upper(),
        "destination": s("destination").str.upper(),
        "sched_dep": pd.to_datetime(s("scheduled_departure"), errors="coerce"),
        "act_dep": pd.to_datetime(s("actual_departure"), errors="coerce"),
        "sched_arr": pd.to_datetime(s("scheduled_arrival"), errors="coerce"),
        "act_arr": pd.to_datetime(s("actual_arrival"), errors="coerce"),
        "cancelled": s("cancelled").str.lower().map(FLAG),
        "delay_reason": s("delay_reason").replace("", "None").str.title(),
        "distance_km": pd.to_numeric(raw["distance_km"], errors="coerce"),
    })
    df["dep_delay"] = (df["act_dep"] - df["sched_dep"]).dt.total_seconds() / 60.0
    df["arr_delay"] = (df["act_arr"] - df["sched_arr"]).dt.total_seconds() / 60.0
    base = ((df["flight_id"] != "") & (df["airline"] != "") & (df["origin"] != "") & (df["destination"] != "")
            & (df["origin"] != df["destination"]) & df["sched_dep"].notna() & df["sched_arr"].notna()
            & (df["sched_arr"] > df["sched_dep"]) & df["cancelled"].notna() & (df["distance_km"] > 0))
    flown_ok = (df["dep_delay"].between(-90, 1440) & df["arr_delay"].between(-90, 1440))
    df = df[base & ((df["cancelled"] == 1) | flown_ok)].copy()
    df["cancelled"] = df["cancelled"].astype(int)
    df.loc[df["cancelled"] == 1, ["dep_delay", "arr_delay"]] = np.nan
    df["date"] = df["sched_dep"].dt.date
    df["month"] = df["sched_dep"].dt.strftime("%Y-%m")
    df["hour"] = df["sched_dep"].dt.hour
    return df.reset_index(drop=True), {"rows_in": rows_in, "rows_out": len(df), "rows_dropped": rows_in - len(df)}


def get_options(df):
    return sorted(df["airline"].unique()), sorted(df["origin"].unique()), df["date"].min(), df["date"].max()


def _summ(g):
    return g.agg(flights=("flight_id", "count"), cancelled=("cancelled", "sum"), flown=("flown", "sum"),
                 delayed=("delayed", "sum"), avg_arr_delay=("arr_delay", "mean"),
                 avg_dep_delay=("dep_delay", "mean")).reset_index()


def analyze(df, start, end, airlines, origins, threshold, min_route):
    f = df[(df["date"] >= start) & (df["date"] <= end) & df["airline"].isin(airlines)
           & df["origin"].isin(origins)].copy()
    f["flown"] = (f["cancelled"] == 0).astype(int)
    f["delayed"] = ((f["cancelled"] == 0) & (f["arr_delay"] > threshold)).astype(int)

    total, canc = len(f), int(f["cancelled"].sum())
    flown, delayed = int(f["flown"].sum()), int(f["delayed"].sum())
    kpi = {"flights": total, "cancelled": canc, "flown": flown, "delayed": delayed,
           "cancelled_pct": canc / total * 100 if total else 0.0,
           "delayed_pct": delayed / flown * 100 if flown else 0.0,
           "ontime_pct": (flown - delayed) / flown * 100 if flown else 0.0,
           "avg_arr_delay": float(f["arr_delay"].mean()) if flown else 0.0,
           "avg_dep_delay": float(f["dep_delay"].mean()) if flown else 0.0}

    by_airline = add_rates(_summ(f.groupby("airline"))).sort_values("ontime_pct", ascending=False).reset_index(drop=True)
    by_origin = add_rates(_summ(f.groupby("origin"))).sort_values("avg_dep_delay", ascending=False).reset_index(drop=True)
    routes = _summ(f.groupby(["origin", "destination"]))
    routes = add_rates(routes[routes["flights"] >= min_route])
    routes["route"] = routes["origin"] + " → " + routes["destination"]
    routes = routes.sort_values("avg_arr_delay", ascending=False).reset_index(drop=True)
    by_month = add_rates(_summ(f.groupby("month"))).sort_values("month").reset_index(drop=True)
    by_hour = add_rates(_summ(f.groupby("hour"))).sort_values("hour").reset_index(drop=True)
    airline_month = add_rates(_summ(f.groupby(["airline", "month"])))
    d = f[f["delayed"] == 1]
    by_reason = (d.groupby("delay_reason").agg(flights=("flight_id", "count"), total_delay_min=("arr_delay", "sum"))
                 .reset_index().sort_values("flights", ascending=False).reset_index(drop=True))
    fl = f[f["cancelled"] == 0]
    dist = (np.floor(fl["arr_delay"].clip(-30, 180) / 15) * 15).astype(int).value_counts().sort_index()
    dist = dist.rename_axis("bin").reset_index(name="flights")
    worst = d.sort_values("arr_delay", ascending=False).head(100)[
        ["flight_id", "date", "airline", "origin", "destination", "arr_delay", "delay_reason"]].reset_index(drop=True)
    return {"kpi": kpi, "by_airline": by_airline, "by_origin": by_origin, "routes": routes, "by_month": by_month,
            "by_hour": by_hour, "airline_month": airline_month, "by_reason": by_reason, "dist": dist, "worst": worst}
