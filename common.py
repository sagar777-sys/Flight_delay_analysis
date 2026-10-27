"""Shared helper used by both engines."""


def add_rates(df):
    """Adds delayed %, on-time % and cancelled % columns to a summary table that has
    flights, cancelled, flown and delayed counts."""
    df = df.copy()
    flown = df["flown"].astype(float).replace(0, float("nan"))
    df["delayed_pct"] = (df["delayed"] / flown * 100).round(1).fillna(0.0)
    df["ontime_pct"] = ((df["flown"] - df["delayed"]) / flown * 100).round(1).fillna(0.0)
    df["cancelled_pct"] = (df["cancelled"] / df["flights"].astype(float) * 100).round(1)
    for c in ("avg_arr_delay", "avg_dep_delay"):
        if c in df.columns:
            df[c] = df[c].astype(float).round(1)
    return df
