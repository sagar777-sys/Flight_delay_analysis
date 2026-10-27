"""Generates flights.csv (SAMPLE data, not real flight records; airline names are fictional).
Run:  python generate_sample_data.py [number_of_rows]
Default is 20,000 flights. Use a bigger number (e.g. 500000) on a laptop to demo 'big data'.
"""
import csv
import math
import random
import sys
from datetime import datetime, timedelta

random.seed(21)
N = int(sys.argv[1]) if len(sys.argv) > 1 else 20000

# airport: (lat, lon, traffic weight, congestion factor)
AIRPORTS = {
    "DEL": (28.56, 77.10, 5, 1.3), "BOM": (19.09, 72.87, 5, 1.4), "BLR": (13.20, 77.71, 4, 1.0),
    "HYD": (17.24, 78.43, 3, 0.9), "MAA": (13.00, 80.17, 3, 0.9), "CCU": (22.65, 88.45, 3, 1.0),
    "AMD": (23.08, 72.63, 2, 0.8), "PNQ": (18.58, 73.92, 1.5, 0.8), "GOI": (15.38, 73.83, 1.5, 0.9),
    "COK": (10.15, 76.40, 1.5, 0.8),
}
AIRLINES = {"SkyJet": 0.7, "AirNova": 1.0, "BlueWing": 1.3, "Cloudline": 0.9, "Aerostar": 1.6}  # delay factor
AIRLINE_W = [3, 3, 2.5, 2, 1.5]
CODES = list(AIRPORTS)
CODE_W = [AIRPORTS[c][2] for c in CODES]
START, DAYS = datetime(2026, 1, 1), 299          # 1 Jan - 26 Oct 2026
MONSOON = {6, 7, 8, 9}


def km(a, b):
    la1, lo1, la2, lo2 = map(math.radians, (AIRPORTS[a][0], AIRPORTS[a][1], AIRPORTS[b][0], AIRPORTS[b][1]))
    h = math.sin((la2 - la1) / 2) ** 2 + math.cos(la1) * math.cos(la2) * math.sin((lo2 - lo1) / 2) ** 2
    return 6371 * 2 * math.asin(math.sqrt(h))


rows = []
for _ in range(N):
    day = START + timedelta(days=random.randrange(DAYS))
    hour = random.choices(range(5, 24), weights=[4, 6, 7, 7, 6, 5, 5, 5, 5, 5, 6, 7, 7, 6, 5, 4, 3, 2, 1])[0]
    airline = random.choices(list(AIRLINES), weights=AIRLINE_W)[0]
    origin = random.choices(CODES, weights=CODE_W)[0]
    dest = origin
    while dest == origin:
        dest = random.choices(CODES, weights=CODE_W)[0]
    dist = km(origin, dest)
    block = dist / 750 * 60 + 30 + random.uniform(-5, 10)
    sched_dep = day.replace(hour=hour, minute=random.choice(range(0, 60, 5)), second=0)
    sched_arr = sched_dep + timedelta(minutes=round(block))

    a_f, ap_f = AIRLINES[airline], AIRPORTS[origin][3]
    hour_f = 0.7 + (hour - 5) / 18 * 0.9                  # delays build up through the day
    monsoon = day.month in MONSOON
    fog = day.month == 1 and origin == "DEL" and hour <= 10
    p_delay = min(0.8, 0.14 * a_f * ap_f * hour_f * (1.4 if monsoon else 1.0) + (0.25 if fog else 0))
    p_cancel = 0.01 * a_f * (2 if monsoon else 1)

    dep_delay = arr_delay = None
    reason = "None"
    cancelled = 0
    r = random.random()
    if r < p_cancel:
        cancelled = 1
        reason = "Weather" if random.random() < (0.75 if monsoon else 0.5) else "Carrier"
    elif r < p_cancel + p_delay:
        reason = random.choices(
            ["Weather", "Air Traffic", "Carrier", "Late Aircraft", "Security"],
            weights=[3 + (6 if monsoon else 0) + (10 if fog else 0), 2 * ap_f, 3 * a_f,
                     1 + 6 * (hour - 5) / 18, 0.3])[0]
        d = 20 + random.expovariate(1 / 40)
        if reason == "Weather":
            d *= 1.4
        dep_delay = int(round(min(d, 600)))
        arr_delay = max(16, int(round(dep_delay + random.uniform(-5, 5))))
    else:
        dep_delay = int(round(random.uniform(-8, 12)))
        arr_delay = int(round(dep_delay + random.uniform(-10, 3)))

    fmt = "%Y-%m-%d %H:%M"
    act_dep = "" if cancelled else (sched_dep + timedelta(minutes=dep_delay)).strftime(fmt)
    act_arr = "" if cancelled else (sched_arr + timedelta(minutes=arr_delay)).strftime(fmt)
    rows.append([sched_dep, airline, origin, dest, sched_dep.strftime(fmt), act_dep,
                 sched_arr.strftime(fmt), act_arr, cancelled, reason, round(dist)])

rows.sort(key=lambda r: r[0])
with open("flights.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["flight_id", "airline", "origin", "destination", "scheduled_departure", "actual_departure",
                "scheduled_arrival", "actual_arrival", "cancelled", "delay_reason", "distance_km"])
    for i, r in enumerate(rows, 1):
        w.writerow([f"FL{i:07d}"] + r[1:])
    # deliberately bad rows so the cleaning step has something to do
    w.writerow(["FLX00001", "SkyJet", "DEL", "DEL", "2026-10-05 10:00", "2026-10-05 10:05", "2026-10-05 12:00", "2026-10-05 12:03", 0, "None", 500])   # same origin & destination
    w.writerow(["FLX00002", "", "BOM", "BLR", "2026-10-05 11:00", "2026-10-05 11:10", "2026-10-05 12:40", "2026-10-05 12:50", 0, "None", 840])         # missing airline
    w.writerow(["FLX00003", "AirNova", "BLR", "HYD", "not-a-time", "2026-10-06 08:10", "2026-10-06 09:20", "2026-10-06 09:25", 0, "None", 500])        # invalid time
    w.writerow(["FLX00004", "BlueWing", "MAA", "CCU", "2026-10-06 09:00", "2026-10-06 09:05", "2026-10-06 11:10", "2026-10-06 11:15", "maybe", "None", 1370])  # invalid cancelled flag
    w.writerow(["FLX00005", "Cloudline", "AMD", "PNQ", "2026-10-07 14:00", "2026-10-07 14:05", "2026-10-07 15:30", "", 0, "None", 440])             # flown but no arrival time
    w.writerow(["FLX00006", "Aerostar", "GOI", "COK", "2026-10-07 16:00", "2026-10-07 16:10", "2026-10-07 17:20", "2026-10-07 17:30", 0, "None", -50])     # negative distance
    w.writerow(["FLX00007", "SkyJet", "DEL", "BOM", "2026-10-08 06:00", "2026-10-08 06:10", "2026-10-08 08:10", "2026-10-13 08:20", 0, "None", 1140])    # absurd 5-day delay
    w.writerow(["FLX00008", "AirNova", "BOM", "", "2026-10-08 07:00", "2026-10-08 07:05", "2026-10-08 09:00", "2026-10-08 09:05", 0, "None", 900])        # missing destination
print(f"flights.csv written with {len(rows) + 8} rows")
