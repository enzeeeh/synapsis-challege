import pandas as pd
from sqlalchemy import create_engine, text
import requests
import os

# --- DB connection setup ---
user = os.getenv("POSTGRES_USER", "user")
password = os.getenv("POSTGRES_PASSWORD", "pass")
host = os.getenv("POSTGRES_HOST", "localhost")
port = os.getenv("POSTGRES_PORT", "5432")
db = os.getenv("POSTGRES_DB", "warehouse")
engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

# --- Extract: Production logs ---
prod_df = pd.read_sql("SELECT * FROM production_logs", engine)

# --- Flag anomalies ---
prod_df["anomaly_flag"] = prod_df["tons_extracted"] < 0

# --- Save anomalies before fixing ---
anomaly_df = prod_df[prod_df["anomaly_flag"] == True].copy()

if not anomaly_df.empty:
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS production_anomalies (
                log_id INT,
                date DATE,
                mine_id INT,
                shift VARCHAR(10),
                original_tons_extracted FLOAT,
                quality_grade FLOAT,
                anomaly_flag BOOLEAN
            );
        """))
        conn.commit()

    anomaly_df = anomaly_df.rename(columns={"tons_extracted": "original_tons_extracted"})
    anomaly_df[["log_id", "date", "mine_id", "shift", "original_tons_extracted", "quality_grade", "anomaly_flag"]] \
        .to_sql("production_anomalies", engine, if_exists="replace", index=False)

    print("⚠️ Anomalies saved to table: production_anomalies")


# --- Replace negative tons with 0 ---
prod_df.loc[prod_df["tons_extracted"] < 0, "tons_extracted"] = 0

# --- Aggregate production metrics ---
prod_agg = prod_df.groupby(["date"]).agg(
    total_production_daily=("tons_extracted", "sum"),
    average_quality_grade=("quality_grade", "mean")
).reset_index()

# --- Extract: Equipment sensors ---
sensor_df = pd.read_csv("/dataset/equipment_sensors.csv", parse_dates=["timestamp"])
sensor_df["date"] = sensor_df["timestamp"].dt.date

# --- Aggregate equipment utilization ---
equip_agg = sensor_df.groupby(["date"]).agg(
    active_hours=("status", lambda x: (x == "active").sum()),
    total_hours=("status", "count"),
    total_fuel_consumption=("fuel_consumption", "sum")
).reset_index()

equip_agg["equipment_utilization"] = (equip_agg["active_hours"] / equip_agg["total_hours"]) * 100
equip_agg["equipment_active_hours"] = equip_agg["active_hours"]
equip_agg["equipment_total_hours"] = equip_agg["total_hours"]
equip_agg = equip_agg[["date", "equipment_utilization", "equipment_active_hours", "equipment_total_hours", "total_fuel_consumption"]]

# --- Merge production metrics ---
merged_df = pd.merge(
    prod_agg,
    equip_agg,
    on="date",
    how="left"
)

# --- Calculate fuel efficiency ---
merged_df["fuel_efficiency"] = merged_df["total_fuel_consumption"] / merged_df["total_production_daily"]

# --- Fetch rainfall data from API ---
def get_rainfall(date_str):
    url = (
        "https://api.open-meteo.com/v1/forecast"
        "?latitude=2.0167&longitude=117.3000"
        f"&daily=temperature_2m_mean,precipitation_sum&timezone=Asia/Jakarta"
        f"&start_date={date_str}&end_date={date_str}"
    )
    response = requests.get(url)
    data = response.json()

    if "daily" in data and "precipitation_sum" in data["daily"]:
        return data["daily"]["precipitation_sum"][0]
    else:
        return 0.0

rainfall_data = []
for d in merged_df["date"]:
    rainfall_mm = get_rainfall(d.strftime("%Y-%m-%d"))
    rainfall_data.append({"date": d, "rainfall_mm": rainfall_mm})

rainfall_df = pd.DataFrame(rainfall_data)

# --- Merge rainfall data ---
merged_df = pd.merge(
    merged_df,
    rainfall_df,
    on="date",
    how="left"
)

# --- Create final table in DB ---
with engine.connect() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS daily_production_metrics (
            date DATE PRIMARY KEY,
            total_production_daily FLOAT,
            average_quality_grade FLOAT,
            equipment_utilization FLOAT,
            equipment_active_hours FLOAT,
            equipment_total_hours FLOAT,
            total_fuel_consumption FLOAT,
            fuel_efficiency FLOAT,
            rainfall_mm FLOAT
        );
    """))
    conn.commit()

# --- Load final merged data ---
merged_df.to_sql("daily_production_metrics", engine, if_exists="replace", index=False)
print("✅ All metrics loaded into daily_production_metrics table successfully.")
