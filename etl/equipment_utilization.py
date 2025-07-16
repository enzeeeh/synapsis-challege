import pandas as pd
from sqlalchemy import text

def run_equipment_utilization(engine):
    sensor_df = pd.read_csv("/app/../dataset/equipment_sensors.csv", parse_dates=["timestamp"])
    sensor_df["date"] = sensor_df["timestamp"].dt.date

    util_df = sensor_df.groupby(["date", "equipment_id"]).agg(
        active_hours=("status", lambda x: (x == "active").sum()),
        total_hours=("status", "count")
    ).reset_index()

    util_df["equipment_utilization"] = (util_df["active_hours"] / util_df["total_hours"]) * 100
    util_df = util_df[["date", "equipment_id", "equipment_utilization"]]

    with engine.connect() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS equipment_utilization (
            date DATE,
            equipment_id INT,
            equipment_utilization FLOAT,
            PRIMARY KEY (date, equipment_id)
        );
        """))
        conn.commit()

    util_df.to_sql("equipment_utilization", engine, if_exists="replace", index=False)
    print("✅ Equipment utilization metrics inserted.")
