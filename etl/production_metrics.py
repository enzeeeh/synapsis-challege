import pandas as pd
from sqlalchemy import create_engine, text
import os

def run_production_metrics(engine):
    query = "SELECT * FROM production_logs"
    df = pd.read_sql(query, engine)
    df.loc[df["tons_extracted"] < 0, "tons_extracted"] = 0

    agg_df = df.groupby(["date", "mine_id"]).agg(
        total_production_daily=("tons_extracted", "sum"),
        average_quality_grade=("quality_grade", "mean")
    ).reset_index()

    with engine.connect() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS daily_production_metrics (
            date DATE,
            mine_id INT,
            total_production_daily FLOAT,
            average_quality_grade FLOAT,
            PRIMARY KEY (date, mine_id)
        );
        """))
        conn.commit()

    agg_df.to_sql("daily_production_metrics", engine, if_exists="replace", index=False)
    print("✅ Production metrics inserted.")
