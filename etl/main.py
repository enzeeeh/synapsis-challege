import pandas as pd
from sqlalchemy import create_engine, text
import os

# DB connection settings
user = os.getenv("POSTGRES_USER", "user")
password = os.getenv("POSTGRES_PASSWORD", "pass")
host = os.getenv("POSTGRES_HOST", "localhost")
port = os.getenv("POSTGRES_PORT", "5432")
db = os.getenv("POSTGRES_DB", "warehouse")

# Create SQLAlchemy engine
engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

# --- Extract ---
query = "SELECT * FROM production_logs"
df = pd.read_sql(query, engine)

# --- Transform ---
# Replace negative tons_extracted with 0
df.loc[df["tons_extracted"] < 0, "tons_extracted"] = 0

# Group by date and mine_id
agg_df = df.groupby(["date", "mine_id"]).agg(
    total_production_daily=("tons_extracted", "sum"),
    average_quality_grade=("quality_grade", "mean")
).reset_index()

# --- Load ---
with engine.connect() as conn:
    # Create metrics table if not exists
    create_sql = """
    CREATE TABLE IF NOT EXISTS daily_production_metrics (
        date DATE,
        mine_id INT,
        total_production_daily FLOAT,
        average_quality_grade FLOAT,
        PRIMARY KEY (date, mine_id)
    );
    """
    conn.execute(text(create_sql))
    conn.commit()

# Insert data (replace existing rows for same date & mine_id)
agg_df.to_sql("daily_production_metrics", engine, if_exists="replace", index=False)

print("✅ ETL completed successfully. Metrics inserted into daily_production_metrics table.")
