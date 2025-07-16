import pandas as pd
from sqlalchemy import create_engine, text
import os

# --- Setup database connection ---
user = os.getenv("POSTGRES_USER", "user")
password = os.getenv("POSTGRES_PASSWORD", "pass")
host = os.getenv("POSTGRES_HOST", "localhost")
port = os.getenv("POSTGRES_PORT", "5432")
db = os.getenv("POSTGRES_DB", "warehouse")

engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

# --- Extract ---
query = "SELECT * FROM production_logs"
df = pd.read_sql(query, engine)

print("✅ Extracted data:")
print(df.head())

# --- Transform ---
# Replace negative tons_extracted with 0
df.loc[df["tons_extracted"] < 0, "tons_extracted"] = 0

agg_df = df.groupby(["date", "mine_id"]).agg(
    total_production_daily=("tons_extracted", "sum"),
    average_quality_grade=("quality_grade", "mean")
).reset_index()

print("✅ Transformed data (aggregated):")
print(agg_df)

# --- Load ---
with engine.connect() as conn:
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

agg_df.to_sql("daily_production_metrics", engine, if_exists="replace", index=False)

print("✅ Load completed: Data inserted into daily_production_metrics.")
