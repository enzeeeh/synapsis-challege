import os
from sqlalchemy import create_engine
import sys

# --- DB setup ---
user = os.getenv("POSTGRES_USER", "user")
password = os.getenv("POSTGRES_PASSWORD", "pass")
host = os.getenv("POSTGRES_HOST", "localhost")
port = os.getenv("POSTGRES_PORT", "5432")
db = os.getenv("POSTGRES_DB", "warehouse")
engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

# --- Import modules ---
import production_metrics
import equipment_utilization

if len(sys.argv) < 2:
    print("Please specify which ETL to run: 'production', 'utilization', or 'all'")
else:
    task = sys.argv[1]
    if task == "production":
        production_metrics.run_production_metrics(engine)
    elif task == "utilization":
        equipment_utilization.run_equipment_utilization(engine)
    elif task == "all":
        production_metrics.run_production_metrics(engine)
        equipment_utilization.run_equipment_utilization(engine)
        print("✅ All ETL tasks completed.")
    else:
        print("Unknown option. Use 'production', 'utilization', or 'all'.")
