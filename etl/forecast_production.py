import pandas as pd
from sqlalchemy import create_engine
import os

# --- DB connection ---
user = os.getenv("POSTGRES_USER", "user")
password = os.getenv("POSTGRES_PASSWORD", "pass")
host = os.getenv("POSTGRES_HOST", "localhost")
port = os.getenv("POSTGRES_PORT", "5432")
db = os.getenv("POSTGRES_DB", "warehouse")

engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

# --- Load data ---
query = "SELECT date, total_production_daily, rainfall_mm, equipment_utilization, fuel_efficiency FROM daily_production_metrics ORDER BY date"
df = pd.read_sql(query, engine)

print("✅ Loaded data:")
print(df.head())

# --- Step 2: Create lag features ---
for lag in range(1, 4):  # Lag for 1, 2, 3 days
    df[f"lag_{lag}"] = df["total_production_daily"].shift(lag)

# Drop rows with NA from lags
df = df.dropna().reset_index(drop=True)

print("✅ Created lag features:")
print(df[["date", "total_production_daily", "lag_1", "lag_2", "lag_3"]].head())

# --- Step 3: Train/Test Split (last 30 days for test) ---
n_test = 30

train_df = df[:-n_test]
test_df = df[-n_test:]

feature_cols = ["lag_1", "lag_2", "lag_3", "rainfall_mm", "equipment_utilization", "fuel_efficiency"]
target_col = "total_production_daily"

X_train = train_df[feature_cols]
y_train = train_df[target_col]

X_test = test_df[feature_cols]
y_test = test_df[target_col]

print("✅ Train/Test split:")
print(f"Train samples: {len(X_train)} | Test samples: {len(X_test)}")

# --- Step 4: Train XGBoost and predict ---
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
import numpy as np

model = XGBRegressor(n_estimators=100, learning_rate=0.1, random_state=42)
model.fit(X_train, y_train)

# Predict
y_pred = model.predict(X_test)

# Evaluation
mae = mean_absolute_error(y_test, y_pred)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))

print(f"✅ MAE: {mae:.2f}")
print(f"✅ RMSE: {rmse:.2f}")

# --- Step 6: Save predictions to Postgres table ---

# Create DataFrame for saving
results_df = pd.DataFrame({
    "date": test_df["date"],
    "actual_production": y_test.values,
    "predicted_production": y_pred
})

# Save to DB
results_df.to_sql("production_forecast", engine, if_exists="replace", index=False)

print("✅ Forecast results saved to table: production_forecast")


