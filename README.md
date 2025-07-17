# 🚀 Data Pipeline – Coal Mining Analytics

A fully containerized data pipeline for coal production analytics using **Python**, **PostgreSQL**, **Docker**, and **Metabase**.  
This project automates data collection, transformation, and visualization — with anomaly detection and weather integration via API.

---

## 📌 Features

- 🔄 ETL pipeline with clean Python code
- 📊 Interactive dashboard with **Metabase**
- 🛢️ Daily metrics stored in **PostgreSQL**
- 🌧️ Weather data via **Open-Meteo API**
- ⚠️ Anomaly detection with log file + database
- 📦 Dockerized and version-controlled with Git

---

## 🛠️ Pipeline Stages

### 1. **Extract**
- Loads:
  - SQL file: `production_logs.sql` - A table production_logs with columns date, mine_id, shift, tons_extracted, quality_grade.
  - CSV file: `equipment_sensors.csv` - with columns timestamp, equipment_id, status, fuel_consumption, maintenance_alert.
  - Rainfall data via Open-Meteo API - https://archive-api.open-meteo.com/v1/archive to retrieve weather based on the data with column temperature_2m_mean (change the given API https://api.open-meteo.com/v1/forecast because the old date is missing)

### 2. **Transform**
- Replaces negative `tons_extracted` with `0`
- Fills missing sensor rows using **previous day’s values**
- Aggregates:
  - Daily coal production
  - Average coal quality
  - Equipment utilization
  - Fuel efficiency

### 3. **Load**
- Stores metrics in `daily_production_metrics`
- Logs invalid values to:
  - `logs/validation_errors.log`
  - `production_anomalies` table (for review)

---

## 📊 Dashboard Visuals

Built using **Metabase** with:

- 📈 **Line chart**: Daily production trend (last 30 days)
- 📊 **Bar chart**: Average coal quality per mine
- ⚪ **Scatter plot**: Rainfall vs. Production
- 📅 **Dashboard-wide date filter** for interactivity

---

## ⚙️ Tech Stack

| Tool            | Role                        |
|------------------|-----------------------------|
| Python           | ETL logic & data processing |
| PostgreSQL       | Central data warehouse      |
| Docker           | Containerization            |
| Metabase         | Data visualization          |
| Open-Meteo API   | Rainfall data enrichment    |
| Git + GitHub     | Version control             |

---

## 🧪 How to Run

```bash
# Start services
docker-compose up -d

# Run ETL pipeline
docker-compose run etl python main.py

# Open Metabase dashboard
http://localhost:3000
```

<img width="1920" height="1129" alt="dashboad" src="https://github.com/user-attachments/assets/f1efe584-015d-4cd5-9578-f1f59240fa89" />

![Production Forecast](https://github.com/user-attachments/assets/bc8110e6-ac31-4a5d-b0e1-6247ba294720)
