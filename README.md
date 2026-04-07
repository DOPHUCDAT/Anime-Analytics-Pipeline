# 🎌 Anime Seasonal Data Pipeline (ELT + Medallion Architecture)

## 📌 Project Overview

This project builds an **end-to-end ELT data pipeline** using modern data engineering tools to collect, transform, model, and orchestrate anime data from the **Jikan API (MyAnimeList unofficial API)**.

The pipeline follows the **Medallion Architecture (Bronze → Silver → Gold → Mart)** and is fully automated using **Dagster**.

### Key Objectives
- Extract anime seasonal data from a public API
- Load raw data into a PostgreSQL data warehouse using Docker
- Transform and model data using dbt (Star schema)
- Orchestrate and monitor the pipeline with Dagster
- Prepare analytics-ready datasets for BI tools
- Develop Machine Learning models to predict score 
---

## 🧱 Architecture Overview

- **ELT pattern**: Extract → Load → Transform
- **Medallion Architecture**:
  - **Bronze**: Raw ingested data
  - **Silver**: Cleaned and standardized data
  - **Gold**: Dimensional models (Fact & Dimensions)
  - **Mart**: Business-ready aggregated datasets

---

## 🛠️ Tech Stack

| Layer | Tool |
|-----|------|
| Data Source | Jikan API |
| Extract & Load | dlt |
| Data Warehouse | PostgreSQL (Docker) |
| Transformation | dbt |
| Orchestration | Dagster |
| Data Modeling | Star Schema |
| Machine Learning | Scitkit-Learn |

---

## Installation

Use the package manager pip to install uv

```bash
pip install uv
```

Install all needed library for this project

```bash
uv add -r requirements.txt
```

---

## Command

To run the project first run docker container

```bash
docker compose up -d
```

Run Dagster Webserver and access port 3000 

```bash
cd orchestration
dagster dev -f definitions.py
```

After get into Dagster Web UI run Materialize All to activate pipeline

Visualization with Metabase on port [3005](http://localhost:3005)

---

## Conclusion

This project delivers an end-to-end data pipeline and machine learning solution to predict anime scores using metadata and user engagement signals. Multiple regression models were evaluated, including Linear Regression, Decision Tree, Random Forest, and Gradient Boosting. The final result found that Random Forest have highest score include:

RMSE: 0.4951
MAE: 0.3652
R²: 0.6686

From a modeling standpoint, an R² of 0.6686 suggests that the model can explain approximately 66.9% of the variance in anime scores. The RMSE and MAE values indicate that the average prediction error remains approximately 0.45 score points, which is acceptable for user-facing applications such as ranking, recommendation, or trend analysis.

From a business perspective, the results confirm that both engagement metrics (members, favorites, popularity) and content attributes (genres, type, rating, seasonality) play a significant role in determining perceived quality.