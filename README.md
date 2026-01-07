# 📈 Stock Analytics Data Engineering Platform

## End-to-End Incremental ETL, Analytics & Visualization Pipeline  
**Technologies:** Apache Airflow, Python, Databricks (Spark), Power BI

---

## 1. Project Overview

This project implements an enterprise-grade stock analytics platform that ingests raw financial data, processes it through a **Bronze–Silver–Gold architecture**, and delivers interactive analytical dashboards in Power BI.

The system is designed to handle:
- Incremental data arrivals
- Data quality issues
- Scalable analytics
- Real-time business insights

The pipeline is fully automated using Apache Airflow and executes analytical workloads in Databricks, with results stored as Delta tables and visualized in Power BI.

---

## 2. Business Problem Statement

Financial organizations deal with continuously changing stock market data. Manual analysis leads to:
- Delayed insights
- Data inconsistency
- Poor risk assessment
- Lack of investor behavior understanding

### This project solves:
- Automated ingestion of raw stock data
- Incremental ETL processing
- Portfolio performance analysis
- Risk and volatility identification
- Investor trading behavior classification

---

## 3. High-Level Architecture
```
Raw CSV Files
↓
Apache Airflow (Orchestration)
↓
Bronze Layer (Raw Storage)
↓
Silver Layer (Cleaned & Transformed Data)
↓
Gold Layer (Business Analytics – Delta Tables)
↓
Power BI Dashboards
```

---

## 4. Technology Stack

| Layer | Technology |
|------|-----------|
| Orchestration | Apache Airflow |
| Processing | Python (Pandas) |
| Big Data Analytics | Databricks (Apache Spark) |
| Visualization | Power BI |

---

## 5. Directory Structure and Initial Datasets Used
```
STOCK-ANALYTICS-PLATFORM
│
├── Airflow/
│   └── dags/
│       └── stock_analytics_etl_dag.py
│
├── Airflow_Outputs/
│
├── Data/
│   ├── raw/
│   │   ├── investor_master.csv
│   │   ├── portfolio_transactions.csv
│   │   └── stock_prices.csv
│   │
│   ├── Bronze/
│   │   ├── bronze_layer.py
│   │   ├── investor_master.csv
│   │   ├── portfolio_transactions.csv
│   │   └── stock_prices.csv
│   │
│   └── processed/
│       ├── investor_master_clean.csv
│       ├── portfolio_transactions_clean.csv
│       └── stock_prices_clean.csv
│
├── Documentation/
│
├── logs/
│   ├── dag_id=stock_analytics_incremental_etl_pipeline_v2/
│   ├── dag_processor_manager/
│   ├── scheduler/
│   ├── bronze.log
│   └── silver_layer.log
│
├── notebooks/
│   └── gold_layer_analytics.ipynb
│
├── PowerBI/
│
└── README.md
```

Three raw CSV datasets representing realistic stock market data.


### 5.1.`stock_prices.csv`

This dataset contains **daily stock market price data** and is primarily used for **time-series analysis, trend identification, volatility calculation, and incremental data processing**.

| Column Name | Explanation |
|------------|------------|
| `trade_date` | Trading date; used for time-series analysis and incremental ETL detection |
| `symbol` | Unique stock identifier (ticker symbol) |
| `company_name` | Name of the listed company |
| `sector` | Business sector of the company (Technology, Finance, Energy, etc.) |
| `open_price` | Stock price at market opening |
| `high_price` | Highest price reached during the trading day |
| `low_price` | Lowest price reached during the trading day |
| `close_price` | Stock price at market closing |
| `volume` | Total number of shares traded during the day |
| `market_cap` | Total market valuation of the company |
| `exchange` | Stock exchange where the stock is listed (simulated as NSE) |


---

### 5.2.`portfolio_transactions.csv`

This dataset records **investor trading activity** and forms the foundation for **portfolio performance evaluation and trading behavior analysis**.

| Column Name | Explanation |
|------------|------------|
| `transaction_id` | Unique identifier for each trade |
| `trade_date` | Date on which the transaction occurred |
| `investor_id` | Identifier of the investor executing the trade |
| `symbol` | Stock that was traded |
| `action` | Type of trade – BUY or SELL |
| `quantity` | Number of shares traded |
| `trade_price` | Price at which the trade was executed |
 

---

### 5.3 `investor_master.csv`

This dataset contains **investor demographic and risk-related information**, enabling **segmentation and behavioral analytics**.

| Column Name | Explanation |
|------------|------------|
| `investor_id` | Unique identifier for each investor |
| `investor_type` | Category of investor (Retail / HNI / Institutional) |
| `risk_profile` | Investor’s risk appetite (Low / Medium / High) |
| `region` | Geographic region of the investor |

---


## 6. Bronze Layer – Raw Data Ingestion

**Objective:** Preserve original data exactly as received.

- Characteristics: No transformations, Full traceability, Recovery-ready

### Implementation:
- Python ingestion script
- Executed via Airflow
- Logs captured for monitoring and stored in bronze.log file

---

## 7. Silver Layer – Data Cleaning & Transformation

**Objective:** Improve data quality and prepare for analytics.
- Cleaning : Remove null values, Remove duplicate records, Normalize data types

### Feature Engineering:
**Daily Return**
daily_return = (close_price - open_price) / open_price

### Outputs:
- `investor_master_clean.csv`
- `portfolio_transactions_clean.csv`
- `stock_prices_clean.csv`

---

## 8. Gold Layer – Business Analytics (Databricks)


In this project, the **Gold Layer** represents the final analytical layer where cleaned and transformed data from the Silver Layer is converted into **business-ready aggregated datasets**.

The purpose of the Gold Layer is to answer key business questions related to:

1. Portfolio performance analysis  
2. Risk and volatility measurement  
3. Sector dominance and market contribution  
4. Investor trading behavior  
5. Risk-adjusted investment efficiency  

All Gold tables are **derived outputs** and are designed to be **directly consumed by Power BI** for visualization and decision-making.

---

## Gold Layer Processing Steps

### Step 1: Load Silver Tables

The following cleaned tables are retrieved from the Databricks Catalog and loaded as Spark DataFrames:

- Cleaned stock prices  
- Cleaned portfolio transactions  
- Cleaned investor master  

These DataFrames form the foundation for all Gold-layer calculations.

---

### Step 2: Gold-Level Analytics & Table Creation

### 1. `gold_portfolio_value`

This table captures the **total portfolio value over time**, aggregated by trade date.

- Transaction-level values are aggregated per day
- Each row represents portfolio value for a specific trade date

#### Formula Used
Portfolio Value (Date) = Σ (Quantity × Trade Price)

#### Columns

| Column Name | Description |
|------------|------------|
| `trade_date` | Trading date |
| `portfolio_value` | Total portfolio value for the date |


---

### 2. `gold_stock_volatility`

This table measures **price risk** for individual stocks.

- Volatility is calculated per stock using the **standard deviation of daily returns**
- Stocks are categorized into risk bands

#### Volatility Classification Logic
IF volatility < 0.012 → Low Volatility
IF volatility BETWEEN 0.012 AND 0.02 → Medium Volatility
IF volatility > 0.02 → High Volatility


#### Columns

| Column Name | Description |
|------------|------------|
| `symbol` | Stock identifier |
| `volatility` | Standard deviation of returns |
| `volatility_range` | Risk category |

---

### 3. `gold_risk_adjusted_returns`

This table evaluates **how efficiently a stock generates returns relative to its risk**.

- Average daily return calculated per stock
- Combined with volatility to compute risk-adjusted return

#### Formula Used
Risk Adjusted Return = Average Daily Return / Volatility

#### Columns

| Column Name | Description |
|------------|------------|
| `symbol` | Stock identifier |![alt text](image-1.png)
| `avg_daily_return` | Average daily return |
| `volatility` | Stock volatility |
| `risk_adjusted_return` | Return per unit of risk |

---

### 4. `gold_sector_kpis`

This table aggregates **market activity at the sector level**.

- Stock data grouped by sector
- Volume and price metrics aggregated

#### Formulas Used

Total Volume = Σ (Volume)
Average Close Price = AVG(Close Price)

#### Columns

| Column Name | Description |
|------------|------------|
| `sector` | Business sector |
| `total_volume` | Total trading volume |
| `avg_close_price` | Average closing price |

---

### 5. `gold_investor_trading_behavior`

This table classifies investors based on **how frequently they trade**, helping understand behavioral patterns.

- Total number of trades calculated per investor
- Investors categorized using defined thresholds
- Region attribute included for geographic analysis

#### Trading Behavior Classification

IF trade_count ≤ 3 → Long-Term Investor
IF trade_count BETWEEN 4 AND 6 → Swing Trader
IF trade_count > 6 → Frequent Trader

#### Columns

| Column Name | Description |
|------------|------------|
| `investor_id` | Investor identifier |
| `trade_count` | Number of trades |
| `trading_behavior` | Investor classification |
| `region` | Geographic region |

---

## 9. Airflow Orchestration

Apache Airflow is used as the core orchestration layer to schedule, coordinate, and manage the end-to-end ETL pipeline for the Stock Analytics Platform. The pipeline is implemented as a Directed Acyclic Graph (DAG) that automates the Bronze, Silver, and Gold data processing layers in a controlled and reliable manner.
![alt text](/Airflow_Ouputs/DAG_graph.png)


### DAG Design and Scheduling
- A single DAG named `stock_analytics_incremental_etl_pipeline_v2` is defined to manage the complete ETL flow.
- The DAG is scheduled to run daily using `@daily`, enabling automated incremental processing.
- Manual triggering is also supported through the Airflow UI for ad-hoc executions and testing.

### Task Orchestration Flow
The DAG executes tasks in a strict sequence to ensure data correctness:
1. **Incremental Data Check**  
   - Compares the last successful execution timestamp with raw data modification times.
   - Determines whether new data is available for processing.

2. **Bronze Layer Ingestion**  
   - Copies raw CSV files into the Bronze layer directory.
   - Acts as a controlled raw data landing zone.

3. **Silver Layer Transformation**  
   - Performs data cleaning, deduplication, null handling, and feature engineering.
   - Produces analytics-ready datasets.

4. **Gold Layer Analytics (Databricks Integration)**  
   - Triggers a Databricks Job using `DatabricksRunNowOperator`.
   - Executes advanced aggregations and analytical transformations in Databricks.
   - Enables scalable compute for heavy analytics workloads.

5. **Last Run Timestamp Update**  
   - Updates Airflow Variables to support incremental execution logic.

### Databricks Integration
- The Gold layer is implemented as a Databricks Job.
- Airflow securely triggers the job using a Databricks connection (`databricks_default`).
- The Databricks connection is defined in Docker Compose using an environment variable:

```yaml
AIRFLOW_CONN_DATABRICKS_DEFAULT: >
  databricks://token:<DATABRICKS_TOKEN>@<DATABRICKS_HOST>
  ```
- The Databricks Job ID is externalized using Airflow Variables for flexibility and reusability.
- This design allows Airflow to act as the control plane while Databricks handles large-scale computation.

### Automated Failure Notifications (Email Alerts)

The pipeline includes **proactive failure notifications** using Apache Airflow’s native email alerting mechanism.

Whenever a critical task (Gold layer analytics) fails, Airflow automatically sends an email notification to the configured recipients. This ensures quick awareness of pipeline issues without requiring manual monitoring.

#### Implementation Details
- Email alerts are enabled using Airflow’s built-in configuration:
  - `email_on_failure = True`
  - `email_on_retry = False`
- Alerts are triggered **only after task failure**, avoiding unnecessary notifications
- SMTP configuration is managed at the Docker environment level

#### Benefits
- Immediate visibility into pipeline failures
- Reduced operational risk
- Industry-standard alerting behavior
- No custom email logic inside DAGs


### Containerized Execution
- Airflow runs inside Docker containers using Docker Compose.
- DAGs, logs, data, and project files are mounted via Docker volumes.
- This ensures environment consistency and portability across systems.

---

## 10. Monitoring, Logging & Error Handling

Robust monitoring, logging, and error handling mechanisms are implemented across all ETL layers to ensure reliability, traceability, and operational transparency.

### Airflow Monitoring
- Airflow UI provides real-time visibility into DAG runs, task states, durations, and retries.
- Each task execution is visually tracked using Graph, Gantt, and Tree views.
- Task-level success, failure, retries, and upstream dependencies are clearly monitored.

### Retry & Failure Handling
- Default retry policies are defined at the DAG level:
  - Failed tasks are retried automatically up to a configured limit.
  - Retry delays prevent immediate repeated failures.
- Failures in any layer halt downstream execution, preventing bad data propagation.

### Application-Level Logging
Separate application logs are implemented for each ETL layer:
- Bronze Layer Logging : Logs ingestion start, file copy operations, and completion status.
- Silver Layer Logging : Logs data cleaning steps, transformations, and feature engineering actions.
- Gold Layer Logging : Execution status is tracked through Airflow task logs.

### Centralized Log Storage
- Airflow logs are written to `/opt/airflow/logs` inside the container.
- Docker volume mapping persists logs to the local project `logs/` directory.


### Error Visibility
- All exceptions are logged with full stack traces.
- Task failures are immediately visible in the Airflow UI.

---

# 11. Power BI Integration

### Databricks Connection & Data Preparation

Power BI is connected to Azure Databricks using the native **Databricks connector**.

**Connection Steps:**
- Retrieved **Server Hostname** and **HTTP Path** from Databricks cluster
- Generated a **Personal Access Token** from Databricks user settings
- Connected via **Power BI Desktop → Get Data → Azure Databricks**

Before visualization, data was cleaned and prepared using **Power Query**:
- Corrected data types (e.g., `total_volume` converted from Decimal to Whole Number)
- Created conditional columns:
  - **Performance Category** (Good / Poor) based on risk-adjusted returns
  - **Volatility Range** (High / Low) for intuitive risk analysis

The semantic model is built on curated **Gold-layer tables**, ensuring consistency across all dashboards.

---

## DAX Measures

Custom DAX measures were implemented to support analytical insights, including:
- Total Trading Volume
- Average Closing Price
- Number of Active Stocks / Investors
- Risk-Adjusted Performance Index
- Risk Efficiency Score
- Portfolio Momentum Index
- Average Trading Intensity

---

## Dashboards Overview

### Stock Price Trend & Portfolio Performance
This combined view provides a **market and portfolio-level perspective**. It highlights sector-wise price trends, trading volume concentration, portfolio value movement over time, and performance distribution. Together, these insights help assess overall market behavior while understanding whether the portfolio is growing, stable, or declining and what factors influence its value.

---

### Investor Behaviour Analysis
This dashboard focuses on **how investors trade**, analyzing trading frequency, intensity, behavior types, regional participation, and top contributors. An R-based histogram with density curve is used to study the distribution of trading activity, helping identify common patterns and outliers in investor behavior.

---

### Risk & Volatility Indicators
This dashboard provides a **comprehensive risk perspective**, analyzing volatility, risk-adjusted returns, and efficiency metrics across stocks and sectors. It supports monitoring overall portfolio risk, identifying high-risk or inefficient stocks, and understanding the trade-off between risk and return.

---

## Publishing, Alerts & Monitoring

- Reports and semantic model were published to **Power BI Service**
- KPI-based alerts were configured on card visuals
  - Example: **Risk Efficiency Score alert**
  - Triggered when value drops below **0.5**
  - Notifications sent via **email and Teams**
---


## 12. Refresh Behaviour (PowerBI Desktop & PowerBI Service)

**Power BI Desktop Refresh:**
- Used during development to validate schema changes and new data

**Power BI Service Refresh:**
- Dataset published to Power BI Service
- Credentials configured using **Databricks token-based authentication**
- Manual refresh tested successfully
- Scheduled refresh enabled to automatically pull updated data from Databricks

**Refresh Validation:**
- Before refresh: **273 records**
- After refresh: **274 records**
- Confirms successful data and schema synchronization

---

## 13. Key Outcomes

- Automated end-to-end pipeline
- Scalable analytics architecture
- Clear financial insights

---

## 14. How to Run the Project

1. **Start Airflow**
   - Navigate to the directory containing `docker-compose.yml` and run:
     ```bash
     docker-compose up -d
     ```
   - This starts all Airflow services and exposes the UI at `http://localhost:8080`.

2. **Verify Services**
   - Ensure all containers are running:
     ```bash
     docker ps
     ```

3. **Load Raw Data**
   - Place or update input CSV files in:
     ```
     Data/raw/
     ```

4. **Trigger the ETL Pipeline**
   - Open the Airflow UI and enable the DAG:
     ```
     stock_analytics_incremental_etl_pipeline_v2
     ```
   - Trigger the DAG manually or allow it to run on its scheduled interval.

5. **Monitor Execution**
   - Track task execution and status using Airflow’s Graph or Gantt views.
   - View logs directly in the Airflow UI or from the local `logs/` directory.

6. **Refresh Analytics**
   - Open the Power BI report and refresh to view the latest Gold layer analytics.


---

## 15. Conclusion

This project demonstrates strong data engineering fundamentals, real-world financial analytics, and enterprise-grade orchestration using modern data platforms.

