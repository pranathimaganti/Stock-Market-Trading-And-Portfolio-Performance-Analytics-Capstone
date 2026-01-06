import os
import pandas as pd
import logging

# Robust environment detection
IS_AIRFLOW = "AIRFLOW_HOME" in os.environ

if IS_AIRFLOW:
    BASE_DIR = "/opt/airflow/project/Data"
    LOG_DIR = "/opt/airflow/logs"
else:
    BASE_DIR = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "Data")
    )
    LOG_DIR = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "logs")
    )

RAW_DIR = os.path.join(BASE_DIR, "Bronze")
PROCESSED_DIR = os.path.join(BASE_DIR, "processed")

os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(LOG_DIR, "silver_layer.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# Silver Layer Transformation Logic
def run_silver_layer():
    try:
        logging.info("Silver layer transformation started")

        # Validate RAW directory
        if not os.path.exists(RAW_DIR):
            raise FileNotFoundError(f"Raw directory not found: {RAW_DIR}")


        # Investor Master Cleaning

        # RAW_DIR : Data/Bronze/
        investor_path = os.path.join(RAW_DIR, "investor_master.csv")
        investor_df = pd.read_csv(investor_path)

        investor_df.dropna(inplace=True)
        investor_df.drop_duplicates(inplace=True)

        # Feature engineering: risk category
        investor_df["risk_category"] = investor_df["risk_profile"].apply(
            lambda x: "High Risk" if x == "High" else "Low Risk"
        )

        investor_df.to_csv(
            os.path.join(PROCESSED_DIR, "investor_master_clean.csv"),
            index=False
        )

        logging.info("Investor master cleaned successfully")

        # Portfolio Transactions Cleaning
        txn_path = os.path.join(RAW_DIR, "portfolio_transactions.csv")
        txn_df = pd.read_csv(txn_path)
        print(txn_df.head())
        print(txn_df.info())
        txn_df["trade_date"] = pd.to_datetime(txn_df["trade_date"],errors="coerce")
        print(txn_df.head())
        print(txn_df.info())

        txn_df.dropna(inplace=True)
        txn_df.drop_duplicates(inplace=True)

        txn_df.to_csv(
            os.path.join(PROCESSED_DIR, "portfolio_transactions_clean.csv"),
            index=False
        )

        logging.info("Portfolio transactions cleaned successfully")


        # Stock Prices Cleaning & Feature Engineering
        stock_path = os.path.join(RAW_DIR, "stock_prices.csv")
        stock_df = pd.read_csv(stock_path)
        stock_df["trade_date"] = pd.to_datetime(stock_df["trade_date"],errors="coerce")

        stock_df.dropna(inplace=True)
        stock_df.drop_duplicates(inplace=True)

        # Feature engineering: daily return
        stock_df["daily_return"] = (
            stock_df["close_price"] - stock_df["open_price"]
        ) / stock_df["open_price"]

        # Moving averages
        stock_df["ma_5"] = (stock_df.groupby("symbol")["close_price"].transform(lambda x: x.rolling(5).mean()))
        stock_df["ma_20"] = (stock_df.groupby("symbol")["close_price"].transform(lambda x: x.rolling(20).mean()))

        stock_df.to_csv(
            os.path.join(PROCESSED_DIR, "stock_prices_clean.csv"),
            index=False
        )

        logging.info("Stock prices cleaned successfully")
        logging.info("Silver layer completed successfully")

    except Exception as e:
        logging.error("Silver layer failed", exc_info=True)
        raise


if __name__ == "__main__":
    run_silver_layer()
