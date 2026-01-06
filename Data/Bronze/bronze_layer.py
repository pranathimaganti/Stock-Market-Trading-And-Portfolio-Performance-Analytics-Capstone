import os
import shutil
import logging

RAW_DIR = "/opt/airflow/project/Data/raw"
BRONZE_DIR = "/opt/airflow/project/Data/Bronze"
LOG_DIR = "/opt/airflow/logs"

os.makedirs(BRONZE_DIR, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(LOG_DIR, "bronze.log"),
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

def run_bronze_layer():
    try:
        logging.info("Bronze layer ingestion started")
        if not os.path.exists(RAW_DIR):
            raise FileNotFoundError(f"Raw directory not found: {RAW_DIR}")

        files_copied=0

        for file in os.listdir(RAW_DIR):
            if file.endswith(".csv"):
                src = os.path.join(RAW_DIR, file)
                dst = os.path.join(BRONZE_DIR, file)
                shutil.copyfile(src, dst)
                logging.info(f"Copied file: {file}")
                files_copied+=1

        logging.info(f"Bronze layer completed successfully. Files copied: {files_copied}")

    except Exception:
        logging.error("Bronze layer failed", exc_info=True)
        raise

if __name__ == "__main__":
    run_bronze_layer()
