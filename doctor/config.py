import os


class Settings:
    airflow_url = os.environ.get("AIRFLOW_URL", "http://localhost:8080")
    airflow_user = os.environ.get("AIRFLOW_USER", "")
    airflow_password = os.environ.get("AIRFLOW_PASSWORD", "")
    warehouse_dsn = os.environ.get("WAREHOUSE_DSN", "")  # empty = warehouse tools disabled
    max_output_chars = int(os.environ.get("MAX_OUTPUT_CHARS", "6000"))


settings = Settings()
