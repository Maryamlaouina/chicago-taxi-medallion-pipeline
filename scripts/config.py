# --- Source de données ---
SODA_API_URL = "https://data.cityofchicago.org/resource/wrvz-psew.csv"
SODA_PAGE_SIZE = 50_000

# --- Période ingérée : 1er trimestre 2023 ---
MONTHS = [{"year": 2023, "month": m} for m in (1, 2, 3)]

# --- Datalake (MinIO) ---
BUCKET = "datalake"
BRONZE_PREFIX = "bronze/taxi_trips"

BRONZE_PATH = f"s3a://{BUCKET}/{BRONZE_PREFIX}/"
SILVER_PATH = f"s3a://{BUCKET}/silver/taxi_trips/"
REJECTED_PATH = f"s3a://{BUCKET}/silver/taxi_trips_rejected/"
GOLD_PATH = f"s3a://{BUCKET}/gold"