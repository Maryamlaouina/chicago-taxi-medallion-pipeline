from datetime import datetime
from airflow.sdk import dag, task
from scripts.config import MONTHS


@dag(
    dag_id="chicago_taxi_pipeline",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["chicago_taxi"],
)
def chicago_taxi_pipeline():
    # --- Bronze
    @task(retries=2)
    def ingest_bronze(period: dict) -> int:
        """Télécharge un mois depuis l'API SODA et le dépose dans bronze/."""
        from scripts.ingest_bronze import ingest_month
        return ingest_month(period["year"], period["month"])

    @task
    def log_bronze_total(counts: list[int]) -> int:
        """Affiche le volume total ingéré."""
        total = sum(counts)
        print(f"Bronze : {total} lignes ingérées sur {len(counts)} mois")
        return total

    # --- Silver
    @task
    def transform_silver() -> dict:
        """Nettoie Bronze avec Spark et écrit silver/ (+ quarantaine des rejets)."""
        from scripts.transform_silver import transform_silver as run
        return run()

    # --- Gold
    @task
    def build_gold() -> dict:
        """Calcule les tables d'indicateurs pour la BI à partir de Silver."""
        from scripts.build_gold import build_gold as run
        return run()

    counts = ingest_bronze.expand(period=MONTHS)
    log_bronze_total(counts) >> transform_silver() >> build_gold()


chicago_taxi_pipeline()
