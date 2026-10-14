import os
import time
import requests
from scripts.config import SODA_API_URL, SODA_PAGE_SIZE, BUCKET, BRONZE_PREFIX
from scripts.minio_utils import get_s3_client, ensure_bucket, delete_prefix


def fetch_page(start: str, end: str, offset: int) -> str:
    """Récupère une page de résultats au format CSV (texte brut), avec réessais."""
    params = {
        "$where": f"trip_start_timestamp >= '{start}' AND trip_start_timestamp < '{end}'",
        "$order": ":id",
        "$limit": SODA_PAGE_SIZE,
        "$offset": offset,
    }
    headers = {}
    token = os.environ.get("SODA_APP_TOKEN")
    if token:
        headers["X-App-Token"] = token

    for attempt in range(5):
        try:
            response = requests.get(SODA_API_URL, params=params, headers=headers, timeout=300)
            response.raise_for_status()
            return response.text
        except requests.RequestException as error:
            wait = 2 ** attempt  # 1s, 2s, 4s, 8s, 16s
            print(f"Erreur API (tentative {attempt + 1}/5) : {error}. Nouvel essai dans {wait}s")
            time.sleep(wait)

    raise RuntimeError(f"Échec de récupération de la page offset={offset}")


def ingest_month(year: int, month: int) -> int:
    """Ingère un mois complet dans bronze/ et renvoie le nombre de lignes."""
    start = f"{year}-{month:02d}-01T00:00:00"
    next_year, next_month = (year + 1, 1) if month == 12 else (year, month + 1)
    end = f"{next_year}-{next_month:02d}-01T00:00:00"

    prefix = f"{BRONZE_PREFIX}/year={year}/month={month:02d}/"
    s3 = get_s3_client()
    ensure_bucket(BUCKET)
    delete_prefix(BUCKET, prefix)

    offset, page, total = 0, 0, 0
    while True:
        csv_text = fetch_page(start, end, offset)
        nb_rows = len(csv_text.splitlines()) - 1
        if nb_rows <= 0:
            break

        key = f"{prefix}part-{page:05d}.csv"
        s3.put_object(Bucket=BUCKET, Key=key, Body=csv_text.encode("utf-8"), ContentType="text/csv")
        print(f"{key} : {nb_rows} lignes")

        total += nb_rows
        page += 1
        offset += SODA_PAGE_SIZE
        if nb_rows < SODA_PAGE_SIZE:
            break

    print(f"Mois {year}-{month:02d} terminé : {total} lignes")
    return total


if __name__ == "__main__":
    # Test : on ingère seulement janvier 2023 pour tester le fonctionnement
    ingest_month(2023, 1)
