from pyspark.sql import DataFrame, Window, functions as F
from scripts.spark_utils import get_spark
from scripts.community_areas import COMMUNITY_AREAS
from scripts.config import SILVER_PATH, GOLD_PATH

# Règle métier : ces paiements ne sont pas du revenu encaissé
NON_REVENUE_PAYMENTS = ["No Charge", "Dispute"]


def write_gold(df: DataFrame, table: str) -> int:
    """Écrit une table Gold (petite : un seul fichier) et renvoie son nombre de lignes."""
    df.coalesce(1).write.mode("overwrite").parquet(f"{GOLD_PATH}/{table}")
    return df.count()


def build_gold() -> dict:
    spark = get_spark("silver-to-gold")

    is_paid = ~F.col("payment_type").isin(NON_REVENUE_PAYMENTS)
    trips = (
        spark.read.parquet(SILVER_PATH)
        .withColumn("revenue", F.when(is_paid, F.col("trip_total")).otherwise(0.0))
        .withColumn("is_paid", is_paid.cast("int"))
        .cache()
    )

    # Les agrégats réutilisés par plusieurs tables
    common_metrics = [
        F.count("*").alias("nb_trips"),
        F.round(F.sum("revenue"), 2).cast("decimal(14,2)").alias("revenue"),
        F.round(F.sum("revenue") / F.sum("is_paid"), 2).cast("decimal(10,2)").alias("avg_revenue_per_trip"),
        F.round(F.avg("trip_seconds") / 60, 1).alias("avg_duration_min"),
        F.round(F.avg("trip_miles"), 2).alias("avg_distance_miles"),
    ]

    # 1. KPIs par jour
    daily_kpis = (
        trips.groupBy("trip_date")
        .agg(*common_metrics, F.round(F.sum("tips"), 2).cast("decimal(12,2)").alias("total_tips"))
        .orderBy("trip_date")
    )

    # 2. Profil par heure de la journée
    nb_days = trips.select("trip_date").distinct().count()
    hourly_patterns = (
        trips.withColumn("hour", F.hour("trip_start_ts"))
        .groupBy("hour")
        .agg(*common_metrics)
        .withColumn("avg_trips_per_day", F.round(F.col("nb_trips") / nb_days, 1))
        .orderBy("hour")
    )

    # 3. Quartiers de prise en charge
    zones = spark.createDataFrame(COMMUNITY_AREAS, ["area_id", "zone_name"])
    ranking = Window.orderBy(F.desc("nb_trips"))
    pickup_zones = (
        trips.filter(F.col("pickup_community_area").isNotNull())
        .groupBy("pickup_community_area")
        .agg(*common_metrics)
        .join(zones, F.col("pickup_community_area") == F.col("area_id"), "left")  # la jointure
        .drop("area_id")
        .withColumn("rank", F.row_number().over(ranking))
        .orderBy("rank")
    )

    report = {
        "daily_kpis": write_gold(daily_kpis, "daily_kpis"),
        "hourly_patterns": write_gold(hourly_patterns, "hourly_patterns"),
        "pickup_zones": write_gold(pickup_zones, "pickup_zones"),
        "dim_community_areas": write_gold(zones, "dim_community_areas"),
    }

    print("=== Aperçu : top 10 des zones de prise en charge ===")
    spark.read.parquet(f"{GOLD_PATH}/pickup_zones").orderBy("rank").show(10, truncate=False)
    print("=== Aperçu : premiers jours ===")
    spark.read.parquet(f"{GOLD_PATH}/daily_kpis").orderBy("trip_date").show(5)

    trips.unpersist()
    spark.stop()
    print(f"=== Tables Gold écrites : {report} ===")
    return report


if __name__ == "__main__":
    build_gold()
