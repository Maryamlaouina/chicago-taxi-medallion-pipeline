from pyspark.sql import DataFrame, functions as F
from scripts.spark_utils import get_spark
from scripts.config import BRONZE_PATH, SILVER_PATH, REJECTED_PATH

# Seuils décidés à partir de l'exploration des données (voir README.md pour plus d'informations)
MIN_TRIP_SECONDS = 60          # en dessous : course annulée ou erreur de compteur
MAX_TRIP_SECONDS = 4 * 3600    # 99 % des trajets durent moins de 65 min
MAX_TRIP_MILES = 100           # 99 % des trajets font moins de 25,5 miles
MIN_FARE = 3.25                # tarif de prise en charge à Chicago
MAX_TRIP_TOTAL = 1000          # au-delà : erreurs de saisie (ex. 9 999,75 $)

TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS"


def cast_columns(df: DataFrame) -> DataFrame:
    """Typage des colonnes et suppression des colonnes inutiles."""
    return df.select(
        "trip_id",
        "taxi_id",
        F.to_timestamp("trip_start_timestamp", TS_FORMAT).alias("trip_start_ts"),
        F.to_timestamp("trip_end_timestamp", TS_FORMAT).alias("trip_end_ts"),
        F.col("trip_seconds").cast("int"),
        F.col("trip_miles").cast("double"),
        F.col("pickup_community_area").cast("int"),
        F.col("dropoff_community_area").cast("int"),
        F.col("fare").cast("double"),
        F.col("tips").cast("double"),
        F.col("tolls").cast("double"),
        F.col("extras").cast("double"),
        F.col("trip_total").cast("double"),
        F.trim("payment_type").alias("payment_type"),
        F.trim("company").alias("company"),
        F.col("pickup_centroid_latitude").cast("double").alias("pickup_lat"),
        F.col("pickup_centroid_longitude").cast("double").alias("pickup_lon"),
        F.col("dropoff_centroid_latitude").cast("double").alias("dropoff_lat"),
        F.col("dropoff_centroid_longitude").cast("double").alias("dropoff_lon"),
        "year",
        "month",
    )


def add_reject_reason(df: DataFrame) -> DataFrame:
    """Attribue à chaque ligne invalide la première raison de rejet rencontrée."""
    c = F.col
    reason = (
        F.when(c("trip_start_ts").isNull() | c("trip_end_ts").isNull(), "missing_timestamp")
        .when(c("trip_seconds").isNull(), "missing_duration")
        .when(c("trip_miles").isNull(), "missing_distance")
        .when(c("fare").isNull() | c("trip_total").isNull(), "missing_amount")
        .when(c("trip_end_ts") < c("trip_start_ts"), "end_before_start")
        .when((c("trip_seconds") < MIN_TRIP_SECONDS) | (c("trip_seconds") > MAX_TRIP_SECONDS), "invalid_duration")
        .when(c("trip_miles") > MAX_TRIP_MILES, "invalid_distance")
        .when((c("fare") < MIN_FARE) | (c("trip_total") > MAX_TRIP_TOTAL), "invalid_amount")
    )
    return df.withColumn("reject_reason", reason)


def transform_silver() -> dict:
    spark = get_spark("bronze-to-silver")

    raw = spark.read.option("header", True).csv(BRONZE_PATH)
    nb_bronze = raw.count()

    deduped = raw.dropDuplicates(["trip_id"])
    nb_duplicates = nb_bronze - deduped.count()

    checked = add_reject_reason(cast_columns(deduped)).cache()

    print("=== Rapport qualité : lignes rejetées par raison ===")
    checked.filter(F.col("reject_reason").isNotNull()) \
        .groupBy("reject_reason").count().orderBy(F.desc("count")).show(truncate=False)

    clean = (
        checked.filter(F.col("reject_reason").isNull())
        .drop("reject_reason")
        # 0 mile = distance non enregistrée -> NULL pour ne pas fausser les moyennes
        .withColumn("trip_miles", F.when(F.col("trip_miles") > 0, F.col("trip_miles")))
        .withColumn("trip_date", F.to_date("trip_start_ts"))
    )
    rejected = checked.filter(F.col("reject_reason").isNotNull())

    clean.repartition("year", "month") \
        .write.mode("overwrite").partitionBy("year", "month").parquet(SILVER_PATH)

    rejected.coalesce(1).write.mode("overwrite").parquet(REJECTED_PATH)

    nb_silver = spark.read.parquet(SILVER_PATH).count()
    nb_rejected = spark.read.parquet(REJECTED_PATH).count()
    checked.unpersist()
    spark.stop()

    report = {
        "bronze": nb_bronze,
        "duplicates": nb_duplicates,
        "rejected": nb_rejected,
        "silver": nb_silver,
    }
    print(f"=== Bilan : {report} ===")
    assert nb_bronze == nb_duplicates + nb_rejected + nb_silver, "Des lignes ont été perdues !"
    return report


if __name__ == "__main__":
    transform_silver()