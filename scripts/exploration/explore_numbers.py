from pyspark.sql import functions as F
from scripts.spark_utils import get_spark
from scripts.config import BRONZE_PATH

spark = get_spark("explore-numbers")
df = spark.read.option("header", True).csv(BRONZE_PATH)

cols = ["trip_seconds", "trip_miles", "fare", "tips", "tolls", "extras", "trip_total"]
num = df.select([F.col(c).cast("double").alias(c) for c in cols])

print("=== Distribution des valeurs numériques ===")
num.summary("min", "1%", "25%", "50%", "75%", "99%", "max").show()

print("=== Nombre de valeurs <= 0 par colonne ===")
num.select([F.sum((F.col(c) <= 0).cast("int")).alias(c) for c in cols]).show()

print("=== Types de paiement ===")
df.groupBy("payment_type").count().orderBy(F.desc("count")).show()

spark.stop()