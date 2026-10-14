from pyspark.sql import functions as F
from scripts.spark_utils import get_spark
from scripts.config import BRONZE_PATH

spark = get_spark("explore-bronze")

df = spark.read.option("header", True).csv(BRONZE_PATH)

print("=== Schéma ===")
df.printSchema()

print(f"=== Nombre de lignes : {df.count()} ===")

print("=== Exemple de lignes ===")
df.show(5, truncate=False, vertical=True)

print("=== Valeurs nulles par colonne ===")
df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]) \
  .show(vertical=True)

print("=== Doublons sur trip_id ===")
print(df.count() - df.select("trip_id").distinct().count())

spark.stop()