from pyspark.sql import functions as F
from scripts.spark_utils import get_spark
from scripts.config import BRONZE_PATH

spark = get_spark("explore-thresholds")
df = spark.read.option("header", True).csv(BRONZE_PATH)

num = df.select(
    F.col("trip_seconds").cast("double").alias("trip_seconds"),
    F.col("trip_miles").cast("double").alias("trip_miles"),
    F.col("fare").cast("double").alias("fare"),
)

# Combien de lignes chaque nouveau seuil va-t-il retirer en plus ?
print("Trajets entre 1 et 59 secondes :",
      num.filter((F.col("trip_seconds") > 0) & (F.col("trip_seconds") < 60)).count())

print("Trajets avec un prix entre 0 et 3,25 $ :",
      num.filter((F.col("fare") > 0) & (F.col("fare") < 3.25)).count())

# Les trajets à 0 mile sont-ils de vrais trajets (vraie durée, vrai prix) ?
print("=== Les trajets à 0 mile : quelle durée et quel prix ? ===")
num.filter(F.col("trip_miles") == 0) \
   .select("trip_seconds", "fare") \
   .summary("count", "25%", "50%", "75%") \
   .show()

spark.stop()