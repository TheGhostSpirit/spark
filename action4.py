from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("myproject").master("local").getOrCreate()

full_file = "partial.csv"

full_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(full_file)

# Afficher dans la console les 10 mots qui reviennent le plus dans les messages de
# commit sur l’ensemble des projets.
full_df.select("repo", explode(split(col("message"), '[ ]')).alias('words'))\
    .groupby("words")\
    .agg(count("words").alias("nbOccurrences")) \
    .orderBy("nbOccurrences", ascending=False) \
    .show(n=100)
