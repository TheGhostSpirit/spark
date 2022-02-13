from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("myproject").master("local").getOrCreate()

full_file = "partial.csv"

full_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(full_file)

# Afficher dans la console le plus gros contributeur (la personne qui a fait le plus de
# commit) du projet apache/spark.
full_df.filter(col("repo") == "apache/spark")\
    .groupby("author")\
    .agg(countDistinct("commit").alias("numberOfCommits")) \
    .orderBy("numberOfCommits", ascending=False) \
    .show(n=1)
