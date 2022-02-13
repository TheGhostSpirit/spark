from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("myproject").master("local").getOrCreate()

full_file = "partial.csv"

full_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(full_file)

# Afficher dans la console les 10 projets Github pour lesquels il y a eu le plus de commit
full_df.select("repo","commit")\
    .groupby("repo")\
    .agg(count("commit").alias("countCommit"))\
    .orderBy("countCommit", ascending=False)\
    .show(n=10)
