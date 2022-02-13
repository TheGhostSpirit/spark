from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("myproject").master("local").getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

DATE_PATTERN = "EEE MMM dd HH:mm:ss yyyy Z"

full_file = "partial.csv"

full_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(full_file)

# Afficher dans la console les plus gros contributeurs du projet apache/spark sur les 24
# derniers mois.
full_df.filter(months_between(current_date(), to_date(col("date"), DATE_PATTERN)) < 24)\
    .where(col("repo") == "apache/spark") \
    .groupby("author") \
    .agg(countDistinct("commit").alias("numberOfCommits")) \
    .orderBy("numberOfCommits", ascending=False) \
    .show(n=100)
