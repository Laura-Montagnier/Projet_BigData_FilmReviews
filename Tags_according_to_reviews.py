from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, udf
from pyspark.sql.types import BooleanType

spark = SparkSession.builder.appName("Tags_according_to_reviews").getOrCreate()

reviews_df = spark.read.json("../raw/reviews_bis.json").select("item_id", "txt")
tags_df = spark.read.json("../raw/tags.json").select("tag", "id")

def contains_tag(text, tag):
    return tag.lower() in text.lower() if text else False

contains_tag_udf = udf(contains_tag, BooleanType())
##User-Defined Functions (UDFs) are user-programmable routines that act on one row. Source : Apache Spark

matched_df = reviews_df.crossJoin(tags_df).filter(contains_tag_udf(col("txt"), col("tag")))

matched_df.select("item_id", "id").write.parquet("tags_according_to_reviews.parquet", mode="overwrite")

print("Parquet file successfully created.")