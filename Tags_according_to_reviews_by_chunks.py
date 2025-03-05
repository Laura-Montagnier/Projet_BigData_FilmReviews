from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, udf
from pyspark.sql.types import BooleanType
import os

spark = SparkSession.builder.appName("Tags_according_to_reviews").getOrCreate()

reviews_df = spark.read.json("../raw/reviews.json").select("item_id", "txt")
tags_df = spark.read.json("../raw/tags.json").select("tag", "id")


def contains_tag(text, tag):
    return tag.lower() in text.lower() if text else False

contains_tag_udf = udf(contains_tag, BooleanType())


def process_in_chunks(reviews_df, tags_df, chunk_size=100000):
    total_rows = reviews_df.count()
    num_chunks = (total_rows // chunk_size) + 1
    
    for chunk_index in range(num_chunks):
        chunk_df = reviews_df.limit(chunk_size).offset(chunk_index * chunk_size)
        

        matched_df = chunk_df.crossJoin(tags_df).filter(contains_tag_udf(col("txt"), col("tag")))
        

        chunk_parquet_path = f"tags_according_to_reviews_chunk_{chunk_index}.parquet"
        matched_df.select("item_id", "id").write.parquet(chunk_parquet_path, mode="append")
        
        print(f"Chunk {chunk_index + 1} traité et sauvegardé.")


process_in_chunks(reviews_df, tags_df)
