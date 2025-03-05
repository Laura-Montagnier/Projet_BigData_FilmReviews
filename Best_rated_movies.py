from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, DateType
import json
from pathlib import Path
from pyspark.sql.functions import desc, col, count

#This code uses two Json, ratings and metadata. It returns the best rated movies,
#sorted by ratings, as well as the number of votes. To avoid having movies that 
#seem incredible but are incredible only for two or three persons, we are adding
#a limit : more than 50 people must have voted for the rating to appear.


def get_spark() -> SparkSession:
    """
    Return a Spark session
    """
    return SparkSession.builder \
        .appName("Rating Analysis") \
        .getOrCreate()


spark = get_spark()


def read_json_with_schema(data_file_paths: list[str], schema: StructType) -> DataFrame:
    """
    Read JSON files using a predefined schema and return the DataFrame.
    """
    df = spark.read.schema(schema).json(data_file_paths)    
    return df



schema = StructType([
    StructField("item_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("rating", FloatType(), True)
])


fichier_json_1 = "../raw/ratings.json"
assert Path(fichier_json_1).exists(), f"{fichier_json_1} does not exist"

DATA_FILE_PATHS = [fichier_json_1]
DF_RAW = read_json_with_schema(data_file_paths=DATA_FILE_PATHS, schema=schema)

assert DF_RAW is not None, "Returned result is None"
assert isinstance(DF_RAW, DataFrame), "Returned result is not a valid DataFrame"

def compute_average_rating(df: DataFrame) -> DataFrame:
    """
    Compute the average rating for each item_id
    """
    # Group by item_id and compute the average rating
    df_avg = df.groupBy("item_id").avg("rating").withColumnRenamed("avg(rating)", "avg_rating")
    
    return df_avg


DF_AVG_RATING = compute_average_rating(DF_RAW)

DF_RATING_COUNT = DF_RAW.groupBy("item_id").count().withColumnRenamed("count", "count_rating")
DF_AVG_WITH_COUNT = DF_AVG_RATING.join(DF_RATING_COUNT, on="item_id", how="inner")
DF_AVG_WITH_COUNT_FILTERED = DF_AVG_WITH_COUNT.filter(col("count_rating")>50)

schema_metadata = StructType([
    StructField("title", StringType(), True),
    StructField("directedBy", StringType(), True),
    StructField("starring", StringType(), True),
    StructField("dateAdded", DateType(), True),
    StructField("avgRating", FloatType(), True),
    StructField("imdbId", IntegerType(), True),
    StructField("item_id", IntegerType(), True)
])

fichier_json_2 = "../raw/metadata.json"
assert Path(fichier_json_2).exists(), f"{fichier_json_2} does not exist"

DATA_FILE_PATHS_2 = [fichier_json_2]
DF_RAW_2 = read_json_with_schema(data_file_paths=DATA_FILE_PATHS_2, schema=schema_metadata)


df_best_rated_movies = DF_AVG_WITH_COUNT_FILTERED.join(
    DF_RAW_2, on="item_id", how="inner"
).select("title", "avg_rating", "count_rating")

df_best_rated_movies_sorted = df_best_rated_movies.orderBy(desc("avg_rating"), desc("count_rating"))
df_best_rated_movies_sorted.show(10, truncate=False)