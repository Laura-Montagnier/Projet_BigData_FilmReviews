from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, DateType
import json
from pathlib import Path


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
    # Lire les fichiers JSON en utilisant le schéma prédéfini
    df = spark.read.schema(schema).json(data_file_paths)
    
    # Afficher le schéma
    df.printSchema()
    
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


print(f"DF_RAW.count(): {DF_RAW.count()}")


def compute_average_rating(df: DataFrame) -> DataFrame:
    """
    Compute the average rating for each item_id
    """
    # Group by item_id and compute the average rating
    df_avg = df.groupBy("item_id").avg("rating").withColumnRenamed("avg(rating)", "avg_rating")
    
    return df_avg

DF_AVG_RATING = compute_average_rating(DF_RAW)


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

from pyspark.sql.functions import col


df_best_rated_movies = DF_AVG_RATING.join(
    DF_RAW_2, on="item_id", how="inner"
).select("title", "avg_rating")

from pyspark.sql.functions import desc

df_best_rated_movies_sorted = df_best_rated_movies.orderBy(desc("avg_rating"))
df_best_rated_movies_sorted.show(10, truncate=False)