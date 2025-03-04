from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
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


from pyspark.sql.functions import desc

DF_AVG_RATING_SORTED = DF_AVG_RATING.orderBy(desc("avg_rating"))
DF_AVG_RATING_SORTED.show(10)
