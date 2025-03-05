import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType


# Définition des variables d'environnement
SPARK_MASTER = "spark://" + os.getenv("SPARK_MASTER", "spark://129.104.73.59:7077")
HDFS_BASE_PATH = os.getenv("HDFS_BASE_PATH", "hdfs://localhost:9000/user/blandine")


def get_spark() -> SparkSession:
    """
    Return a Spark session configured with the master URL
    """
    return SparkSession.builder \
        .appName("Rating Analysis") \
        .master(SPARK_MASTER) \
        .getOrCreate()


spark = get_spark()


def read_json_with_schema(data_file_paths: list[str], schema: StructType) -> DataFrame:
    """
    Read JSON files using a predefined schema and return the DataFrame.
    """
    df = spark.read.schema(schema).json(data_file_paths)
    df.printSchema()
    return df


# Schéma des données JSON
schema = StructType([
    StructField("item_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("rating", FloatType(), True)
])

# Définition des chemins d'entrée et de sortie sur HDFS
DATA_FILE_PATHS = [f"{HDFS_BASE_PATH}/ratings.json"]
OUTPUT_PATH = f"{HDFS_BASE_PATH}/output/ratings_parquet"

# Lire les données en utilisant le schéma défini
DF_RAW = read_json_with_schema(data_file_paths=DATA_FILE_PATHS, schema=schema)

# Vérification
assert DF_RAW is not None, "Returned result is None"
assert isinstance(DF_RAW, DataFrame), "Returned result is not a valid DataFrame"

print(f"DF_RAW.count(): {DF_RAW.count()}")


def compute_average_rating(df: DataFrame) -> DataFrame:
    """
    Compute the average rating for each item_id
    """
    return df.groupBy("item_id").avg("rating").withColumnRenamed("avg(rating)", "avg_rating")


# Calcul de la moyenne des évaluations
DF_AVG_RATING = compute_average_rating(DF_RAW)
DF_AVG_RATING.show(10)


def write_to_parquet(df: DataFrame, output_path: str):
    """
    Write DataFrame to Parquet format
    """
    df.write.mode("overwrite").parquet(output_path)


# Sauvegarde des résultats au format Parquet sur HDFS
write_to_parquet(df=DF_AVG_RATING, output_path=OUTPUT_PATH)
print(f"Output path {OUTPUT_PATH} has been written successfully.")


def read_parquet(input_path: str) -> DataFrame:
    """
    Read parquet file
    """
    return spark.read.parquet(input_path)


# Lire les fichiers Parquet générés depuis HDFS
DF_PARQUET = read_parquet(input_path=OUTPUT_PATH)

DF_PARQUET.printSchema()
print(f"DF_PARQUET.count(): {DF_PARQUET.count()}")
