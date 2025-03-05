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
        .master("spark://129.104.73.59:7077").getOrCreate()


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


# Schéma déjà défini
schema = StructType([
    StructField("item_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("rating", FloatType(), True)
])

# Le chemin exact vers le fichier JSON sur HDFS
fichier_json_1 = "hdfs://localhost:9000/user/blandine/ratings.json"
DATA_FILE_PATHS = [fichier_json_1]

# Lire les données en utilisant le schéma défini
DF_RAW = read_json_with_schema(data_file_paths=DATA_FILE_PATHS, schema=schema)

# Vérification
assert DF_RAW is not None, "Returned result is None"
assert isinstance(DF_RAW, DataFrame), "Returned result is not a valid DataFrame"

# Affichage du nombre de lignes
print(f"DF_RAW.count(): {DF_RAW.count()}")


def compute_average_rating(df: DataFrame) -> DataFrame:
    """
    Compute the average rating for each item_id
    """
    # Group by item_id and compute the average rating
    df_avg = df.groupBy("item_id").avg("rating").withColumnRenamed("avg(rating)", "avg_rating")
    
    return df_avg


# Calcul de la moyenne des évaluations
DF_AVG_RATING = compute_average_rating(DF_RAW)

# Affichage des 10 premières lignes
DF_AVG_RATING.show(10)


def write_to_parquet(df: DataFrame, output_path: str):
    """
    Write DataFrame to Parquet format
    """
    df.write.mode("overwrite").parquet(output_path)


# Emplacement de sortie pour les fichiers Parquet sur HDFS
OUTPUT_PATH = "hdfs://localhost:9000/user/blandine/output/ratings_parquet"

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
