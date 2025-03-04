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


# Schéma déjà défini
schema = StructType([
    StructField("item_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("rating", FloatType(), True)
])

# Le chemin exact vers le fichier JSON
fichier_json_1 = "genome_2021/movie_dataset_public_final/raw/ratings.json"
assert Path(fichier_json_1).exists(), f"{fichier_json_1} does not exist"
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


# Emplacement de sortie pour les fichiers Parquet
OUTPUT_PATH = Path("tmp/data/parquet")
assert OUTPUT_PATH.parent.exists(), f"{OUTPUT_PATH.parent} does not exist"

write_to_parquet(df=DF_AVG_RATING, output_path=str(OUTPUT_PATH))
assert OUTPUT_PATH.exists(), f"Output path {OUTPUT_PATH} does not exist"


def read_parquet(input_path: str) -> DataFrame:
    """
    Read parquet file
    """
    return spark.read.parquet(input_path)


# Lire les fichiers Parquet générés
DF_PARQUET = read_parquet(input_path=str(OUTPUT_PATH))

DF_PARQUET.printSchema()
print(f"DF_PARQUET.count(): {DF_PARQUET.count()}")
