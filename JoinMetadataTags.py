from pyspark.sql import SparkSession

# Initialiser Spark
spark = SparkSession.builder.appName("JoinMetadataTags").getOrCreate()

# Charger les fichiers
metadata_df = spark.read.json("../raw/metadata.json").select("item_id", "title")
tags_df = spark.read.json("../raw/tags.json").select("id", "tag")
reviews_parquet_df = spark.read.parquet("Tags_according_to_reviews.parquet")

metadata_df.show(5)
tags_df.show(5)
reviews_parquet_df.show(5)

# Joindre les reviews avec les tags (via id = id du tag)
reviews_tags_df = reviews_parquet_df.join(tags_df, reviews_parquet_df.id == tags_df.id).select(reviews_parquet_df.id.alias("film_id"), tags_df.tag)

# Après la première jointure
reviews_tags_df.show(5)

# Joindre avec metadata (via film_id = item_id du film)
final_df = reviews_tags_df.join(metadata_df, reviews_tags_df.film_id == metadata_df.item_id).select(metadata_df.title, reviews_tags_df.tag)

final_df.show(5)

# Sauvegarde en Parquet
final_df.write.parquet("Movies_with_Tags.parquet", mode="overwrite")

print("Fichier Movies_with_Tags.parquet créé avec succès !")
