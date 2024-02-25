from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("LectureParquet") \
    .getOrCreate()

# Assurez-vous que le chemin vers le fichier Parquet est correct
path_to_parquet = "path/to/elem"  # Ajustez selon le chemin réel

df = spark.read.parquet(path_to_parquet)

# Affichage des données pour vérification
df.show(100000, False)

spark.stop()
