from pyspark.sql import SparkSession

# Chemin vers les JARs téléchargés
path_to_hadoop_aws_jar = "/home/ubuntu/Téléchargements/hadoop-aws-3.3.2.jar"
path_to_aws_java_sdk_jar = "/home/ubuntu/Téléchargements/aws-java-sdk-bundle-1.11.1026.jar"

spark = SparkSession.builder \
    .appName("SparkMinIOExample") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.jars", f"{path_to_hadoop_aws_jar},{path_to_aws_java_sdk_jar}") \
    .getOrCreate()

# Assurer que le niveau de log est réglé sur ERROR pour réduire le bruit dans les sorties de log
spark.sparkContext.setLogLevel("ERROR")

# Lire depuis MinIO
bucket_name = "warehouse"
file_path = "elem"  # Assurez-vous que ce chemin est correct et correspond à l'endroit où les fichiers Parquet sont stockés
full_path = f"s3a://{bucket_name}/{file_path}"

df = spark.read.parquet(full_path)

# Afficher les données pour vérification
df.show()
df.printSchema()

# Supposons que toutes les transformations nécessaires ont déjà été effectuées
# Écrire le DataFrame modifié dans un autre emplacement dans le bucket MinIO
output_path = f"s3a://{bucket_name}/processed-data"
df.write.mode("overwrite").parquet(output_path)
