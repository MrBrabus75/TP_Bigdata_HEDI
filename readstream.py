from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, to_timestamp, when  # Assurez-vous d'importer 'when' ici
from pyspark.sql.types import StructType, StringType, FloatType, TimestampType, IntegerType

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, to_timestamp
from pyspark.sql.types import StructType, StringType, FloatType, TimestampType, IntegerType

spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

schema = StructType().add("id_transaction", StringType()) \
                     .add("type_transaction", StringType()) \
                     .add("montant", FloatType()) \
                     .add("devise", StringType()) \
                     .add("date", StringType()) \
                     .add("lieu", StringType()) \
                     .add("moyen_paiement", StringType()) \
                     .add("details", StructType().add("produit", StringType()) \
                                                .add("quantite", IntegerType()) \
                                                .add("prix_unitaire", FloatType())) \
                     .add("utilisateur", StructType().add("id_utilisateur", StringType()) \
                                                     .add("nom", StringType()) \
                                                     .add("adresse", StringType()) \
                                                     .add("email", StringType()))

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "transaction") \
    .load()

df = kafka_df.selectExpr("CAST(value AS STRING)").select(from_json("value", schema).alias("data")).select("data.*")

# Conversion USD en EUR (taux fixe pour l'exemple, ajustez selon le besoin)
taux_conversion = 0.85  # Exemple de taux de conversion
df = df.withColumn("montant", when(col("devise") == "USD", col("montant") * taux_conversion).otherwise(col("montant"))) \
       .withColumn("devise", when(col("devise") == "USD", "EUR").otherwise(col("devise")))

# Ajout du TimeZone et conversion de la date en Timestamp
df = df.withColumn("date", to_timestamp("date", "yyyy-MM-dd'T'HH:mm:ss").cast("timestamp"))

# Suppression des transactions en erreur et des valeurs None pour adresse
df = df.filter((col("moyen_paiement") != "erreur") & (col("utilisateur.adresse").isNotNull()))

# Écriture du DataFrame au format Parquet
query = df.writeStream \
          .outputMode("append") \
          .format("parquet") \
          .option("checkpointLocation", "path/to/checkpoint/dir") \
          .option("path", "path/to/output/dir") \
          .start()

query.awaitTermination()
