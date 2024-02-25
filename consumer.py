from kafka import KafkaConsumer
import json

try:
    consumer = KafkaConsumer(
        'transaction',
        bootstrap_servers=['127.0.0.1:9092'],
        auto_offset_reset='earliest',  # Commence à lire depuis le début du topic si aucun offset n'est spécifié
        group_id='my-group',  # Spécifiez un group_id pour permettre le suivi des offsets consommés
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    for message in consumer:
        print(f"Reçu un message: {message.value}")

except Exception as e:
    print(f"Erreur lors de la consommation des messages: {e}")
