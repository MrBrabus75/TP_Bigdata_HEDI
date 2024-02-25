from kafka import KafkaProducer
import datetime as dt
import json
import random
import time
import uuid

def generate_transaction():
    transaction_types = ['achat', 'remboursement', 'transfert']
    payment_methods = ['carte_de_credit', 'especes', 'virement_bancaire', 'erreur']
    current_time = dt.datetime.now().isoformat()
    cities = ["Paris", "Marseille", "Lyon", "Toulouse", "Nice", "Nantes", "Strasbourg", "Montpellier", "Bordeaux", "Lille", "Rennes", "Reims", "Le Havre", "Saint-Étienne", "Toulon", None]
    streets = ["Rue de la République", "Rue de Paris", "rue Auguste Delaune", "Rue Gustave Courbet", "Rue de Luxembourg", "Rue Fontaine", "Rue Zinedine Zidane", "Rue de Bretagne", "Rue Marceaux", "Rue Gambetta", "Rue du Faubourg Saint-Antoine", "Rue de la Grande Armée", "Rue de la Villette", "Rue de la Pompe", "Rue Saint-Michel", None]

    transaction_data = {
        "id_transaction": str(uuid.uuid4()),
        "type_transaction": random.choice(transaction_types),
        "montant": round(random.uniform(10.0, 1000.0), 2),
        "devise": "USD",
        "date": current_time,
        "lieu": f"{random.choice(cities)}, {random.choice(streets)}",
        "moyen_paiement": random.choice(payment_methods),
        "details": {
            "produit": f"Produit{random.randint(1, 100)}",
            "quantite": random.randint(1, 10),
            "prix_unitaire": round(random.uniform(5.0, 200.0), 2)
        },
        "utilisateur": {
            "id_utilisateur": f"User{random.randint(1, 1000)}",
            "nom": f"Utilisateur{random.randint(1, 1000)}",
            "adresse": f"{random.randint(1, 1000)} {random.choice(streets)}, {random.choice(cities)}",
            "email": f"utilisateur{random.randint(1, 1000)}@example.com"
        }
    }
    return transaction_data

try:
    producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'],
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    for _ in range(200):  # Ajustez le nombre de messages à générer si nécessaire
        transaction = generate_transaction()
        future = producer.send("transaction", transaction)
        future.get(timeout=10)  # Attendre la confirmation de l'envoi
        time.sleep(1)
finally:
    producer.close()
