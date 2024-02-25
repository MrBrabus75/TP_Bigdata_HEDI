1. Démarrer les services nécessaires

Avant d'exécuter ces scripts, assurez-vous que les services suivants sont opérationnels :

    Apache Kafka : Pour la queue de messages. Assurez-vous que le broker Kafka et le serveur Zookeeper sont en cours d'exécution.
    MinIO : Pour le stockage des fichiers Parquet. Assurez-vous que le serveur MinIO est en cours d'exécution et configuré.

2. Exécuter capteur.py pour produire des messages

Ce script génère des transactions et les envoie à un topic Kafka nommé "transaction". Il simule un producteur de données en temps réel.

    Utilisation : Exécutez ce script dans votre environnement Python. Il enverra 200 messages (transactions) au topic Kafka. Assurez-vous que l'adresse du serveur Kafka dans le script correspond à votre configuration Kafka.

3. Exécuter consumer.py pour consommer les messages (Optionnel)

Bien que ce script ne soit pas directement lié à l'objectif de traitement des données avec Spark, il sert à vérifier que les messages sont correctement produits par capteur.py et consommés depuis Kafka.

    Utilisation : Exécutez ce script après capteur.py pour voir les messages consommés à partir du topic Kafka. Cela vous permet de vérifier que la production de messages fonctionne comme prévu.

4. Exécuter readstream.py pour traiter les messages avec Spark Streaming

Ce script lit les messages du topic Kafka, effectue les transformations nécessaires sur les données (conversion de devise, ajout de TimeZone, etc.), et écrit le résultat dans un fichier Parquet.

    Utilisation : Lancez ce script avec spark-submit après avoir démarré capteur.py. Assurez-vous que les chemins de sortie pour les fichiers Parquet et les emplacements de checkpoint sont correctement configurés et accessibles.

5. Exécuter main.py pour écrire les données transformées dans MinIO

Après le traitement par Spark Streaming, main.py lit les fichiers Parquet générés et les écrit dans un bucket MinIO.

    Utilisation : Lancez ce script avec spark-submit. Il lira le fichier Parquet produit par readstream.py et écrira les données dans MinIO. Assurez-vous que les configurations de MinIO (clefs d'accès, secret, endpoint) correspondent à votre environnement MinIO.

6. Exécuter readparquet.py pour lire les données depuis MinIO (Optionnel)

Ce script est utile pour vérifier que les données ont été correctement écrites dans MinIO et pour examiner leur contenu.

    Utilisation : Exécutez ce script après main.py pour lire les données stockées dans MinIO. Il affiche le contenu des fichiers Parquet stockés. Assurez-vous que le chemin vers le fichier Parquet est correct.

Résumé de l'ordre d'exécution :

    capteur.py : Produit des messages dans Kafka.
    consumer.py (Optionnel) : Vérifie la consommation des messages Kafka.
    readstream.py : Lit les messages Kafka, les traite, et les écrit au format Parquet.
    main.py : Lit les fichiers Parquet et les écrit dans MinIO.
    readparquet.py (Optionnel) : Lit les fichiers Parquet depuis MinIO pour vérification.
