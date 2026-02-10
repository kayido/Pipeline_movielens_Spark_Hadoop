# BigDataProject - Pipeline ETL MovieLens

## 📋 Description

**BigDataProject** est un projet de traitement de données massives basé sur un **pipeline ETL** complet. Il traite le dataset MovieLens en utilisant une architecture Big Data moderne avec **Apache Hadoop**, **Apache Spark**, et **Hive**.

Le projet démontre les étapes complètes d un pipeline ETL :
- **Extract** : Extraction des données brutes depuis des fichiers CSV
- **Transform** : Nettoyage, enrichissement et transformation avec PySpark et Hive
- **Load** : Chargement des résultats dans HDFS

## 🏗️ Architecture

```
Données brutes (CSV)
    ↓
┌─────────────────────────────────────────┐
│  Stage 1: Extraction (spark-etl.py)     │
└─────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────┐
│  Stage 2: Transformation (Spark/Hive)   │
│  - Nettoyage des données                │
│  - Jointures et agrégations             │
│  - Calculs de métriques                 │
└─────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────┐
│  Stage 3: Chargement (HDFS)             │
│  - Silver layer (données traitées)      │
│  - Results layer (résultats d analyse)  │
└─────────────────────────────────────────┘
```

## 📁 Structure du Projet

```
BigDataProject/
├── spark-etl.py                 # Script principal de transformation PySpark
├── hive-etl.hql                 # Requêtes Hive pour analyses avancées
├── etl.ipynb                    # Notebook Jupyter pour exploration 
├── full_dataset_v2.csv          # Dataset complet consolidé
├── docker/
│   └── docker-compose.yml       # Configuration du cluster Hadoop
└── ml-latest/                   # Dataset MovieLens original
    ├── movies.csv               # Données des films
    ├── ratings.csv              # Données des évaluations
    ├── tags.csv                 # Tags attribués aux films
    ├── links.csv                # Identifiants externes (IMDb, TMDb)
    ├── genome-scores.csv        # Scores de genome (tags)
    └── genome-tags.csv          # Définitions des tags du genome
```

## 🚀 Getting Started

### Prérequis

- **Docker** et **Docker Compose** pour l infrastructure
- **Python 3.8+** pour PySpark
- **Java 8+** pour Hadoop/Spark
- **Jupyter Notebook** (optionnel) pour l exploration interactive

### Installation

#### 1. Démarrer le cluster Hadoop

```bash
cd docker
docker-compose up -d
```
ou
 Avec Docker Desktop et VsCode Remote , ouvrez le dossier `docker` et lancez le conteneur.

Cela démarre :
- **hadoop-master** : NameNode et ResourceManager
- **hadoop-worker1** et **hadoop-worker2** : DataNodes

Vérifiez l état du cluster sur `http://localhost:9870` (NameNode UI)

#### 2. Charger les données dans HDFS

```bash
# Créer les répertoires dans HDFS
./start-hadoop.sh
hdfs dfs -mkdir -p /user/root/raw

# Copier les fichiers MovieLens dans le conteneur Hadoop
docker  cp ml-latest/movies.csv hadoop-master-project:/tmp/movies.csv.csv
docker  cp ml-latest/ratings.csv hadoop-master-project:/tmp/ratings.csv

# Depuis le conteneur Hadoop, déplacer les fichiers dans HDFS
hdfs dfs -put /tmp/movies.csv /user/root/raw/
hdfs dfs -put /tmp/ratings.csv /user/root/raw/
```

#### 3. Exécuter le pipeline PySpark

```bash
# Copier les scripts dans le conteneur Hadoop
docker cp spark-etl.py hadoop-master-project:/tmp/spark-etl.py
docker cp hive-etl.hql hadoop-master-project:/tmp/hive-etl.hql

# Depuis le conteneur Hadoop, exécuter le script PySpark
spark-submit /tmp/spark-etl.py
hive -f /tmp/hive-etl.hql
```

### Utilisation

#### 🔍 Exploration avec Jupyter

```bash
jupyter notebook etl.ipynb
```

Le notebook contient :
- Chargement et exploration des données
- Visualisations des distributions
- Analyses préliminaires

#### 📊 Requêtes Hive avancées

```bash
docker exec hadoop-master-project hive -f hive-etl.hql
```

Le fichier Hive contient des requêtes pour :
- Top films par genre
- Films les mieux notés ...

## 📊 Dataset MovieLens

Le projet utilise le dataset **MovieLens ml-latest** qui contient :
- **27 878 films**
- **1 108 997 évaluations**
- **65 133 tags**
- **Tags génétiques** (genome scores)

### Fichiers disponibles

| Fichier | Description |
|---------|-------------|
| `movies.csv` | ID, titre, genres |
| `ratings.csv` | ID utilisateur, ID film, note, timestamp |
| `tags.csv` | ID utilisateur, ID film, tag, timestamp |
| `links.csv` | Identifiants IMDb et TMDb |
| `genome-scores.csv` | Scores de pertinence des tags |
| `genome-tags.csv` | Description des tags du genome |

## ⚙️ Configuration

### Chemins HDFS

Modifiez les constantes dans `spark-etl.py` pour personnaliser les chemins :

```python
RAW_BASE = "hdfs:///user/root/raw"         # données brutes
SILVER_OUT = "hdfs:///user/root/silver"    # données transformées
RESULTS_OUT = "hdfs:///user/root/results"  # résultats finaux
```

## 🐳 Services Docker

### Commandes utiles


# Accéder au shell du conteneur
docker exec -it hadoop-master-project bash

# Lister les fichiers HDFS
hdfs dfs -ls /

# Arrêter le cluster
docker-compose down
```

## 📈 Pipeline Détaillé

### Étape 1 : Extraction (Extract)
```python
# Lecture des fichiers CSV depuis HDFS
movies = spark.read.csv("hdfs:///user/root/raw/movies.csv")
ratings = spark.read.csv("hdfs:///user/root/raw/ratings.csv")
```

### Étape 2 : Transformation (Transform)
- Typage des colonnes (strings → nombres)
- Nettoyage des valeurs manquantes
- Jointures entre tables
- Agrégations et calculs de métriques
- Fenêtrage pour analyses temporelles

### Étape 3 : Chargement (Load)
```python
# Écriture des résultats
transformed_data.write.mode("overwrite").csv("hdfs:///user/root/results/")
```

## 🔧 Troubleshooting


**Problème** : Données introuvables dans HDFS
```bash
# Lister et vérifier les fichiers
docker exec hadoop-master-project hdfs dfs -ls /user/root/raw/
```

## 📝 Notes

- Les données complètes sont consolidées dans `full_dataset_v2.csv`


## 📄 Licence

Données MovieLens : [License MovieLens](https://grouplens.org/datasets/movielens/)
