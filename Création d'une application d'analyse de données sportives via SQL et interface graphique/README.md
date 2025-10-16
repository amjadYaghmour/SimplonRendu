# 🏅 Application Analyse Données Olympiques

Application web pour interroger des données olympiques avec SQL.

## 🚀 Démarrage

```bash
docker-compose up
```

Accéder à l'application : **http://localhost:8517**

## 📋 Ce que fait l'application

- Interface web pour écrire des requêtes SQL
- Base de données PostgreSQL avec 35,690 résultats olympiques
- Pipeline Airflow pour simuler l'ingestion de données

## 📂 Fichiers importants

```
├── data/fact_resultats_epreuves.csv    # Données CSV
├── sql/init.sql                        # Création de la table
├── app/app.py                          # Interface Streamlit
├── airflow/dags/dag.py                 # Pipeline Airflow
└── docker-compose.yml                  # Configuration Docker
```

## 🛠️ Technologies

- PostgreSQL : stockage des données
- Streamlit : interface web
- Airflow : pipeline automatique
- Docker : déploiement

## 📊 Données disponibles

- 35,690 résultats
- 5 éditions (2016-2024)
- 66 sports
- 211 pays

## 🔧 Commandes utiles

```bash
# Démarrer
docker-compose up

# Arrêter  
docker-compose down

# Logs
docker-compose logs -f
```

