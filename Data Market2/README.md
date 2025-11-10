# Data Market 2 - Entrepôt de Données en Schéma Étoile

**Étudiant :** Angelo Yaghmour  
**Projet :** Implémentation d'un Entrepôt de Données en Schéma Étoile  
**Base de Données :** PostgreSQL

---

## 📋 Aperçu du Projet

Ce projet implémente un **entrepôt de données en schéma étoile** pour analyser les leads marketing et les ventes conclues. L'entrepôt se compose de :

- **1 Table de Faits :** `fact_closed_deals` (table centrale de transactions)
- **5 Tables de Dimensions :** `dim_lead`, `dim_seller`, `dim_sdr`, `dim_sr`, `dim_date`

## 📂 Structure du Projet

```
Data Market2/
├── data/
│   └── clean/
│       ├── leads_clean.csv              # Source : données des leads
│       ├── closed_deals_clean.csv       # Source : données des ventes
│       ├── dim_lead.csv                 # Dimension générée
│       ├── dim_seller.csv               # Dimension générée
│       ├── dim_sdr.csv                  # Dimension générée
│       ├── dim_sr.csv                   # Dimension générée
│       └── dim_date.csv                 # Dimension générée
├── scripts/
│   ├── generate_dimensions.py           # Génère tous les CSV de dimensions
│   ├── create_tables.sql                # DDL SQL pour le schéma étoile
│   └── load_to_db.py                    # Script ETL pour charger les données
├── models/
│   ├── ERD.txt                          # Diagramme Entité-Relations
│   └── Star_Schema_Diagram.png          # Diagramme visuel du schéma étoile
├── E3_Schema_Technique_Angelo_Yaghmour.docx
├── E4_Rapport_Technique_Angelo_Yaghmour.docx
└── README.md
```

## 🚀 Démarrage Rapide

### Prérequis

- PostgreSQL 12+
- Python 3.8+
- pip

### Étapes d'Installation

**1. Installer les dépendances Python :**

```bash
pip install pandas psycopg2-binary
```

**2. Créer la base de données PostgreSQL :**

```bash
# Se connecter à PostgreSQL
psql -U postgres

# Créer la base de données
CREATE DATABASE datamarket2;
\q
```

**3. Définir les variables d'environnement (optionnel) :**

```bash
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=datamarket2
export DB_USER=postgres
export DB_PASSWORD=votre_mot_de_passe
```

**4. Générer les fichiers CSV de dimensions :**

```bash
python scripts/generate_dimensions.py
```

Ceci crée :
- `dim_lead.csv`
- `dim_seller.csv`
- `dim_sdr.csv`
- `dim_sr.csv`
- `dim_date.csv`

**5. Créer le schéma de base de données :**

```bash
psql -d datamarket2 -f scripts/create_tables.sql
```

**6. Charger les données dans PostgreSQL :**

```bash
python scripts/load_to_db.py
```

### Vérifier l'Installation

```bash
psql -d datamarket2
```

```sql
-- Vérifier le nombre de lignes
SELECT 'dim_lead' as table_name, COUNT(*) FROM dim_lead
UNION ALL
SELECT 'dim_seller', COUNT(*) FROM dim_seller
UNION ALL
SELECT 'dim_sdr', COUNT(*) FROM dim_sdr
UNION ALL
SELECT 'dim_sr', COUNT(*) FROM dim_sr
UNION ALL
SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL
SELECT 'fact_closed_deals', COUNT(*) FROM fact_closed_deals;
```

## 📊 Structure du Schéma Étoile

```
           ┌───────────┐
           │ dim_date  │
           └─────┬─────┘
                 │
   ┌──────────┐ │ ┌───────────┐
   │ dim_lead ├─┼─┤ dim_seller│
   └─────┬────┘ │ └─────┬─────┘
         │      │       │
         │  ┌───▼───────▼───┐
         └──┤ fact_closed   │
            │    _deals     │
         ┌──┤               ├──┐
         │  └───────────────┘  │
         │                     │
    ┌────▼────┐          ┌────▼────┐
    │ dim_sdr │          │ dim_sr  │
    └─────────┘          └─────────┘
```

## 📈 Exemples de Requêtes Analytiques

### 1. Ventes Conclues par Segment d'Activité

```sql
SELECT 
    s.business_segment,
    COUNT(*) as total_ventes,
    ROUND(AVG(f.declared_monthly_revenue), 2) as revenu_moyen
FROM fact_closed_deals f
JOIN dim_seller s ON f.seller_id = s.seller_id
GROUP BY s.business_segment
ORDER BY total_ventes DESC;
```

### 2. Taux de Conversion par Origine du Lead

```sql
SELECT 
    l.origin,
    COUNT(DISTINCT l.mql_id) as total_leads,
    COUNT(DISTINCT f.mql_id) as ventes_conclues,
    ROUND(100.0 * COUNT(DISTINCT f.mql_id) / COUNT(DISTINCT l.mql_id), 2) as taux_conversion
FROM dim_lead l
LEFT JOIN fact_closed_deals f ON l.mql_id = f.mql_id
GROUP BY l.origin
ORDER BY taux_conversion DESC;
```

### 3. Performance Commerciale par Équipe

```sql
SELECT 
    sr.sr_team,
    sr.sr_experience,
    COUNT(*) as ventes_conclues,
    SUM(f.declared_monthly_revenue) as revenu_total
FROM fact_closed_deals f
JOIN dim_sr sr ON f.sr_id = sr.sr_id
GROUP BY sr.sr_team, sr.sr_experience
ORDER BY revenu_total DESC;
```

### 4. Analyse des Tendances Mensuelles

```sql
SELECT 
    d.year,
    d.month,
    d.month_name,
    COUNT(*) as nombre_ventes,
    SUM(f.declared_monthly_revenue) as revenu
FROM fact_closed_deals f
JOIN dim_date d ON f.won_date_id = d.date_id
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;
```

## 🔧 Maintenance

### Actualiser les Données

Pour mettre à jour l'entrepôt avec de nouvelles données :

```bash
# 1. Mettre à jour les CSV sources (leads_clean.csv, closed_deals_clean.csv)
# 2. Régénérer les dimensions
python scripts/generate_dimensions.py

# 3. Vider et recharger (ou utiliser la logique UPSERT)
psql -d datamarket2 -c "TRUNCATE fact_closed_deals CASCADE;"
python scripts/load_to_db.py
```

### Sauvegarder la Base de Données

```bash
pg_dump -d datamarket2 > backup_$(date +%Y%m%d).sql
```

## 📚 Documentation

- **E3_Schema_Technique_Angelo_Yaghmour.docx :** Conception du schéma technique, ERD, dictionnaire de données
- **E4_Rapport_Technique_Angelo_Yaghmour.docx :** Implémentation ETL et documentation technique
- **Star_Schema_Diagram.png :** Diagramme visuel du schéma en étoile

## 🏗️ Stack Technique

| Composant | Technologie | Objectif |
|-----------|-------------|----------|
| Base de Données | PostgreSQL | Stockage des données |
| Langage ETL | Python 3 | Traitement des données |
| Bibliothèque Data | Pandas | Manipulation des CSV |
| Connecteur DB | psycopg2 | Connectivité PostgreSQL |

## ✅ Caractéristiques Clés

- ⭐ **Schéma en étoile** optimisé pour l'analyse
- 🔗 **Intégrité référentielle** via clés étrangères
- 📊 **Dimension date** pour l'analyse temporelle
- 🎲 **Attributs synthétiques** pour enrichir les dimensions
- 🔄 **Pipeline ETL reproductible**
- 📝 **Documentation complète**
