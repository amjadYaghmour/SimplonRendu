#!/usr/bin/env python3
"""
Script Python pour charger les données CSV dans PostgreSQL
Alternative à la commande COPY SQL
"""

import pandas as pd
import psycopg2
from psycopg2 import sql
import os
from datetime import datetime

# Configuration de la connexion
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', 'blablabla')
}

CSV_FILE = 'data/fact_resultats_epreuves.csv'

def connect_db():
    """Connexion à PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("✓ Connexion à PostgreSQL réussie")
        return conn
    except Exception as e:
        print(f"✗ Erreur de connexion: {e}")
        return None

def load_csv_data(conn, csv_path, batch_size=1000):
    """Charge les données CSV dans PostgreSQL par batch"""
    
    print(f"\n📂 Lecture du fichier CSV: {csv_path}")
    
    # Lecture du CSV avec pandas - tous les champs comme string d'abord
    df = pd.read_csv(csv_path, na_values=['NULL', 'null', ''], low_memory=False)
    
    print(f"✓ {len(df)} lignes lues")
    print(f"✓ {len(df.columns)} colonnes")
    
    # Conversion des colonnes numériques avec gestion des NaN
    numeric_cols = ['id_resultat', 'id_resultat_source', 'id_athlete_base_resultats', 
                    'id_personne', 'id_equipe', 'id_pays', 'classement_epreuve',
                    'performance_finale', 'id_evenement', 'id_edition', 'id_competition_sport',
                    'id_type_competition', 'id_ville_edition', 'id_nation_edition_base_resultats',
                    'id_sport', 'id_discipline_administrative', 'id_specialite', 'id_epreuve',
                    'est_epreuve_individuelle', 'est_epreuve_olympique', 'est_epreuve_ete',
                    'est_epreuve_handi', 'epreuve_sens_resultat', 'id_federation']
    
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Remplacement des NaN par None pour PostgreSQL
    df = df.where(pd.notnull(df), None)
    
    cursor = conn.cursor()
    
    # Requête d'insertion
    insert_query = """
    INSERT INTO resultats_epreuves (
        id_resultat, id_resultat_source, source, id_athlete_base_resultats, 
        id_personne, athlete_nom, athlete_prenom, id_equipe, equipe_en, 
        id_pays, pays_en_base_resultats, classement_epreuve, 
        performance_finale_texte, performance_finale, id_evenement, 
        evenement, evenement_en, categorie_age, id_edition, 
        id_competition_sport, competition_en, id_type_competition, 
        type_competition, edition_saison, date_debut_edition, 
        date_fin_edition, id_ville_edition, edition_ville_en, 
        id_nation_edition_base_resultats, edition_nation_en, id_sport, 
        sport, sport_en, id_discipline_administrative, 
        discipline_administrative, id_specialite, specialite, 
        id_epreuve, epreuve, epreuve_genre, epreuve_type, 
        est_epreuve_individuelle, est_epreuve_olympique, est_epreuve_ete, 
        est_epreuve_handi, epreuve_sens_resultat, id_federation, 
        federation, federation_nom_court, dt_creation, dt_modification
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s
    )
    """
    
    # Insertion par batch
    total_inserted = 0
    errors = 0
    
    print(f"\n📥 Insertion des données par batch de {batch_size}...")
    
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size]
        
        try:
            # Conversion en liste de tuples
            data = [tuple(row) for row in batch.values]
            
            # Insertion en masse
            cursor.executemany(insert_query, data)
            conn.commit()
            
            total_inserted += len(batch)
            print(f"  ✓ {total_inserted}/{len(df)} lignes insérées", end='\r')
            
        except Exception as e:
            errors += 1
            print(f"\n  ✗ Erreur batch {i//batch_size + 1}: {e}")
            conn.rollback()
    
    print(f"\n\n✓ Insertion terminée!")
    print(f"  - Lignes insérées: {total_inserted}")
    print(f"  - Erreurs: {errors}")
    
    cursor.close()
    
    return total_inserted

def verify_data(conn):
    """Vérifie les données chargées"""
    cursor = conn.cursor()
    
    print("\n📊 Vérification des données:")
    print("-" * 50)
    
    # Nombre total de lignes
    cursor.execute("SELECT COUNT(*) FROM resultats_epreuves")
    total = cursor.fetchone()[0]
    print(f"  Total de résultats: {total:,}")
    
    # Nombre d'éditions
    cursor.execute("SELECT COUNT(DISTINCT edition_saison) FROM resultats_epreuves WHERE edition_saison ~ '^[0-9]{4}$'")
    editions = cursor.fetchone()[0]
    print(f"  Nombre d'éditions: {editions}")
    
    # Nombre de sports
    cursor.execute("SELECT COUNT(DISTINCT sport_en) FROM resultats_epreuves WHERE sport_en IS NOT NULL")
    sports = cursor.fetchone()[0]
    print(f"  Nombre de sports: {sports}")
    
    # Nombre de pays
    cursor.execute("SELECT COUNT(DISTINCT pays_en_base_resultats) FROM resultats_epreuves WHERE pays_en_base_resultats IS NOT NULL")
    pays = cursor.fetchone()[0]
    print(f"  Nombre de pays: {pays}")
    
    # Top 5 pays avec le plus de médailles d'or
    cursor.execute("""
        SELECT pays_en_base_resultats, COUNT(*) as nb_or
        FROM resultats_epreuves
        WHERE classement_epreuve = 1 AND pays_en_base_resultats IS NOT NULL
        GROUP BY pays_en_base_resultats
        ORDER BY nb_or DESC
        LIMIT 5
    """)
    
    print("\n  Top 5 pays (médailles d'or):")
    for pays, nb in cursor.fetchall():
        print(f"    {pays}: {nb} 🥇")
    
    cursor.close()

def main():
    """Fonction principale"""
    print("=" * 50)
    print("🏅 CHARGEMENT DES DONNÉES OLYMPIQUES")
    print("=" * 50)
    
    # Connexion
    conn = connect_db()
    if not conn:
        return
    
    # Chargement des données
    try:
        load_csv_data(conn, CSV_FILE)
        verify_data(conn)
    except Exception as e:
        print(f"\n✗ Erreur: {e}")
    finally:
        conn.close()
        print("\n✓ Connexion fermée")

if __name__ == "__main__":
    main()

