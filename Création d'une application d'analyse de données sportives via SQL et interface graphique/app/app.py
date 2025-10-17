import streamlit as st
import psycopg2
import pandas as pd

# Configuration de la page
st.set_page_config(
    page_title="Analyse Données Olympiques",
    page_icon="🏅",
    layout="wide"
)

# Configuration de la connexion
import os
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', 'blablabla')
}

@st.cache_resource
def get_connection():
    """Connexion à PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        st.error(f"❌ Erreur de connexion: {e}")
        return None

def execute_query(query):
    """Exécute une requête SQL et retourne les résultats"""
    conn = get_connection()
    if conn is None:
        return None
    
    try:
        df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        st.error(f"❌ Erreur SQL: {e}")
        return None

# Titre de l'application
st.title("🏅 Analyse des Données Olympiques")
st.markdown("---")

# Sidebar - Statistiques rapides
with st.sidebar:
    st.header("📊 Statistiques")
    
    conn = get_connection()
    if conn:
        # Stats générales
        stats_query = """
        SELECT 
            COUNT(*) as total_resultats,
            COUNT(DISTINCT edition_saison) as editions,
            COUNT(DISTINCT sport_en) as sports,
            COUNT(DISTINCT pays_en_base_resultats) as pays
        FROM resultats_epreuves;
        """
        stats = execute_query(stats_query)
        
        if stats is not None:
            st.metric("Total Résultats", f"{stats['total_resultats'][0]:,}")
            st.metric("Éditions", stats['editions'][0])
            st.metric("Sports", stats['sports'][0])
            st.metric("Pays", stats['pays'][0])
    
    st.markdown("---")
    st.markdown("### 📝 Exemples de requêtes")
    
    if st.button("Top 10 Pays (Or)"):
        st.session_state['query'] = """SELECT 
    pays_en_base_resultats AS pays,
    COUNT(*) AS medailles_or
FROM resultats_epreuves
WHERE classement_epreuve = 1
    AND pays_en_base_resultats IS NOT NULL
GROUP BY pays_en_base_resultats
ORDER BY medailles_or DESC
LIMIT 10;"""
    
    if st.button("Médailles par Sport"):
        st.session_state['query'] = """SELECT 
    sport_en,
    COUNT(CASE WHEN classement_epreuve = 1 THEN 1 END) AS or,
    COUNT(CASE WHEN classement_epreuve = 2 THEN 1 END) AS argent,
    COUNT(CASE WHEN classement_epreuve = 3 THEN 1 END) AS bronze
FROM resultats_epreuves
WHERE classement_epreuve <= 3
    AND sport_en IS NOT NULL
GROUP BY sport_en
ORDER BY or DESC
LIMIT 10;"""
    
    if st.button("JO par Année"):
        st.session_state['query'] = """SELECT 
    edition_saison,
    type_competition,
    COUNT(DISTINCT id_epreuve) AS epreuves,
    COUNT(DISTINCT pays_en_base_resultats) AS pays
FROM resultats_epreuves
WHERE edition_saison ~ '^[0-9]{4}$'
GROUP BY edition_saison, type_competition
ORDER BY edition_saison DESC;"""

# Zone principale - Éditeur SQL
st.header("💻 Éditeur SQL")

# Initialiser la query dans session_state si elle n'existe pas
if 'query' not in st.session_state:
    st.session_state['query'] = "SELECT * FROM resultats_epreuves LIMIT 10;"

# Zone de texte pour la requête
query = st.text_area(
    "Écrivez votre requête SQL :",
    value=st.session_state['query'],
    height=150,
    key='sql_input'
)

# Bouton d'exécution
col1, col2 = st.columns([1, 5])
with col1:
    execute_btn = st.button("▶️ Exécuter", type="primary")

# Exécution de la requête
if execute_btn and query:
    with st.spinner("Exécution de la requête..."):
        df = execute_query(query)
        
        if df is not None:
            st.success(f"✅ Requête exécutée avec succès ! ({len(df)} lignes)")
            
            # Affichage des résultats
            st.dataframe(df, use_container_width=True, height=400)
            
            # Options d'export
            st.download_button(
                label="📥 Télécharger CSV",
                data=df.to_csv(index=False).encode('utf-8'),
                file_name='resultats.csv',
                mime='text/csv'
            )

# Footer
st.markdown("---")
st.markdown("**Base de données:** `postgres` | **Table:** `resultats_epreuves` | **Lignes:** 35,690")

