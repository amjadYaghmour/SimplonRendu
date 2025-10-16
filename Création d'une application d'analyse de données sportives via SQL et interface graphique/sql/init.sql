
DROP TABLE IF EXISTS resultats_epreuves CASCADE;

-- Create main table
CREATE TABLE resultats_epreuves (
    -- Identifiants
    id_resultat BIGINT PRIMARY KEY,
    id_resultat_source BIGINT,
    source VARCHAR(50),
    
    -- Informations Athlète
    id_athlete_base_resultats BIGINT,
    id_personne BIGINT,
    athlete_nom VARCHAR(100),
    athlete_prenom VARCHAR(100),
    
    -- Informations Équipe
    id_equipe BIGINT,
    equipe_en VARCHAR(100),
    
    -- Informations Pays
    id_pays BIGINT,
    pays_en_base_resultats VARCHAR(100),
    
    -- Résultats
    classement_epreuve INTEGER,
    performance_finale_texte TEXT,
    performance_finale NUMERIC,
    
    -- Événement
    id_evenement BIGINT,
    evenement VARCHAR(100),
    evenement_en VARCHAR(100),
    categorie_age VARCHAR(50),
    
    -- Édition des Jeux
    id_edition BIGINT,
    id_competition_sport BIGINT,
    competition_en VARCHAR(100),
    id_type_competition INTEGER,
    type_competition VARCHAR(50),
    edition_saison VARCHAR(10),
    date_debut_edition DATE,
    date_fin_edition DATE,
    
    -- Lieu
    id_ville_edition BIGINT,
    edition_ville_en VARCHAR(100),
    id_nation_edition_base_resultats BIGINT,
    edition_nation_en VARCHAR(100),
    
    -- Sport
    id_sport BIGINT,
    sport VARCHAR(100),
    sport_en VARCHAR(100),
    
    -- Discipline
    id_discipline_administrative BIGINT,
    discipline_administrative VARCHAR(100),
    
    -- Spécialité
    id_specialite BIGINT,
    specialite VARCHAR(100),
    
    -- Épreuve
    id_epreuve BIGINT,
    epreuve VARCHAR(200),
    epreuve_genre VARCHAR(20),
    epreuve_type VARCHAR(50),
    
    -- Flags booléens
    est_epreuve_individuelle SMALLINT,
    est_epreuve_olympique SMALLINT,
    est_epreuve_ete SMALLINT,
    est_epreuve_handi SMALLINT,
    epreuve_sens_resultat SMALLINT,
    
    -- Fédération
    id_federation BIGINT,
    federation VARCHAR(200),
    federation_nom_court VARCHAR(100),
    
    -- Métadonnées
    dt_creation TIMESTAMP,
    dt_modification TIMESTAMP
);



