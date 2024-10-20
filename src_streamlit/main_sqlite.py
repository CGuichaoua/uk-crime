import pandas as pd
import numpy as np
import sqlite3
import streamlit as st
from db_service import create_connection
from menu_accueil import menu_accueil
from menu_localisation import menu_localisation
from menu_repartition import menu_repartition_delit
from menu_repartition import menu_repartition_LSOAcode
from menu_repartition import menu_repartition_Lastoutcomecategory
from menu_repartition import menu_repartition_Reportedby
from menu_filtre_avance import menu_filtre_avance


# Nom de la base de données SQLite
db_name = "../src_batch/crime.db"  # Fichier SQLite

# Connexion à SQLite
conn = create_connection(db_name)

# Menu de navigation
menu = st.sidebar.radio(
    "Menu",
    [
        "Accueil",
        "Localisation par type de délit",
        "Répartition des délits par catégorie",
        "Répartition des délits par code LSOA",
        "Répartition des délits par catégorie de résultat",
        "Répartition des délits par poste de police",
        "Filtre avancé",
    ],
)

if menu == "Accueil":
    menu_accueil()

elif menu == "Localisation par type de délit":
    menu_localisation(conn)

elif menu == "Répartition des délits par catégorie":
    menu_repartition_delit(conn)

elif menu == "Répartition des délits par code LSOA":
    menu_repartition_LSOAcode(conn)

elif menu == "Répartition des délits par catégorie de résultat":
    menu_repartition_Lastoutcomecategory(conn)

elif menu == "Répartition des délits par poste de police":
    menu_repartition_Reportedby(conn)

elif menu == "Filtre avancé":
    menu_filtre_avance(conn)
