import pandas as pd
import numpy as np
import atexit
import streamlit as st
from db_service_sqlalchemy import create_connection, close_connection
from menu_accueil import menu_accueil
from menu_localisation import menu_localisation
from menu_repartition import menu_repartition_delit
from menu_repartition import menu_repartition_LSOAcode
from menu_repartition import menu_repartition_Lastoutcomecategory
from menu_repartition import menu_repartition_Reportedby
from menu_filtre_avance import menu_filtre_avance



# Connexion à la base de données
db_name="crime"
conn, engine = create_connection("127.0.0.1", "root", "", db_name)

# Enregistrer la fonction de nettoyage pour qu'elle soit appelée à la sortie de Streamlit
atexit.register(lambda:close_connection(conn,engine))

# Menu de navigation
menu = st.sidebar.radio(
    "Menu",
    [
        "Accueil",
        "Localisation par type de délit",
        "Répartition par type de délit",
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

elif menu == "Répartition par type de délit":
    menu_repartition_delit(conn)

elif menu == "Répartition des délits par code LSOA":
    menu_repartition_LSOAcode(conn)

elif menu == "Répartition des délits par catégorie de résultat":
    menu_repartition_Lastoutcomecategory(conn)

elif menu == "Répartition des délits par poste de police":
    menu_repartition_Reportedby(conn)

elif menu == "Filtre avancé":
    menu_filtre_avance(conn)
