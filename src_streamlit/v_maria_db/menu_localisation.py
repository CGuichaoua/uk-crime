import pandas as pd
import numpy as np
import sqlite3
import streamlit as st
import pydeck as pdk
from utils_carte import afficher_carte


# Fonction pour mapper les types de crimes à des couleurs
def get_color(crimetype, color_map):
    return color_map.get(crimetype, [255, 255, 255])  # Blanc par défaut


def menu_localisation(conn: sqlite3.Connection) -> None:
    if conn:
        try:
            st.title("Localisation par type de délit")
            # Exécuter la requête pour récupérer les types de crimes distincts
            distinct_query = "SELECT DISTINCT Crimetype FROM street;"
            distinct_df = pd.read_sql_query(distinct_query, conn)

            # Créer un dictionnaire pour mapper les types de crimes à des couleurs
            color_map = {
                crimetype: [int(i) for i in np.random.choice(range(256), size=3)]
                for crimetype in distinct_df["Crimetype"]
            }

            # Ajouter des cases à cocher pour chaque type de crime
            st.sidebar.markdown("### Types de crimes à afficher")
            selected_crimetypes = []
            for crimetype in color_map.keys():
                if st.sidebar.checkbox(crimetype, value=True):
                    selected_crimetypes.append(crimetype)

            # exécuter la requête si bouton "Afficher la carte" est cliqué
            if st.button("Afficher la carte"):
                # Exécuter la requête pour récupérer les données géolocalisées
                query = """
                SELECT Latitude, Longitude, Crimetype
                FROM street
                WHERE Latitude IS NOT NULL AND Longitude IS NOT NULL
                LIMIT 500000;
                """
                df = pd.read_sql_query(query, conn)

                # Filtrer les données en fonction des types de crimes sélectionnés
                df = df[df["Crimetype"].isin(selected_crimetypes)]

                # Fermer la connexion
                conn.close()

                # Ajouter une colonne de couleur au DataFrame
                df["Color"] = df["Crimetype"].apply(lambda x: get_color(x, color_map))

                titre_carte = "Localisation par type de délit"

                afficher_carte(
                    titre_carte,
                    df,
                    df["Latitude"].mean(),
                    df["Longitude"].mean(),
                    color_map,
                )

        except sqlite3.Error as e:
            st.error(f"Erreur lors de l'exécution de la requête SQL : {e}")

    else:
        st.error("Impossible de se connecter à la base de données.")
