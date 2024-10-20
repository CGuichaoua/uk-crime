import streamlit as st
import pydeck as pdk
import pandas as pd
import mysql.connector
from mysql.connector import Error


# Fonction de connexion à MariaDB
def create_connection(host_name, user_name, user_password, db_name=None, port=3306):
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            password=user_password,
            database=db_name,
            port=port,
            connection_timeout=600,  # 10 minutes de délai
        )
        print(
            f"Connexion réussie à la base de données {db_name}"
            if db_name
            else "Connexion réussie au serveur MariaDB"
        )
        return connection
    except Error as e:
        print(f"Erreur : '{e}'")
        return None


# Connexion à MariaDB
conn = create_connection("localhost", "username", "password", "crime")

# Vérifier si la connexion a réussi
if conn:
    try:
        # Exécuter la requête pour récupérer les données géolocalisées
        query = """
        SELECT Latitude, Longitude, Crimetype
        FROM street_temp
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL;
        """
        df = pd.read_sql(query, conn)

        # Fermer la connexion
        conn.close()

        # Configuration de la vue de la carte
        view_state = pdk.ViewState(
            latitude=df["latitude"].mean(),  # Position initiale de la carte
            longitude=df["longitude"].mean(),
            zoom=10,
            pitch=50,
        )

        # Création de la couche de visualisation des points
        layer = pdk.Layer(
            "ScatterplotLayer",
            df,
            get_position="[longitude, latitude]",
            get_radius=200,
            get_color=[255, 0, 0],  # Couleur rouge pour les points
            pickable=True,
        )

        # Affichage de la carte dans Streamlit
        st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view_state))

    except Error as e:
        st.error(f"Erreur lors de l'exécution de la requête SQL : {e}")

else:
    st.error("Impossible de se connecter à la base de données.")
