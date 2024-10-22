import pandas as pd
import numpy as np
import sqlite3
import streamlit as st
import plotly.express as px


def menu_repartition_delit(conn: sqlite3.Connection) -> None:
    if conn:
        try:
            st.title("Répartition par type de délit")
            # Afficher la répartition des crimes par type
            query1 = """
            SELECT Crimetype, COUNT(*) AS count
            FROM street_temp
            GROUP BY Crimetype
            ORDER BY count DESC;
            """
            df1 = pd.read_sql_query(query1, conn)
            conn.close()

            # Vérifier les données triées
            df1_sorted = df1.sort_values(by="count", ascending=False)
            st.write("Répartition par type de délit :", df1_sorted)

            # Utiliser Plotly pour créer un bar chart
            fig = px.bar(
                df1_sorted,
                x="Crimetype",
                y="count",
                title="Répartition par type de délit",
            )

            # Afficher le graphique Plotly dans Streamlit
            st.plotly_chart(fig, use_container_width=True)
        except sqlite3.Error as e:
            st.error(f"Erreur lors de l'exécution de la requête SQL : {e}")

    else:
        st.error("Impossible de se connecter à la base de données.")


def menu_repartition_LSOAcode(conn: sqlite3.Connection) -> None:
    if conn:
        try:
            st.title("Répartition par code LSOA")
            # Afficher la répartition des crimes par type
            query1 = """
            SELECT LSOAcode, COUNT(*) AS count
            FROM street_temp
            GROUP BY LSOAcode
            ORDER BY count DESC;
            """
            df1 = pd.read_sql_query(query1, conn)
            conn.close()

            # Vérifier les données triées
            df1_sorted = df1.sort_values(by="count", ascending=False)
            st.write("Répartition par code LSOA :", df1_sorted)

            # Utiliser Plotly pour créer un bar chart
            fig = px.bar(
                df1_sorted,
                x="LSOAcode",
                y="count",
                title="Répartition par code LSOA",
            )

            # Afficher le graphique Plotly dans Streamlit
            st.plotly_chart(fig, use_container_width=True)
        except sqlite3.Error as e:
            st.error(f"Erreur lors de l'exécution de la requête SQL : {e}")

    else:
        st.error("Impossible de se connecter à la base de données.")


def menu_repartition_Lastoutcomecategory(conn: sqlite3.Connection) -> None:
    if conn:
        try:
            st.title("Répartition par catégorie de résultat")
            # Afficher la répartition des crimes par type
            query1 = """
            SELECT Lastoutcomecategory, COUNT(*) AS count
            FROM street_temp
            GROUP BY Lastoutcomecategory
            ORDER BY count DESC;
            """
            df1 = pd.read_sql_query(query1, conn)
            conn.close()

            # Vérifier les données triées
            df1_sorted = df1.sort_values(by="count", ascending=False)
            st.write("Répartition par catégorie de résultat :", df1_sorted)

            # Utiliser Plotly pour créer un bar chart
            fig = px.bar(
                df1_sorted,
                x="Lastoutcomecategory",
                y="count",
                title="Répartition par catégorie de résultat",
            )

            # Afficher le graphique Plotly dans Streamlit
            st.plotly_chart(fig, use_container_width=True)
        except sqlite3.Error as e:
            st.error(f"Erreur lors de l'exécution de la requête SQL : {e}")

    else:
        st.error("Impossible de se connecter à la base de données.")


def menu_repartition_Reportedby(conn: sqlite3.Connection) -> None:
    if conn:
        try:
            st.title("Répartition par poste de police")
            # Afficher la répartition des crimes par type
            query1 = """
            SELECT Reportedby, COUNT(*) AS count
            FROM street_temp
            GROUP BY Reportedby
            ORDER BY count DESC;
            """
            df1 = pd.read_sql_query(query1, conn)
            conn.close()

            # Vérifier les données triées
            df1_sorted = df1.sort_values(by="count", ascending=False)
            st.write("Répartition par poste de police :", df1_sorted)

            # Utiliser Plotly pour créer un bar chart
            fig = px.bar(
                df1_sorted,
                x="Reportedby",
                y="count",
                title="Répartition par poste de police",
            )

            # Afficher le graphique Plotly dans Streamlit
            st.plotly_chart(fig, use_container_width=True)
        except sqlite3.Error as e:
            st.error(f"Erreur lors de l'exécution de la requête SQL : {e}")

    else:
        st.error("Impossible de se connecter à la base de données.")
