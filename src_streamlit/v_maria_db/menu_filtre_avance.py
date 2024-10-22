import pandas as pd
import streamlit as st
import sqlite3

def menu_filtre_avance(conn: sqlite3.Connection) -> None:

    st.title("Filtre avancé")

    # Récupérer les valeurs uniques pour les filtres
    crime_type_list = pd.read_sql("SELECT DISTINCT Crimetype FROM street_temp;", conn)[
        "Crimetype"
    ].tolist()

    last_outcome_list = pd.read_sql(
        "SELECT DISTINCT Lastoutcomecategory FROM street_temp;", conn
    )["Lastoutcomecategory"].tolist()

    reportedby_list = pd.read_sql("SELECT DISTINCT Reportedby FROM street_temp;", conn)[
        "Reportedby"
    ].tolist()

    start_date = pd.read_sql("SELECT MIN(Month) FROM street_temp;", conn).iloc[0][0]
    end_date = pd.read_sql("SELECT MAX(Month) FROM street_temp;", conn).iloc[0][0]

    min_lat, max_lat = pd.read_sql(
        "SELECT MIN(Latitude), MAX(Latitude) FROM street_temp;", conn
    ).iloc[0]

    min_lon, max_lon = pd.read_sql(
        "SELECT MIN(Longitude), MAX(Longitude) FROM street_temp;", conn
    ).iloc[0]

    # Filtres interactifs
    all_crime_types = st.checkbox("Sélectionner tous les types de crime")
    if all_crime_types:
        crime_types = st.multiselect(
            "Sélectionner un ou plusieurs types de crime",
            options=crime_type_list,
            default=crime_type_list,
        )
    else:
        crime_types = st.multiselect(
            "Sélectionner un ou plusieurs types de crime", options=crime_type_list
        )
    crime_types_str = ", ".join(f"'{crime_type}'" for crime_type in crime_types)

    all_last_outcomes = st.checkbox("Sélectionner tous les résultats")
    if all_last_outcomes:
        last_outcome = st.multiselect(
            "Sélectionner un ou plusieurs résultats",
            options=last_outcome_list,
            default=last_outcome_list,
        )
    else:
        last_outcome = st.multiselect(
            "Sélectionner un résultat", options=last_outcome_list
        )
    last_outcome_str = ", ".join(f"'{outcome}'" for outcome in last_outcome)

    all_reportedby = st.checkbox("Sélectionner tous les reporteurs")
    if all_reportedby:
        reportedby = st.multiselect(
            "Sélectionner un ou plusieurs reporteurs",
            options=reportedby_list,
            default=reportedby_list,
        )
    else:
        reportedby = st.multiselect(
            "Sélectionner un ou plusieurs reporteurs", options=reportedby_list
        )
    reportedby_str = ", ".join(f"'{reporter}'" for reporter in reportedby)
    start_date = st.date_input("Date de début", value=pd.to_datetime(start_date).date())
    end_date = st.date_input("Date de fin", value=pd.to_datetime(end_date).date())
    latitude_range = st.slider(
        "Latitude", min_value=min_lat, max_value=max_lat, value=(min_lat, max_lat)
    )
    longitude_range = st.slider(
        "Longitude", min_value=min_lon, max_value=max_lon, value=(min_lon, max_lon)
    )

    # Construire la requête SQL basée sur les filtres
    if st.button("Exécuter"):
        query = f"""
        SELECT * FROM street_temp
        WHERE Crimetype IN ({crime_types_str})
        AND Lastoutcomecategory IN ({last_outcome_str})
        AND Reportedby IN ({reportedby_str})
        AND Latitude BETWEEN {latitude_range[0]} AND {latitude_range[1]}
        AND Longitude BETWEEN {longitude_range[0]} AND {longitude_range[1]}
        LIMIT 1000
        """
        # Exécution de la requête et affichage
        filtered_data = pd.read_sql(query, conn)
        st.write(filtered_data)
