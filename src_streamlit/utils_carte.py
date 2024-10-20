import streamlit as st
import pydeck as pdk
import pandas as pd
import numpy as np
import sqlite3


def afficher_carte(
    titre_carte: str,
    df: pd.DataFrame,
    latitude: float,
    longitude: float,
    color_map: dict,
) -> None:

    st.markdown(f"## {titre_carte}")

    # Configuration de la vue de la carte
    view_state = pdk.ViewState(
        latitude=latitude,  # Position initiale de la carte
        longitude=longitude,
        zoom=10,
        pitch=50,
    )

    # Création de la couche de visualisation des points
    layer = pdk.Layer(
        "ScatterplotLayer",
        df,
        get_position="[Longitude, Latitude]",
        get_radius=200,
        get_color="Color",  # Utiliser la colonne de couleur
        pickable=True,
    )

    # Affichage de la carte dans Streamlit
    st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view_state))

    # Affichage de la légende
    st.markdown("### Légende")
    for crimetype, color in color_map.items():
        color_hex = "#%02x%02x%02x" % tuple(color)
        st.markdown(
            f'<span style="color:{color_hex};">■</span> {crimetype}',
            unsafe_allow_html=True,
        )
