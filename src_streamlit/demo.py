import streamlit as st
import pydeck as pdk
import pandas as pd

# Exemple de données avec latitude et longitude
data = pd.DataFrame(
    {
        "latitude": [37.7749, 40.7128, 34.0522],
        "longitude": [-122.4194, -74.0060, -118.2437],
        "city": ["San Francisco", "New York", "Los Angeles"],
    }
)

# Configurer la vue initiale de la carte
view_state = pdk.ViewState(latitude=37.7749, longitude=-122.4194, zoom=4, pitch=50)

# Ajouter des points pour chaque ville
layer = pdk.Layer(
    "ScatterplotLayer",
    data,
    get_position="[longitude, latitude]",
    get_radius=100000,
    get_color=[255, 0, 0],
    pickable=True,
)

# Configurer la carte avec la vue et les points
map = pdk.Deck(
    layers=[layer], initial_view_state=view_state, tooltip={"text": "{city}"}
)

# Afficher la carte dans Streamlit
st.pydeck_chart(map)
