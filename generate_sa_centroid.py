# -*- coding: utf-8 -*-
"""
Created on Mon Oct 21 22:58:03 2024

@author: ianni
"""
# conda install anaconda::pyshp
import shapefile  # pyshp
from shapely.geometry import shape
from pyproj import Transformer


transformer = Transformer.from_crs("EPSG:29902", "EPSG:4326")
shp_path = "C:/Users/ianni/Desktop/projet/Crimes au Royaume-Uni-20241014T124028Z-001/SA2011_Esri_Shapefile_0/SA2011.shp"

with shapefile.Reader(shp_path) as shp:
    # Get the field names (to find the relevant SA code field, e.g., 'SA2011')
    fields = [field[0] for field in shp.fields[1:]]  # Skip first deletion flag field
    sa_code_index = fields.index("SA2011")  # Adjust to match your shapefile field name

    # Loop over all records and shapes
    centroids = []
    for record, shape_rec in zip(shp.records(), shp.shapes()):
        sa_code = record[sa_code_index]

        # Get the geometry and calculate the centroid
        geom = shape(shape_rec.__geo_interface__)
        centroid = geom.centroid

        # Convert the centroid to WGS84 (lat/lon)
        lon, lat = transformer.transform(centroid.x, centroid.y)

        # Store the SA code and centroid coordinates
        centroids.append([lat, lon, sa_code])
        
import pandas as pd
centroid_df = pd.DataFrame(centroids, columns=['lon', 'lat', 'SA code'])

# Save to CSV if needed
centroid_df.to_csv("sa_centroids.csv", index=False)

# Display the first few rows
print(centroid_df.head())