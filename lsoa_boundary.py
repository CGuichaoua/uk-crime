# -*- coding: utf-8 -*-
"""
Created on Sun Oct 20 00:07:18 2024

@author: ianni
"""

# =============================================================================
# conda activate projet_integration
#
# conda install conda-forge::geopandas
# conda install conda-forge::shapely
# conda install anaconda::openpyxl
# =============================================================================
import json
from shapely.geometry import shape, Point




# Function to load GeoJSON data
def load_geojson(filepath):
    with open(filepath, 'r') as f:
        data = json.load(f)
    return data

# Function to find the LSOA for a given latitude and longitude
def find_lsoa(lat, lon, geojson_data):
    """
    Takes latitude and longitude, returns LSOA if the point falls within any polygon.
    """
    point = Point(lon, lat)  # Note: (lon, lat) format for Point
    for feature in geojson_data['features']:
        polygon = shape(feature['geometry'])
        if polygon.contains(point):
            return feature['properties']['LSOA11CD']  # Adjust to match LSOA code field in your GeoJSON
    return None

# Example usage
geojson_data = load_geojson("C:/Users/ianni/Desktop/projet/Crimes au Royaume-Uni-20241014T124028Z-001/lsoa.geojson")

lat = 51.5074
lon = -0.1278  # Example: Central London
lsoa_code = find_lsoa(lat, lon, geojson_data)
print(f"LSOA Code for coordinates ({lat}, {lon}): {lsoa_code}")


# Function to get the centroid of a given LSOA code
def get_lsoa_centroid(lsoa_code, geojson_data):
    """
    Takes an LSOA code and returns the centroid coordinates (lat, lon).
    """
    for feature in geojson_data['features']:
        if feature['properties']['LSOA11CD'] == lsoa_code:  # Adjust if LSOA code field is different
            polygon = shape(feature['geometry'])
            centroid = polygon.centroid
            return centroid.y, centroid.x  # Return lat (y), lon (x)
    return None  # If the LSOA code is not found

# Example usage
lsoa_code = "E01000001"  # Replace with a valid LSOA code
centroid_coords = get_lsoa_centroid(lsoa_code, geojson_data)

if centroid_coords:
    print(f"Centroid of LSOA {lsoa_code}: {centroid_coords}")
else:
    print(f"LSOA code {lsoa_code} not found.")
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
def load_geojson(filepath):
    with open(filepath, 'r') as f:
        geojson_data = json.load(f)
    return geojson_data
    
def find_sa(lat, lon, geojson_data):
    point = Point(lon, lat)  # Point requires (longitude, latitude)
    
    for feature in geojson_data['features']:
        polygon = shape(feature['geometry'])
        if polygon.contains(point):
            return feature['properties']  # Return properties of the Small Area
    return None  # If no matching SA is found

# Load the Small Area GeoJSON file
sa_geojson = load_geojson('C:/Users/ianni/Desktop/projet/Crimes au Royaume-Uni-20241014T124028Z-001/sa2011.json')

# Example coordinates (replace with coordinates you'd like to test)
lat = 54.5973  # Latitude for Belfast
lon = -5.9301  # Longitude for Belfast

# Find the Small Area that contains the given point
sa_properties = find_sa(lat, lon, sa_geojson)

if sa_properties:
    print(f"Point ({lat}, {lon}) is in Small Area: {sa_properties}")
else:
    print(f"No Small Area found for point ({lat}, {lon}).")
    
    
    
    
    
    
def get_sa_centroid(sa_code, geojson_data):
    for feature in geojson_data['features']:
        if feature['properties']['SA2011'] == sa_code:
            polygon = shape(feature['geometry'])
            return polygon.centroid.y, polygon.centroid.x  # Return (latitude, longitude)
    return None  # If the Small Area code is not found

# Example Small Area code (replace with a valid Small Area code)
sa_code = 'N00000101'  # Example Small Area Code

# Get the centroid of the given Small Area
centroid = get_sa_centroid(sa_code, sa_geojson)
centroid_easting,centroid_northing=(centroid[0],centroid[1])

if centroid:
    print(f"Centroid of Small Area {sa_code}: (Latitude: {centroid[0]}, Longitude: {centroid[1]})")
else:
    print(f"No Small Area found with code {sa_code}.")
    
    
from pyproj import Proj, transform

# Define the projections
# Northern Ireland Grid (ITM)
proj_itm = Proj("epsg:29902")  # EPSG code for ITM
# WGS84 (standard latitude/longitude)
proj_wgs84 = Proj("epsg:4326")  # EPSG code for WGS84

def convert_itm_to_latlon(easting, northing):
    """
    Converts Northern Ireland Grid (ITM) coordinates to latitude and longitude.
    """
    longitude, latitude = transform(proj_itm, proj_wgs84, easting, northing)
    return latitude, longitude

# Example usage: replace with your centroid coordinates
centroid_easting = 388669.79957831727  # Example easting (ITM)
centroid_northing = 314737.32509261044  # Example northing (ITM)

# Convert to latitude and longitude
latitude, longitude = convert_itm_to_latlon(centroid_easting, centroid_northing)

print(f"Converted Centroid: (Latitude: {latitude}, Longitude: {longitude})")


def convert_latlon_to_itm(lat, lon):
    """
    Converts latitude and longitude (WGS84) to ITM (Northern Ireland Grid) coordinates.
    """
    proj_wgs84 = Proj("epsg:4326")  # WGS84
    proj_itm = Proj("epsg:29902")   # ITM (Northern Ireland Grid)
    easting, northing = transform(proj_wgs84, proj_itm, lon, lat)  # Remember: lon first
    return easting, northing

def find_sa_by_itm(easting, northing, geojson_data):
    """
    Takes ITM (easting/northing) and returns the SA code by checking which Small Area contains the point.
    """
    point = Point(easting, northing)  # Create a point using ITM coordinates (easting, northing)

    for feature in geojson_data['features']:
        polygon = shape(feature['geometry'])  # Get the polygon shape for each SA
        if polygon.contains(point):
            return feature['properties']  # Return the properties or SA code
    return None  # If no Small Area contains the point

# Load the Small Area GeoJSON file
# sa_geojson = load_geojson('path_to_your_sa_geojson.geojson')


# Example latitude and longitude (WGS84)
latitude = 54.5973  # Latitude for Belfast
longitude = -5.9301  # Longitude for Belfast

# Step 1: Convert the latitude/longitude to ITM coordinates
easting, northing = convert_latlon_to_itm(latitude, longitude)
print(f"Converted to ITM: (Easting: {easting}, Northing: {northing})")

# Step 2: Find the Small Area (SA) for the ITM coordinates
sa_properties = find_sa_by_itm(easting, northing, sa_geojson)

if sa_properties:
    print(f"Point ({latitude}, {longitude}) is in Small Area: {sa_properties}")
else:
    print(f"No Small Area found for the ITM point (Easting: {easting}, Northing: {northing}).")








lat=54.5107
lon=-7.96868
lat_ir,lon_ir=convert_latlon_to_itm(latitude,longitude)
sa_properties = find_sa_by_itm(lon_ir,lat_ir, sa_geojson)

