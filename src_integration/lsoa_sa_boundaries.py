# -*- coding: utf-8 -*-
"""
Created on Sun Oct 20 11:59:17 2024

@author: ianni
"""

import json
from shapely.geometry import shape, Point
from pyproj import Proj, transform
from pyproj import Transformer
import pandas as pd


paths_geo=["C:/Users/Admin.local/Documents/projetint/files_LSOA/lsoa.geojson",
      'C:/Users/Admin.local/Documents/projetint/files_LSOA/sa2011.json',
      'C:/Users/Admin.local/Documents/projetint/files_LSOA/Look-up Tables_0.xlsx']

def load_geojson(filepath):
    with open(filepath, 'r') as f:
        data = json.load(f)
    return data

def data_loader(paths_geo):
    """
    Parameters
    ----------
    paths_geo : string
        a path to file
    Returns
    -------
    lsoa_geojson : dict
        dictionary that maps coordinates & lsoas
    sa_geojson : dict
        dictionary that maps coordinates & sas (warning not standard coordinates)
    sa_name : dataframe
        dataframe that maps sas' names with sas' codes
    """
    path_lsoa_geo=paths_geo[0]
    path_sa_geo=paths_geo[1]
    path_sa_name=paths_geo[2]
    lsoa_geojson = load_geojson(path_lsoa_geo)
    sa_geojson = load_geojson(path_sa_geo)
    sa_name=pd.read_excel(path_sa_name,sheet_name=2, engine='openpyxl')
    return lsoa_geojson,sa_geojson,sa_name[sa_name.columns[:2]]

def find_lsoa(lat, lon, geojson_data):
    """
    Parameters
    ----------
    lat : float
        coordinate value (latitude)
    lon : float
        coordinate value (longitude)
    geojson_data : dict
        dictionary that maps coordinates & lsoas
    Returns
    -------
    string
        lsoa code associated with the coordinates
    string
        lsoa name associated with the coordinates
    """
    point = Point(lon, lat)
    for feature in geojson_data['features']:
        polygon = shape(feature['geometry'])
        if polygon.contains(point):
            return feature['properties']['LSOA11CD'],feature['properties']['LSOA11NM']

def get_lsoa_centroid(lsoa_code, geojson_data):
    """
    Parameters
    ----------
    lsoa_code : string
        lsoa code
    geojson_data : dict
        dictionary that maps coordinates & lsoas  
    Returns
    -------
    centroid.y : float
        coordinate value (latitude) of the centroid of the lsoa
    centroid.x : float
        coordinate value (longitude) of the centroid of the lsoa
    """
    for feature in geojson_data['features']:
        if feature['properties']['LSOA11CD'] == lsoa_code:  # Adjust if LSOA code field is different
            polygon = shape(feature['geometry'])
            centroid = polygon.centroid
            return centroid.y, centroid.x  # Return lat (y), lon (x)
        
#itm = Irish Transverse Mercator
def convert_latlon_to_itm(lat, lon):
    """
    Parameters
    ----------
    lat : float
        coordinate value (latitude)
    lon : float
        coordinate value (longitude)
    Returns
    -------
    easting : float
        coordinate in the itm standard
    northing : float
        coordinate in the itm standard
    """
    # proj_wgs84 = Proj("epsg:4326")  # WGS84 (World Geodetic System 1984)
    # proj_itm = Proj("epsg:29902")   # ITM (Northern Ireland Grid)
    # easting, northing = transform(proj_wgs84, proj_itm, lon, lat)
    transformer = Transformer.from_crs("EPSG:4326", "epsg:29902")
    easting,northing= transformer.transform(lon, lat)
    return easting, northing
        
def find_sa(lat, lon, geojson_data, sa_names):
    """
    Parameters
    ----------
    lat : float
        coordinate value (latitude)
    lon : float
        coordinate value (longitude)
    geojson_data : dict
        dictionary that maps coordinates & sas (warning not standard coordinates)
    Returns
    -------
    sa_code : string
        sa code associated with the coordinates
    sa_name : string
        sa name associated with the coordinates
    """    
    easting,northing=convert_latlon_to_itm(lon,lat)
    point = Point(easting, northing)
    for feature in geojson_data['features']:
        polygon = shape(feature['geometry'])  # Get the polygon shape for each SA
        if polygon.contains(point):
            sa_code=feature['properties']['SA2011']
            sa_name=sa_names[sa_names['SA2011']==sa_code]['SA2011NAME'].iloc[0].split('(')[1][:-1]
            return sa_code,sa_name

def convert_itm_to_latlon(easting, northing):
    """
    Parameters
    ----------
    easting : float
        coordinate in the itm standard
    northing : float
        coordinate in the itm standard
    Returns
    -------
    longitude : float
        coordinate value (longitude)
    latitude : float
        coordinate value (latitude)
    """
    # proj_wgs84 = Proj("epsg:4326")  # WGS84 (World Geodetic System 1984)
    # proj_itm = Proj("epsg:29902")   # ITM (Northern Ireland Grid)
    # longitude, latitude = transform(proj_itm, proj_wgs84, easting, northing)
    transformer = Transformer.from_crs("epsg:29902","EPSG:4326")
    longitude, latitude= transformer.transform(easting,northing)
    return latitude,longitude

def get_sa_centroid(sa_code, geojson_data):
    """
    Parameters
    ----------
    sa_code : string
        sa code
    geojson_data : dict
        dictionary that maps coordinates & sas (warning not standard coordinates)
    Returns
    -------
    longitude : float
        coordinate value (longitude)
    latitude : float
        coordinate value (latitude)
    """
    for feature in geojson_data['features']:
        if feature['properties']['SA2011'] == sa_code:
            polygon = shape(feature['geometry'])
            latitude, longitude = convert_itm_to_latlon(polygon.centroid.y, polygon.centroid.x)
            return longitude, latitude
        
lsoa_geojson,sa_geojson,sa_names=data_loader(paths_geo)









