# -*- coding: utf-8 -*-
"""
Created on Sun Oct 20 13:46:14 2024

@author: ianni
"""
import os
import pandas as pd
import numpy as np
from sklearn.neighbors import NearestNeighbors
from collections import Counter
from lsoa_sa_boundaries import *



folder='C:/Users/ianni/Desktop/projet/Crimes au Royaume-Uni-20241014T124028Z-001/Crimes au Royaume-Uni'
lsoa_file='C:/Users/ianni/Desktop/projet/Crimes au Royaume-Uni-20241014T124028Z-001/LSOA_(2011)_to_LSOA_(2021)_to_Local_Authority_District_(2022)_Best_Fit_Lookup_for_EW_(V2).csv'
sa_file='C:/Users/ianni/Desktop/projet/Crimes au Royaume-Uni-20241014T124028Z-001/Look-up Tables_0.xlsx'



ending1='outcomes.csv'
ending2='stop-and-search.csv'
ending3='street.csv'

dfs1 = []
dfs2 = []
dfs3 = []
for root,dirs,_ in os.walk(folder):
    for directory in dirs:
        file_path=root+'/'+directory
        for root2,_,files in os.walk(file_path):
             for files_path in files:
                 path=root2+'/'+files_path
                 if path.endswith(ending1):                         
                     # Read the CSV file and append it to the list
                     dfs1.append(pd.read_csv(path))
                 elif path.endswith(ending2):
                     # Read the CSV file and append it to the list
                     dfs2.append(pd.read_csv(path))
                 elif path.endswith(ending3):
                     # Read the CSV file and append it to the list
                     dfs3.append(pd.read_csv(path))
                 else:
                     print('error line nowwhere')
                     
# Concatenate the lists of DataFrames into single DataFrames
df1 = pd.concat(dfs1, ignore_index=True) if dfs1 else pd.DataFrame()  # DataFrame for outcomes.csv
df2 = pd.concat(dfs2, ignore_index=True) if dfs2 else pd.DataFrame()  # DataFrame for stop-and-search.csv
df3 = pd.concat(dfs3, ignore_index=True) if dfs3 else pd.DataFrame()  # DataFrame for street.csv

lsoa_data=pd.read_csv(lsoa_file)
lsoa_data=lsoa_data[['LSOA11CD','LSOA11NM']]

sa_data=pd.read_excel(sa_file,sheet_name=2, engine='openpyxl')
sa_data=sa_data[['SA2011','SA2011NAME']]
sa_data['SA2011NAME']=sa_data['SA2011NAME'].str.extract(r'\((.*?)\)')

# =============================================================================
# step 1 take row with latitude and no lsoa and fill lsoa if possible in df1
# =============================================================================
mask = (df1['LSOA code'].isnull()) & (df1['Latitude'].notnull())
# Use the mask with .loc to keep the original indices
missing_lsoa_df1 = df1.loc[mask]
# missing_lsoa_df1 = df1[df1['LSOA code'].isnull()] # si code = null, name aussi
# missing_lsoa_df1 = missing_lsoa_df1[missing_lsoa_df1['Latitude'].notnull()] # si longitude = ok, latitude aussi
valid_lsoa_df1 = df1[df1['LSOA code'].notnull()] # si code = null, name aussi
valid_lsoa_df1 = valid_lsoa_df1[valid_lsoa_df1['Latitude'].notnull()] # si longitude = ok, latitude aussi
valid_lsoa_df1 = valid_lsoa_df1.drop_duplicates(subset=['Latitude', 'Longitude', 'LSOA code'])


# Prepare coordinates for KNN
valid_coords = valid_lsoa_df1[['Latitude', 'Longitude']].values
missing_coords = missing_lsoa_df1[['Latitude', 'Longitude']].values

knn = NearestNeighbors(n_neighbors=5, metric='euclidean')  # Euclidean distance for geographical proximity
knn.fit(valid_coords)
threshold=0.02

distances, indices = knn.kneighbors(missing_coords)

# Step 4: Extract the LSOA codes for the nearest neighbors
nearest_lsoa_codes = valid_lsoa_df1['LSOA code'].values[indices]


def most_common_lsoa(codes):
    # Use Counter to count occurrences of each LSOA code
    if len(codes) > 0:
        counts = Counter(codes)
        # Get the most common LSOA code
        most_common = counts.most_common()
        # Check for ties
        highest_count = most_common[0][1]  # Get the highest frequency
        candidates = [code for code, count in most_common if count == highest_count]
        # Return the first one (the left-most in case of a draw)
        return candidates[0]
    return None  # In case there are no codes

# Apply the function to find the most common LSOA code for each row
# =============================================================================
# missing_lsoa_df1['LSOA code'] = [most_common_lsoa(row) for row in nearest_lsoa_codes]
# =============================================================================
missing_lsoa_df1['LSOA code'] = [
    most_common_lsoa(valid_lsoa_df1['LSOA code'].values[indices[i][distances[i] < threshold]]) 
    for i in range(len(missing_coords))
]

missing_lsoa_df1['original_index'] = missing_lsoa_df1.index
missing_lsoa_df1 = missing_lsoa_df1.merge(lsoa_data, left_on='LSOA code', right_on='LSOA11CD', how='left')
missing_lsoa_df1['LSOA name'] = missing_lsoa_df1['LSOA name'].fillna(missing_lsoa_df1['LSOA11NM'])
missing_lsoa_df1 = missing_lsoa_df1.drop(['LSOA11CD', 'LSOA11NM'], axis=1)
missing_lsoa_df1 = missing_lsoa_df1[missing_lsoa_df1['LSOA code'].notnull()]
missing_lsoa_df1.set_index('original_index', inplace=True)

df1.update(missing_lsoa_df1)


# =============================================================================
# step 2 check if row with lsoa but no latitude do exist in df1
# =============================================================================
rows_with_lsoa_no_latitude = df1[df1['LSOA code'].notnull() & df1['Latitude'].isnull()]

# Display the result
print(rows_with_lsoa_no_latitude)


# =============================================================================
# step 3 take row with latitude and no lsoa and fill lsoa if possible in df3
# =============================================================================

mask = (df3['LSOA code'].isnull()) & (df3['Latitude'].notnull())
# Use the mask with .loc to keep the original indices
missing_lsoa_df3 = df3.loc[mask]

centroid_lsoa=[[get_lsoa_centroid(x,lsoa_geojson),x] for x in lsoa_data['LSOA11CD']]
centroid_sa=[[get_sa_centroid(x,sa_geojson),x] for x in sa_data['SA2011']]

centroid_lsoa = [(lat_lon[0], lat_lon[1], code) for (lat_lon, code) in centroid_lsoa]
centroid_lsoa = pd.DataFrame(centroid_lsoa, columns=['lat', 'lon', 'LSOA code'])

centroid_sa = [(lat_lon[0], lat_lon[1], code) for (lat_lon, code) in centroid_sa if lat_lon is not None]
centroid_sa = pd.DataFrame(centroid_sa, columns=['lat', 'lon', 'SA code'])


#knn sur les centroid pour trouver le plus proche voisin des lat,lon orphelin de lsoa/sa code
nn_sa = NearestNeighbors(n_neighbors=1, metric='euclidean')
nn_sa.fit(centroid_sa[['lat', 'lon']])

nn_lsoa = NearestNeighbors(n_neighbors=1, metric='euclidean')
nn_lsoa.fit(centroid_lsoa[['lat', 'lon']])

lsoa_codes = []
distance_lsoa = []
sa_codes = []
distance_sa = []

coordinates = missing_lsoa_df3[['Latitude', 'Longitude']].values

# Get nearest neighbors for LSOA
distances_lsoa, indices_lsoa = nn_lsoa.kneighbors(coordinates)

# Get nearest neighbors for SA
distances_sa, indices_sa = nn_sa.kneighbors(coordinates)

# Extract the corresponding LSOA codes and SA codes
missing_lsoa_df3['lsoa_code_match'] = centroid_lsoa.iloc[indices_lsoa.flatten()]['LSOA code'].values
missing_lsoa_df3['distance_lsoa_match'] = distances_lsoa.flatten()

missing_lsoa_df3['sa_code_match'] = centroid_sa.iloc[indices_sa.flatten()]['SA code'].values
missing_lsoa_df3['distance_sa_match'] = distances_sa.flatten()


threshold = 0.01  #~1km

# Update the LSOA code based on the smallest distance
missing_lsoa_df3['LSOA code'] = np.select(
    [
        (missing_lsoa_df3['distance_lsoa_match'] < threshold) & 
        (missing_lsoa_df3['distance_lsoa_match'] < missing_lsoa_df3['distance_sa_match']),
        (missing_lsoa_df3['distance_sa_match'] < threshold)
    ],
    [
        missing_lsoa_df3['lsoa_code_match'],
        missing_lsoa_df3['sa_code_match']
    ],
    default=missing_lsoa_df3['LSOA code']
)

missing_lsoa_df3.drop(columns=missing_lsoa_df3.columns[-4:], inplace=True)


missing_lsoa_df3['original_index'] = missing_lsoa_df3.index
missing_lsoa_df3 = missing_lsoa_df3.merge(lsoa_data, left_on='LSOA code', right_on='LSOA11CD', how='left')
missing_lsoa_df3['LSOA name'] = missing_lsoa_df3['LSOA name'].fillna(missing_lsoa_df3['LSOA11NM'])
missing_lsoa_df3 = missing_lsoa_df3.drop(['LSOA11CD', 'LSOA11NM'], axis=1)

missing_lsoa_df3 = missing_lsoa_df3.merge(sa_data, left_on='LSOA code', right_on='SA2011', how='left')
missing_lsoa_df3['LSOA name'] = missing_lsoa_df3['LSOA name'].fillna(missing_lsoa_df3['SA2011NAME'])
missing_lsoa_df3 = missing_lsoa_df3.drop(['SA2011', 'SA2011NAME'], axis=1)

missing_lsoa_df3 = missing_lsoa_df3[missing_lsoa_df3['LSOA code'].notnull()]
missing_lsoa_df3.set_index('original_index', inplace=True)

df3.update(missing_lsoa_df3)


# =============================================================================
# step 4 check if row with lsoa but no latitude do exist in df3
# =============================================================================
rows_with_lsoa_no_latitude = df3[df3['LSOA code'].notnull() & df3['Latitude'].isnull()]

# Display the result
print(rows_with_lsoa_no_latitude)





















