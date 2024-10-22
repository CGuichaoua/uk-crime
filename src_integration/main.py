import pandas as pd
import numpy as np
import atexit
from db_service_sqlalchemy import create_connection, close_connection
from sklearn.neighbors import NearestNeighbors
from collections import Counter
from lsoa_sa_boundaries import *


def update_table(df, session):
   
    # Mettre à jour la table dans la base de données
    for index, row in df.iterrows():
        update_query = f"""
        UPDATE your_table
        SET existing_column = {row['existing_column']}
        WHERE id = {row['id']}
        """
        session.execute(update_query)

    # Commit les changements
    session.commit()



# Connexion à la base de données
db_name="crime"
conn, engine = create_connection("127.0.0.1", "root", "", db_name)

# Enregistrer la fonction de nettoyage pour qu'elle soit appelée à la sortie
atexit.register(lambda:close_connection(conn,engine))

if __name__ == "__main__":
    print("Connexion à la base de données réussie")

    
    # Exemple de requête SQL
    query = "SELECT Longitude, Latitude, LSOAcode, LSOAname FROM outcomes_temp"
    df1 = pd.read_sql(query, conn)
   
    query = "SELECT Longitude, Latitude FROM stopandsearch_temp"
    df2 = pd.read_sql(query, conn)
    df2['LSOAcode'] = None
    df2['LSOAname'] = None


    query= "SELECT Longitude, Latitude, LSOAcode, LSOAname FROM street_temp"
    df3 = pd.read_sql(query, conn)


    print('vérification des données avant intégration')
    print(len(df1))
    print(len(df2))
    print(len(df3))
    lsoa_file='LSOA_(2011)_to_LSOA_(2021)_to_Local_Authority_District_(2022)_Best_Fit_Lookup_for_EW_(V2).csv'
    sa_file='Look-up Tables_0.xlsx'




    lsoa_data=pd.read_csv(lsoa_file)
    lsoa_data=lsoa_data[['LSOA11CD','LSOA11NM']]

    sa_data=pd.read_excel(sa_file,sheet_name=2, engine='openpyxl')
    sa_data=sa_data[['SA2011','SA2011NAME']]
    sa_data['SA2011NAME']=sa_data['SA2011NAME'].str.extract(r'\((.*?)\)')

    # =============================================================================
    # step 1 take row with latitude and no lsoa and fill lsoa if possible in df1
    # =============================================================================
    mask = (df1['LSOAcode'].isnull()) & (df1['Latitude'].notnull())
    # Use the mask with .loc to keep the original indices
    missing_lsoa_df1 = df1.loc[mask]
    # missing_lsoa_df1 = df1[df1['LSOAcode'].isnull()] # si code = null, name aussi
    # missing_lsoa_df1 = missing_lsoa_df1[missing_lsoa_df1['Latitude'].notnull()] # si longitude = ok, latitude aussi
    valid_lsoa_df1 = df1[df1['LSOAcode'].notnull()] # si code = null, name aussi
    valid_lsoa_df1 = valid_lsoa_df1[valid_lsoa_df1['Latitude'].notnull()] # si longitude = ok, latitude aussi
    valid_lsoa_df1 = valid_lsoa_df1.drop_duplicates(subset=['Latitude', 'Longitude', 'LSOAcode'])


    # Prepare coordinates for KNN
    valid_coords = valid_lsoa_df1[['Latitude', 'Longitude']].values
    missing_coords = missing_lsoa_df1[['Latitude', 'Longitude']].values

    knn = NearestNeighbors(n_neighbors=5, metric='euclidean')  # Euclidean distance for geographical proximity
    knn.fit(valid_coords)
    threshold=0.02

    distances, indices = knn.kneighbors(missing_coords)

    # Step 4: Extract the LSOAcodes for the nearest neighbors
    nearest_lsoa_codes = valid_lsoa_df1['LSOAcode'].values[indices]


    def most_common_lsoa(codes):
        # Use Counter to count occurrences of each LSOAcode
        if len(codes) > 0:
            counts = Counter(codes)
            # Get the most common LSOAcode
            most_common = counts.most_common()
            # Check for ties
            highest_count = most_common[0][1]  # Get the highest frequency
            candidates = [code for code, count in most_common if count == highest_count]
            # Return the first one (the left-most in case of a draw)
            return candidates[0]
        return None  # In case there are no codes

    # Apply the function to find the most common LSOAcode for each row
    # =============================================================================
    # missing_lsoa_df1['LSOAcode'] = [most_common_lsoa(row) for row in nearest_lsoa_codes]
    # =============================================================================
    missing_lsoa_df1['LSOAcode'] = [
        most_common_lsoa(valid_lsoa_df1['LSOAcode'].values[indices[i][distances[i] < threshold]]) 
        for i in range(len(missing_coords))
    ]

    missing_lsoa_df1['original_index'] = missing_lsoa_df1.index
    missing_lsoa_df1 = missing_lsoa_df1.merge(lsoa_data, left_on='LSOAcode', right_on='LSOA11CD', how='left')
    missing_lsoa_df1['LSOAname'] = missing_lsoa_df1['LSOAname'].fillna(missing_lsoa_df1['LSOA11NM'])
    missing_lsoa_df1 = missing_lsoa_df1.drop(['LSOA11CD', 'LSOA11NM'], axis=1)
    missing_lsoa_df1 = missing_lsoa_df1[missing_lsoa_df1['LSOAcode'].notnull()]
    missing_lsoa_df1.set_index('original_index', inplace=True)

    df1.update(missing_lsoa_df1)
    print(df1.head())


    # =============================================================================
    # step 2 check if row with lsoa but no latitude do exist in df1
    # =============================================================================
    rows_with_lsoa_no_latitude = df1[df1['LSOAcode'].notnull() & df1['Latitude'].isnull()]

    # Display the result
    print(rows_with_lsoa_no_latitude)


    # =============================================================================
    # step 3 take row with latitude and no lsoa and fill lsoa if possible in df3
    # =============================================================================

    mask = (df3['LSOAcode'].isnull()) & (df3['Latitude'].notnull())
    # Use the mask with .loc to keep the original indices
    missing_lsoa_df3 = df3.loc[mask]

    centroid_lsoa=[[get_lsoa_centroid(x,lsoa_geojson),x] for x in lsoa_data['LSOA11CD']]
    # centroid_sa=[[get_sa_centroid(x,sa_geojson),x] for x in sa_data['SA2011']]

    centroid_lsoa = [(lat_lon[0], lat_lon[1], code) for (lat_lon, code) in centroid_lsoa]
    centroid_lsoa = pd.DataFrame(centroid_lsoa, columns=['lat', 'lon', 'LSOAcode'])

    # centroid_sa = [(lat_lon[0], lat_lon[1], code) for (lat_lon, code) in centroid_sa if lat_lon is not None]
    # centroid_sa = pd.DataFrame(centroid_sa, columns=['lat', 'lon', 'SA code'])
    centroid_sa=pd.read_csv('sa_centroids.csv')

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

    # Extract the corresponding LSOAcodes and SA codes
    missing_lsoa_df3['lsoa_code_match'] = centroid_lsoa.iloc[indices_lsoa.flatten()]['LSOAcode'].values
    missing_lsoa_df3['distance_lsoa_match'] = distances_lsoa.flatten()

    missing_lsoa_df3['sa_code_match'] = centroid_sa.iloc[indices_sa.flatten()]['SA code'].values
    missing_lsoa_df3['distance_sa_match'] = distances_sa.flatten()


    threshold = 0.05  #~1km

    # Update the LSOAcode based on the smallest distance
    missing_lsoa_df3['LSOAcode'] = np.select(
        [
            (missing_lsoa_df3['distance_lsoa_match'] < threshold) & 
            (missing_lsoa_df3['distance_lsoa_match'] < missing_lsoa_df3['distance_sa_match']),
            (missing_lsoa_df3['distance_sa_match'] < threshold)
        ],
        [
            missing_lsoa_df3['lsoa_code_match'],
            missing_lsoa_df3['sa_code_match']
        ],
        default=missing_lsoa_df3['LSOAcode']
    )

    missing_lsoa_df3.drop(columns=missing_lsoa_df3.columns[-4:], inplace=True)


    missing_lsoa_df3['original_index'] = missing_lsoa_df3.index
    missing_lsoa_df3 = missing_lsoa_df3.merge(lsoa_data, left_on='LSOAcode', right_on='LSOA11CD', how='left')
    missing_lsoa_df3['LSOAname'] = missing_lsoa_df3['LSOAname'].fillna(missing_lsoa_df3['LSOA11NM'])
    missing_lsoa_df3 = missing_lsoa_df3.drop(['LSOA11CD', 'LSOA11NM'], axis=1)

    missing_lsoa_df3 = missing_lsoa_df3.merge(sa_data, left_on='LSOAcode', right_on='SA2011', how='left')
    missing_lsoa_df3['LSOAname'] = missing_lsoa_df3['LSOAname'].fillna(missing_lsoa_df3['SA2011NAME'])
    missing_lsoa_df3 = missing_lsoa_df3.drop(['SA2011', 'SA2011NAME'], axis=1)

    missing_lsoa_df3 = missing_lsoa_df3[missing_lsoa_df3['LSOAcode'].notnull()]
    missing_lsoa_df3.set_index('original_index', inplace=True)

    df3.update(missing_lsoa_df3)
    print(df3.head())

    # =============================================================================
    # step 4 check if row with lsoa but no latitude do exist in df3
    # =============================================================================
    rows_with_lsoa_no_latitude = df3[df3['LSOAcode'].notnull() & df3['Latitude'].isnull()]

    # Display the result
    print(rows_with_lsoa_no_latitude)


    # =============================================================================
    # step 5 take row with latitude and no lsoa and fill lsoa if possible in df2 (repeat step 3)
    # =============================================================================


    missing_lsoa_df2 = df2[df2['Latitude'].notnull()]
    # =============================================================================
    # missing_lsoa_df2['LSOAcode']=None
    # missing_lsoa_df2['LSOAname']=None
    # =============================================================================
    lsoa_codes = []
    distance_lsoa = []
    sa_codes = []
    distance_sa = []

    coordinates = missing_lsoa_df2[['Latitude', 'Longitude']].values

    # Get nearest neighbors for LSOA
    distances_lsoa, indices_lsoa = nn_lsoa.kneighbors(coordinates)

    # Get nearest neighbors for SA
    distances_sa, indices_sa = nn_sa.kneighbors(coordinates)

    # Extract the corresponding LSOAcodes and SA codes
    missing_lsoa_df2['lsoa_code_match'] = centroid_lsoa.iloc[indices_lsoa.flatten()]['LSOAcode'].values
    missing_lsoa_df2['distance_lsoa_match'] = distances_lsoa.flatten()

    missing_lsoa_df2['sa_code_match'] = centroid_sa.iloc[indices_sa.flatten()]['SA code'].values
    missing_lsoa_df2['distance_sa_match'] = distances_sa.flatten()

    threshold = 0.05  #~1km


    # Update the LSOAcode based on the smallest distance
    missing_lsoa_df2['LSOAcode'] = np.select(
        [
            (missing_lsoa_df2['distance_lsoa_match'] < threshold) & 
            (missing_lsoa_df2['distance_lsoa_match'] < missing_lsoa_df2['distance_sa_match']),
            (missing_lsoa_df2['distance_sa_match'] < threshold)
        ],
        [
            missing_lsoa_df2['lsoa_code_match'],
            missing_lsoa_df2['sa_code_match']
        ],
        default=missing_lsoa_df2['LSOAcode']
    )

    missing_lsoa_df2.drop(columns=missing_lsoa_df2.columns[-4:], inplace=True)

    missing_lsoa_df2['original_index'] = missing_lsoa_df2.index
    missing_lsoa_df2 = missing_lsoa_df2.merge(lsoa_data, left_on='LSOAcode', right_on='LSOA11CD', how='left')
    missing_lsoa_df2['LSOAname'] = missing_lsoa_df2['LSOAname'].fillna(missing_lsoa_df2['LSOA11NM'])
    missing_lsoa_df2 = missing_lsoa_df2.drop(['LSOA11CD', 'LSOA11NM'], axis=1)

    missing_lsoa_df2 = missing_lsoa_df2.merge(sa_data, left_on='LSOAcode', right_on='SA2011', how='left')
    missing_lsoa_df2['LSOAname'] = missing_lsoa_df2['LSOAname'].fillna(missing_lsoa_df2['SA2011NAME'])
    missing_lsoa_df2 = missing_lsoa_df2.drop(['SA2011', 'SA2011NAME'], axis=1)

    missing_lsoa_df2 = missing_lsoa_df2[missing_lsoa_df2['LSOAcode'].notnull()]
    missing_lsoa_df2.set_index('original_index', inplace=True)


    # df2['LSOAcode']=None
    # df2['LSOAname']=None
    df2.update(missing_lsoa_df2)
    print(df2.head())









