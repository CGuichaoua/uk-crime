# -*- coding: utf-8 -*-
"""
Created on Fri Oct 18 10:23:11 2024

@author: Admin.local
"""

from sanstitre1 import *

df_outcomes,df_stop_search,df_street=genere_dataframe_from_data(folder)

a=df_outcomes['Crime ID']
b=df_street['Crime ID']

a_clean = a.dropna().replace("", None).dropna()
b_clean = b.dropna().replace("", None).dropna()

# Step 2: Create a DataFrame using full outer join
df = pd.merge(a_clean.rename('a'), b_clean.rename('b'), left_on=a_clean.index, right_on=b_clean.index, how='outer', indicator=True)

# Step 3: Look for null values (these will indicate missing values in either series)
missing_values = df[df.isnull().any(axis=1)]





merged_df = pd.merge(df1, df3, on='Crime ID', suffixes=('_df1', '_df3'))
merged_df['Lat_Long_Match'] = (
    (merged_df['Latitude_df1'] == merged_df['Latitude_df3']) &
    (merged_df['Longitude_df1'] == merged_df['Longitude_df3'])
)

# Identify rows with mismatches
mismatches = merged_df[~merged_df['Lat_Long_Match']]
mismatches = mismatches.dropna(subset=['Latitude_df1'])
mismatches['lat_diff']=mismatches['Latitude_df1']-mismatches['Latitude_df3']
mismatches['lon_diff']=mismatches['Longitude_df1']-mismatches['Longitude_df3']
mismatches['dist_diff'] = np.sqrt(mismatches['lat_diff'] ** 2 + mismatches['lon_diff'] ** 2)
mismatches.dropna(subset=['dist_diff'])
mismatches.dropna(subset=['dist_diff'],inplace=True)
len(mismatches)
len(mismatches[mismatches['dist_diff']<=0.1])
len(mismatches[mismatches['dist_diff']<=0.01])
len(merged_df[merged_df['Lat_Long_Match']])