# -*- coding: utf-8 -*-
"""
Created on Fri Oct 18 10:23:11 2024

@author: Admin.local
"""

from sanstitre1 import *

df_outcomes,df_stop_search,df_street=genere_dataframe_from_data(folder)


df_outcomes=df_outcomes[['Longitude','Latitude','Location','LSOA code','LSOA name']]
df_street=df_street[['Longitude','Latitude','Location','LSOA code','LSOA name']]

df_outcomes2=df_outcomes.copy()
df_street2=df_street.copy()


df_outcomes.drop_duplicates().dropna(inplace=True)
df_street.drop_duplicates().dropna(inplace=True)

df_outcomes=df_outcomes[df_outcomes['Location']!='No location']
df_street=df_street[df_street['Location']!='No location']




outcomes_lsoas=df_outcomes[['LSOA code','LSOA name']]
street_lsoas=df_street[['LSOA code','LSOA name']]

outcomes_lsoas=outcomes_lsoas.drop_duplicates()
street_lsoas=street_lsoas.drop_duplicates()


df = pd.merge(outcomes_lsoas, street_lsoas, on='LSOA code', how='outer', indicator=True)


df2= pd.merge(outcomes_lsoas, street_lsoas, on='LSOA name', how='outer', indicator=True)


mismatch_rows = df[(df['_merge'] == 'both') & (df['LSOA name_x'] != df['LSOA name_y'])]
mismatch_rows2 = df2[(df2['_merge'] == 'both') & (df2['LSOA code_x'] != df2['LSOA code_y'])]


df3=pd.concat([df_outcomes,df_street])