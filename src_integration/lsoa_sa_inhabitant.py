# -*- coding: utf-8 -*-
"""
Created on Sun Oct 20 13:24:21 2024

@author: ianni
"""

import pandas as pd

path_inhabitant=['C:/Users/ianni/Desktop/projet/Crimes au Royaume-Uni-20241014T124028Z-001/lsoa_inhabitant.xlsx',
                 'C:/Users/ianni/Desktop/projet/Crimes au Royaume-Uni-20241014T124028Z-001/sa_inhabitant.xlsx']

lsoa_inhabitant=pd.read_excel(path_inhabitant[0],sheet_name=3,skiprows=4, engine='openpyxl')
sa_inhabitant=pd.read_excel(path_inhabitant[1],sheet_name=2,skiprows=7,engine='openpyxl')

lsoa_inhabitant=lsoa_inhabitant[['LSOA Code','All Ages']]
sa_inhabitant=sa_inhabitant[['Area_Code',2020]]

lsoa_inhabitant = lsoa_inhabitant.rename(columns={
    'All Ages': 'inhabitants'})
sa_inhabitant=sa_inhabitant.rename(columns={
    'Area_Code': 'SA Code',
    2020: 'inhabitants'})

# Export to CSV
csv_file_path = 'lsoa_inhabitant.csv'  # Specify the path where you want to save the CSV
lsoa_inhabitant.to_csv(csv_file_path, index=False)

csv_file_path = 'sa_inhabitant.csv'  # Specify the path where you want to save the CSV
sa_inhabitant.to_csv(csv_file_path, index=False)