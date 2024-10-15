# -*- coding: utf-8 -*-
"""
Created on Mon Oct 14 22:15:07 2024

@author: ianni
"""

import os
import pandas as pd

folder="C:/Users/Admin.local/Documents/projetint/files"



ending1='outcomes.csv'
ending2='stop-and-search.csv'
ending3='street.csv'
# Initialize empty lists to hold DataFrames
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
                     
# Concatenate the lists of DataFrames into single DataFrames
df1 = pd.concat(dfs1, ignore_index=True) if dfs1 else pd.DataFrame()  # DataFrame for outcomes.csv
df2 = pd.concat(dfs2, ignore_index=True) if dfs2 else pd.DataFrame()  # DataFrame for stop-and-search.csv
df3 = pd.concat(dfs3, ignore_index=True) if dfs3 else pd.DataFrame()  # DataFrame for street.csv

