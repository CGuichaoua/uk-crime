# -*- coding: utf-8 -*-
"""
Created on Mon Oct 14 22:15:07 2024

@author: ianni
"""

import os
import pandas as pd

folder='C:/Users/ianni/Desktop/projet/Crimes au Royaume-Uni-20241014T124028Z-001/Crimes au Royaume-Uni'

count=0
dico=dict()
for root,dirs,_ in os.walk(folder):
    for directory in dirs:
        file_path=root+'/'+directory
        for root2,_,files in os.walk(file_path):
             for files_path in files:
                 path=root2+'/'+files_path
                 #print(root2+'/'+files_path)
                 data=pd.read_csv(path)
                 count+=1
                 for x in data.columns:
                     if x in dico:
                         dico[x]+=1
                     else:
                         dico[x]=1
 
print(count)
print(dico)                



ending1='outcomes.csv'
ending2='stop-and-search.csv'
ending3='street.csv'

count1=0
count2=0
count3=0
dico1=dict()
dico2=dict()
dico3=dict()
for root,dirs,_ in os.walk(folder):
    for directory in dirs:
        file_path=root+'/'+directory
        for root2,_,files in os.walk(file_path):
             for files_path in files:
                 path=root2+'/'+files_path
                 if path.endswith(ending1):
                     count1+=1
                     data=pd.read_csv(path)
                     for x in data.columns:
                         if x in dico1:
                             dico1[x]+=1
                         else:
                             dico1[x]=1
                 elif path.endswith(ending2):
                    count2+=1
                    data=pd.read_csv(path)
                    for x in data.columns:
                        if x in dico2:
                            dico2[x]+=1
                        else:
                            dico2[x]=1
                 elif path.endswith(ending3):
                    count3+=1
                    data=pd.read_csv(path)
                    for x in data.columns:
                        if x in dico3:
                            dico3[x]+=1
                        else:
                            dico3[x]=1
                 else:
                    pass
                 
                         
                         
ending1='outcomes.csv'
ending2='stop-and-search.csv'
ending3='street.csv'
                        
dico_1=dict()
dico_2=dict()
dico_3=dict()
count_1=0
count_2=0
count_3=0                        
                   
for root,dirs,_ in os.walk(folder):
    for directory in dirs:
        file_path=root+'/'+directory
        for root2,_,files in os.walk(file_path):
             for files_path in files:
                 path=root2+'/'+files_path
                 if path.endswith(ending1):

                     data=pd.read_csv(path)
                     null_values=data.isnull().sum()
                     lenn=len(data)
                     count_1+=lenn
                     for x in data.columns:
                         if x in dico_1:
                             dico_1[x]+=null_values[x]
                             
                         else:
                             dico_1[x]=null_values[x]
                        
                 elif path.endswith(ending2):

                    data=pd.read_csv(path)
                    null_values=data.isnull().sum()
                    lenn=len(data)
                    count_2+=lenn
                    for x in data.columns:
                        if x in dico_2:
                            dico_2[x]+=null_values[x]
                            
                        else:
                            dico_2[x]=null_values[x]
                 elif path.endswith(ending3):

                    data=pd.read_csv(path)
                    null_values=data.isnull().sum()
                    lenn=len(data)
                    count_3+=lenn
                    for x in data.columns:
                        if x in dico_3:
                            dico_3[x]+=null_values[x]
                            
                        else:
                            dico_3[x]=null_values[x]
                 else:
                    print('something went wrong here')
                    print(path)
                    pass      
                         
                         
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

def count_unique_values(df):
    counts = {}
    for col in df.columns:
        counts[col] = df[col].value_counts().sort_values(ascending=False)  # Count and sort
    return counts                    
                         
# Count occurrences for each DataFrame
df1_counts = count_unique_values(df1)
df2_counts = count_unique_values(df2)
df3_counts = count_unique_values(df3)
                         
import matplotlib.pyplot as plt
import seaborn as sns


def plot_counts(df_counts, df_name):
    for col, count in df_counts.items():
        if len(count)==0:
            print(f'no data for the columns {col}')
            pass
        else:
            plt.figure(figsize=(10, 6))
            
            # Only keep the first 1000 values
            count_to_plot = count.head(1000)  # Get the top 1000 counts
    
            sns.barplot(x=count_to_plot.index, y=count_to_plot.values, palette="viridis")
            
            # Draw a horizontal line for total number of rows
            total_rows = sum(count.values)
            plt.axhline(total_rows, color='red', linestyle='--', label='Total Rows')
            
            plt.title(f'{df_name} - {col} Value Counts (Top 1000)')
            plt.xlabel(col)
            plt.ylabel('Count')
            plt.xticks(rotation=45)
            plt.legend()
            plt.tight_layout()
            plt.show()

# Plotting the counts for each DataFrame
plot_counts(df1_counts, 'df1')
plot_counts(df2_counts, 'df2')
plot_counts(df3_counts, 'df3')
                         