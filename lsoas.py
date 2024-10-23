# -*- coding: utf-8 -*-
"""
Created on Tue Oct 15 12:28:09 2024

@author: Admin.local
"""

import os
import pandas as pd

folder='C:/Users/ianni/Desktop/projet/Crimes au Royaume-Uni-20241014T124028Z-001/Crimes au Royaume-Uni'
# folder='C:/Users/Admin.local/Desktop/projet integration/Crimes au Royaume-Uni-20241015T094109Z-001/Crimes au Royaume-Uni'

# =============================================================================
# old_lsoa_folder='C:/Users/Admin.local/Desktop/projet integration/Lower_Layer_Super_Output_Areas_(December_2011)_Names_and_Codes_in_England_and_Wales.csv'
# new_lsoa_folder='C:/Users/Admin.local/Desktop/projet integration/LSOA_DEC_2021_EW_NC_v3_8389821430447047524.csv'
# df_lsoa_old=pd.read_csv(old_lsoa_folder)
# df_lsoa_new=pd.read_csv(new_lsoa_folder)
# =============================================================================




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


# =============================================================================
# # tentative de faire un matching "manuel" des anciens et nouveaux lsoas
# df_lsoa_old=df_lsoa_old.sort_values(by='LSOA11NM',reset_index=True)
# df_lsoa_new=df_lsoa_new.sort_values(by='LSOA21NM',reset_index=True)
# 
# df_lsoa_old=df_lsoa_old.reset_index(drop=True)
# df_lsoa_new=df_lsoa_new.reset_index(drop=True)
# 
# 
# 
# result = pd.merge(df_lsoa_old,df_lsoa_new, left_on='LSOA11CD', right_on='LSOA21CD',how='outer')
# result.drop(columns='LSOA21NMW',inplace=True)
# rez=result[result.isnull().any(axis=1)]
# 
# rez['lsoa11']=rez['LSOA11NM'].str[:-5]
# rez['lsoa11_code']=rez['LSOA11NM'].str[-4:]
# 
# rez['lsoa21']=rez['LSOA21NM'].str[:-5]
# rez['lsoa21_code']=rez['LSOA21NM'].str[-4:]
# 
# =============================================================================




# folder_change_lsoas='C:/Users/Admin.local/Desktop/projet integration/LSOA_(2011)_to_LSOA_(2021)_to_Local_Authority_District_(2022)_Best_Fit_Lookup_for_EW_(V2).csv'
folder_change_lsoas='C:/Users/ianni/Desktop/projet/Crimes au Royaume-Uni-20241014T124028Z-001/LSOA_(2011)_to_LSOA_(2021)_to_Local_Authority_District_(2022)_Best_Fit_Lookup_for_EW_(V2).csv'
data=pd.read_csv(folder_change_lsoas)
data=data[['LSOA11CD','LSOA11NM','LSOA21CD','LSOA21NM']] 
#garder les columns qui nous intéresse, code & name avant et après 2021
data2=data.copy()
# =============================================================================
# # utile pour savoir si seulement un des deux champs change
# data['change']=data['LSOA11CD']!=data['LSOA21CD']
# data['change2']=data['LSOA11NM']!=data['LSOA21NM']
# data['change3']=data['change2']!=data['change']
# data=data[data['change3']==True]
# =============================================================================

aa=df1.iloc[0000000:4600000]
bb=df1.iloc[5000000:]
# coupure avant 2021 (2020-12) et après 2021 (2021-01)

aa=aa.dropna(subset=['LSOA code'])
bb=bb.dropna(subset=['LSOA code'])
# suppression des cas sans lsoa code, qui ne peuvent donc pas être modifié

a=aa.merge(data2, left_on=['LSOA code','LSOA name'],right_on=['LSOA11CD','LSOA11NM'],how='left')
aaa=a[a['LSOA11CD']!=a['LSOA21CD']]
b=bb.merge(data2, left_on=['LSOA code','LSOA name'],right_on=['LSOA11CD','LSOA11NM'],how='left')
bbb=b[b['LSOA11CD']!=b['LSOA21CD']]
# application du changement de nomenclature lsoa

print(len(aaa)/len(aa),len(bbb)/len(bb))
# du au ratio sensiblement similaire de changement de données via le changement de nomenclature ~7.5%
# on le garde dans un tiroir pour le moment



# =============================================================================
# data3=data[data['LSOA11CD']!=data['LSOA21CD']]
# print(len(data3)/len(data2))
# # ~3% de changement de nomenclature sur le code lsoa
# data4=data[(data['LSOA11CD']==data['LSOA21CD']) & (data['LSOA11NM']==data['LSOA21NM'])]
# print(len(data4)/len(data2))
# # ~9% de changement de nomenclature total (il n'y a qu'une seule occurence de moins si on check uniquement le name lsoa)
# =============================================================================



# =============================================================================
# a=aa.merge(data2, left_on=['LSOA code','LSOA name'],right_on=['LSOA21CD','LSOA21NM'],how='left')
# aaa=a[a['LSOA11CD']!=a['LSOA21CD']]
# b=bb.merge(data2, left_on=['LSOA code','LSOA name'],right_on=['LSOA21CD','LSOA21NM'],how='left')
# bbb=b[b['LSOA11CD']!=b['LSOA21CD']]
# # IDENTIFIER CE QU'IL SE PASSE EXACTEMENT SUR LES ~2000 qui roll back
# =============================================================================


def calcul_ratio_changement_nomenclature(df1):
    old_outcomes=df1.iloc[0000000:4600000]
    new_outcomes=df1.iloc[5000000:]
    
    old_outcomes=old_outcomes.dropna(subset=['LSOA code'])
    new_outcomes=new_outcomes.dropna(subset=['LSOA code'])
    
    old_outcomes=old_outcomes.merge(data,left_on=['LSOA code','LSOA name'],right_on=['LSOA11CD','LSOA11NM'],how='left')
    new_outcomes=new_outcomes.merge(data,left_on=['LSOA code','LSOA name'],right_on=['LSOA11CD','LSOA11NM'],how='left')
    old_outcomes.dropna(subset=['LSOA11CD'],inplace=True)
    print('ratio de changement sur les vieilles valeurs old->new nomenclature')
    print(len(old_outcomes[old_outcomes['LSOA11CD']!=old_outcomes['LSOA21CD']])/len(old_outcomes))
    new_outcomes.dropna(subset=['LSOA11CD'],inplace=True)
    print('ratio de changement sur les nouvelles valeurs old->new nomenclature')
    print(len(new_outcomes[new_outcomes['LSOA11CD']!=new_outcomes['LSOA21CD']])/len(new_outcomes))
    
    
    old_outcomes=df1.iloc[0000000:4600000]
    new_outcomes=df1.iloc[5000000:]
    
    old_outcomes=old_outcomes.dropna(subset=['LSOA code'])
    new_outcomes=new_outcomes.dropna(subset=['LSOA code'])
    
    old_outcomes=old_outcomes.merge(data, left_on=['LSOA code','LSOA name'],right_on=['LSOA21CD','LSOA21NM'],how='left')
    old_outcomes.dropna(subset=['LSOA11CD'],inplace=True)
    print('ratio de changement sur les vieilles valeurs new->old nomenclature')
    print(len(old_outcomes[old_outcomes['LSOA11CD']!=old_outcomes['LSOA21CD']])/len(old_outcomes))
    new_outcomes=new_outcomes.merge(data, left_on=['LSOA code','LSOA name'],right_on=['LSOA21CD','LSOA21NM'],how='left')
    new_outcomes.dropna(subset=['LSOA11CD'],inplace=True)
    print('ratio de changement sur les nouvelles valeurs new->old nomenclature')
    print(len(new_outcomes[new_outcomes['LSOA11CD']!=new_outcomes['LSOA21CD']])/len(new_outcomes))







