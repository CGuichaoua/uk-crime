# %%
import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, Boolean, DateTime
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime



# Connexion à MariaDB via SQLAlchemy
def create_connection(host_name, user_name, user_password, db_name=None, port=3306):
    try:
        db_url = f"mysql+pymysql://{user_name}:{user_password}@{host_name}:{port}/{db_name}"
        engine = create_engine(db_url, pool_recycle=3600)
        connection = engine.connect()
        print(f"Connexion réussie à la base de données {db_name}" if db_name else "Connexion réussie au serveur MariaDB")
        return connection, engine
    except SQLAlchemyError as e:
        print(f"Erreur : '{e}'")
        return None, None



# %%
def creer_table_si_absente(connection, engine, nom_table, colonnes, dtype):
    """
    Crée une table si elle n'existe pas déjà dans la base de données.
    """
    metadata = MetaData()
    colonnes_avec_types = []

    for colonne in colonnes:
        col_type = dtype.get(colonne, String(255))
        if col_type == 'int':
            col_type = Integer
        elif col_type == 'float':
            col_type = Float
        elif col_type == 'bool':
            col_type = Boolean
        elif col_type == 'datetime':
            col_type = DateTime
        else:
            col_type = String(255)
        colonnes_avec_types.append(Column(colonne, col_type))

    table = Table(nom_table, metadata, *colonnes_avec_types)
    metadata.create_all(engine)

# %%
def inserer_donnees(connection, nom_table, colonnes, donnees):
    """
    Insère les données dans la table MariaDB via SQLAlchemy.
    """
    metadata = MetaData()
    table = Table(nom_table, metadata, autoload_with=connection)
    
    # Convertir les données en une liste de dictionnaires
    data_dicts = [dict(zip(colonnes, row)) for row in donnees]
    
    try:
        connection.execute(table.insert(), data_dicts)
        connection.commit()
        print(f"{len(donnees)} lignes insérées avec succès dans la table {nom_table}")

    except SQLAlchemyError as e:
        print(f"Erreur lors de l'insertion : '{e}'")


def inserer_donnees_chunky(connection, nom_table, colonnes, donnees, batch_size=100000):
    """
    Insère les données dans la table MariaDB via SQLAlchemy par lots.
    
    Args:
    connection: La connexion SQLAlchemy.
    nom_table (str): Le nom de la table dans laquelle insérer les données.
    colonnes (list): La liste des colonnes de la table.
    donnees (pd.DataFrame): Le DataFrame contenant les données à insérer.
    batch_size (int): La taille des lots pour l'insertion.
    """
    metadata = MetaData()
    table = Table(nom_table, metadata, autoload_with=connection)
    
    # Convertir les données en une liste de dictionnaires
    data_dicts = [dict(zip(colonnes, row)) for row in donnees.itertuples(index=False, name=None)]
    
    try:
        for i in range(0, len(data_dicts), batch_size):
            batch = data_dicts[i:i + batch_size]
            print(f"Insertion du batch {i // batch_size + 1} contenant {len(batch)} lignes...")
            connection.execute(table.insert(), batch)
            connection.commit()
            print(f"{len(batch)} lignes insérées avec succès dans la table {nom_table} (batch {i // batch_size + 1})")

    except SQLAlchemyError as e:
        print(f"Erreur lors de l'insertion : '{e}'")

# %%
def extraire_info_du_nom_fichier(fichier,liste_nom_table):  
    # extraction informations du nom de fichier à encoder dans la table
    annee_mois=fichier[:7]
    info_geo=fichier[8:-4]
    # where sans l'élément qui est dans la liste
    for i in liste_nom_table:
        if i in info_geo:
            info_geo=info_geo.replace(i,"")[:-1]
    return annee_mois,info_geo

# %%
def nettoyer_noms_colonnes(colonnes):
    """
    Nettoie les noms des colonnes pour éviter les problèmes de requêtes SQL.
    """
     # nettoyage des noms de colonnes
    colonnes = [colonne.replace(' ', '') for colonne in colonnes]
    colonnes = [colonne.replace('-', '') for colonne in colonnes]
    colonnes = [colonne.replace('_', '') for colonne in colonnes]
    colonnes_nettoyees = [colonne.replace(' ', '').replace('-', '').replace('_', '') for colonne in colonnes]
    return colonnes_nettoyees

# %%
def nettoyer_nom_table(liste_nom_table,chemin_fichier):
    for nom_table in liste_nom_table:
        if nom_table in chemin_fichier:
            nom_table = nom_table.replace('-', '')
            break
        else:
            nom_table = 'autre'
            
    print(f"nom de la table : {nom_table}")
    # Créer la table si elle n'existe pas
    return nom_table


# %%
def nettoyer_donnees(donnees):
    """
    Remplace les NaN dans les données par des valeurs par défaut.
    Pour les chaînes, remplace par une chaîne vide, pour les numériques, par NULL.
    """

    # extraire les doublons et supprimer les doublons - remplacer les nan
    duplicates=donnees[donnees.duplicated()]
    duplicates=duplicates.replace({np.nan:None})
    
    donnees=donnees.drop_duplicates()
    donnees = donnees.replace({np.nan: None})
        
    return donnees, duplicates


# %%
def rajouter_un_index_table(nom_table,index_dict,df):
     # Ajouter une colonne d'index unique dans les tables
    if nom_table not in index_dict:
        index_dict[nom_table] = 1
    df['id'] = range(index_dict[nom_table], index_dict[nom_table] + len(df))
    index_dict[nom_table] += len(df)
    return df,index_dict


# %%
def traiter_fichier(connection,chemin_fichier, fichier,liste_nom_table,dtype, index_dict,liste_col_a_supprimer,dict_nom_fichier):
    """
    Traite un fichier CSV en créant une table correspondante et en y insérant les données.
    """
    # Gérer les valeurs manquantes
    na_values = ['NA', 'N/A', '']
        # Lire le fichier CSV
    parse_dates = [col for col, typ in dtype.items() if typ == 'datetime']
    df = pd.read_csv(chemin_fichier, dtype={col: typ for col, typ in dtype.items() if typ != 'datetime'},na_values=na_values)

    # supprimer les colonnes de la liste 'liste_col_a_supprimer'
    df = df.drop(columns=[col for col in liste_col_a_supprimer if col in df.columns])
 
    # rajout des informations year_month et where dans le dataframe
    annee_mois,info_geo = extraire_info_du_nom_fichier(fichier,liste_nom_table)
    df['annee_mois'] = annee_mois
    new_info_geo=info_geo
    # test si dict_nom_fichier[info_geo] existe
    if info_geo in dict_nom_fichier:
        new_info_geo=dict_nom_fichier[info_geo]
    df['info_geo'] = new_info_geo

    # nettoyer les données
    df, duplicates = nettoyer_donnees(df)
    duplicates['source']=fichier

    nom_table = nettoyer_nom_table(liste_nom_table,chemin_fichier)+"_temp"
    nom_table_duplicates=nom_table+"_duplicates"

    # calculer l'index de table, insérer les données dans la table
    df,index_dict=rajouter_un_index_table(nom_table,index_dict,df)
    colonnes = nettoyer_noms_colonnes(list(df.columns))
    creer_table_si_absente(connection, engine, nom_table, colonnes, dtype)
    inserer_donnees_chunky(connection,nom_table, colonnes, df)
  
    duplicates,index_dict=rajouter_un_index_table(nom_table_duplicates,index_dict,duplicates)
    colonnes_duplicates=nettoyer_noms_colonnes(list(duplicates.columns))
    creer_table_si_absente(connection, engine, nom_table_duplicates, colonnes_duplicates, dtype)
    inserer_donnees_chunky(connection,nom_table_duplicates, colonnes_duplicates, duplicates)
  
    print(f"Traitement du fichier : {chemin_fichier} terminé.")

    return index_dict

# %%
def parcourir_arborescence(connection,engine,chemin_racine, db_path,liste_nom_table,dtype,index_dict,liste_col_a_supprimer,dict_nom_fichier, filt=None):
    """
    Parcourt récursivement l'arborescence et traite chaque fichier CSV trouvé.
    """
    
    for racine, sous_repertoires, fichiers in os.walk(chemin_racine):
         for fichier in fichiers:
            if filt is not None and not filt(fichier):
                continue
            if fichier.endswith(".csv"):
                chemin_fichier = os.path.join(racine, fichier)
                index_dict=traiter_fichier(connection,chemin_fichier, fichier,liste_nom_table,dtype, index_dict,liste_col_a_supprimer,dict_nom_fichier)
    return index_dict

# %%
if __name__ == '__main__':
    start_time = datetime.now()
    
    short_import = False # Vrai si on fait un import partiel

    #chemin_racine = "Crimes au Royaume-Uni"
    chemin_racine = "C:/Users/HP/Documents/ICAM/chunky"

    db_name = "crime" + ('_short' if short_import else "") 
    test = True
    if test:
        db_name += '_test'
    print(f"Nom de la base de données : {db_name}")
    # on définit le nom des tables en fonction du nom du fichier (terminaison)
    liste_nom_table = ['outcomes','stop-and-search','street']

    # encodage des types en fonction de la colonne
    dtype={'Longitude': 'float',
            'Latitude': 'float',
            'id':'int',
            'Partofapolicingoperation': 'bool',
            'Date': 'datetime',
            'Outcomelinkedtoobjectofsearch': 'bool',
            'Removalofmorethanjustouterclothing': 'bool'
    }

    liste_col_a_supprimer=['Falls within']

    dict_nom_fichier={'avon-and-somerset': 'Avon and Somerset Constabulary',
 'bedfordshire': 'Bedfordshire Police',
 'cambridgeshire': 'Cambridgeshire Constabulary',
 'cheshire': 'Cheshire Constabulary',
 'city-of-london': 'City of London Police',
 'cleveland': 'Cleveland Police',
 'cumbria': 'Cumbria Constabulary',
 'derbyshire': 'Derbyshire Constabulary',
 'devon-and-cornwall': 'Devon & Cornwall Police',
 'dorset': 'Dorset Police',
 'durham': 'Durham Constabulary',
 'dyfed-powys': 'Dyfed-Powys Police',
 'essex': 'Essex Police',
 'gloucestershire': 'Gloucestershire Constabulary',
 'gwent': 'Gwent Police',
 'hampshire': 'Hampshire Constabulary',
 'hertfordshire': 'Hertfordshire Constabulary',
 'humberside': 'Humberside Police',
 'kent': 'Kent Police',
 'lancashire': 'Lancashire Constabulary',
 'leicestershire': 'Leicestershire Police',
 'lincolnshire': 'Lincolnshire Police',
 'merseyside': 'Merseyside Police',
 'metropolitan': 'Metropolitan Police Service',
 'norfolk': 'Norfolk Constabulary',
 'north-wales': 'North Wales Police',
 'north-yorkshire': 'North Yorkshire Police',
 'northamptonshire': 'Northamptonshire Police',
 'northumbria': 'Northumbria Police',
 'nottinghamshire': 'Nottinghamshire Police',
 'south-wales': 'South Wales Police',
 'south-yorkshire': 'South Yorkshire Police',
 'staffordshire': 'Staffordshire Police',
 'suffolk': 'Suffolk Constabulary',
 'surrey': 'Surrey Police',
 'sussex': 'Sussex Police',
 'thames-valley': 'Thames Valley Police',
 'warwickshire': 'Warwickshire Police',
 'west-mercia': 'West Mercia Police',
 'west-midlands': 'West Midlands Police',
 'west-yorkshire': 'West Yorkshire Police',
 'wiltshire': 'Wiltshire Police',
 'btp': 'British Transport Police',
 'northern-ireland': 'Police Service of Northern Ireland'}
    
    filt = (lambda s: "2019" in s) if short_import else None

    # Dictionnaire pour mémoriser l'index pour chaque table
    index_dict = {}

    # Connexion à la base de données 'crime'
    connection, engine = create_connection("127.0.0.1", "root", "", db_name)

    if connection:
        
        index_dict=parcourir_arborescence(connection,engine,chemin_racine, db_name,liste_nom_table,dtype,
                                              index_dict,liste_col_a_supprimer, dict_nom_fichier, filt=filt)
        # Fermeture de la connexion
        if connection:
            connection.close()
            print("Connexion MariaDB fermée")
    

    # %%
    print(index_dict)
    end_time = datetime.now()
    print(f"Durée d'exécution : {end_time - start_time}")
    # %%



