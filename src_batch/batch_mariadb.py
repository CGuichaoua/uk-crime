# %%
import os
import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import Error

# %%
# Connexion à MariaDB
def create_connection(host_name, user_name, user_password, db_name=None, port=3306):
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            password=user_password,
            database=db_name,
            port=port,
            connection_timeout=600  # 10 minutes de délai
        )
        print(f"Connexion réussie à la base de données {db_name}" if db_name else "Connexion réussie au serveur MariaDB")
        return connection
    except Error as e:
        print(f"Erreur : '{e}'")
        return None



# %%
def creer_table_si_absente(connection, nom_table, colonnes, dtype):
    """
    Crée une table dans MariaDB si elle n'existe pas déjà,
    en utilisant les colonnes extraites et leurs types.
    """
    colonnes_avec_types = []
    
    for colonne in colonnes:
        # On récupère le type depuis dtype ou on met 'VARCHAR(255)' par défaut
        type_colonne = dtype.get(colonne, 'VARCHAR(255)')
        
        # Ajustement des types pour MariaDB
        if type_colonne == 'float':
            type_colonne = 'FLOAT'
        elif type_colonne == 'int':
            type_colonne = 'INT'
        elif type_colonne == 'bool':
            type_colonne = 'TINYINT(1)'
        elif type_colonne == 'datetime':
            type_colonne = 'DATETIME'
    
        
        colonnes_avec_types.append(f"{colonne} {type_colonne}")
    
    # Génère la requête SQL avec les colonnes et leurs types
    colonnes_avec_types_str = ', '.join(colonnes_avec_types)
    
    # Requête pour créer la table si elle n'existe pas déjà
    requete_creation = f"CREATE TABLE IF NOT EXISTS {nom_table} ({colonnes_avec_types_str});"
    
    print(requete_creation)  # Pour déboguer, afficher la requête générée
    
    cursor=connection.cursor()
    try:
        cursor.execute(requete_creation)  # Exécuter la requête
        #connection.commit()
        print(f"Table '{nom_table}' créée avec succès (si absente)")
    except Error as e:
        print(f"Erreur lors de la création de la table : '{e}'")


# %%
def inserer_donnees(connection, nom_table, colonnes, donnees):
    """
    Insère les données dans la table MariaDB.
    """
    # Création des placeholders pour MariaDB (%s)
    placeholders = ', '.join(['%s' for _ in colonnes])
    # Génère une chaîne avec les noms de colonnes
    colonnes_str = ', '.join(colonnes)
    # Création de la requête d'insertion
    requete_insertion = f"INSERT INTO {nom_table} ({colonnes_str}) VALUES ({placeholders});"
    cursor=connection.cursor()
    try:
        # Exécution de l'insertion avec executemany
        cursor.executemany(requete_insertion, donnees)
        # Commit pour enregistrer les changements dans la base de données
        #connection.commit()
        print(f"{cursor.rowcount} lignes insérées avec succès dans la table {nom_table}")
    except Error as e:
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
def traiter_fichier(connection,chemin_fichier, fichier,liste_nom_table,dtype, index_dict,liste_col_a_supprimer):
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
    df['info_geo'] = info_geo

    # nettoyer les données
    df, duplicates = nettoyer_donnees(df)
    duplicates['source']=fichier

    nom_table = nettoyer_nom_table(liste_nom_table,chemin_fichier)+"_temp"
    nom_table_duplicates=nom_table+"_duplicates"

    # calculer l'index de table, insérer les données dans la table
    df,index_dict=rajouter_un_index_table(nom_table,index_dict,df)
    colonnes = nettoyer_noms_colonnes(list(df.columns))
    creer_table_si_absente(connection,nom_table, colonnes,dtype)
    inserer_donnees(connection,nom_table, colonnes, df.values.tolist())
  
    duplicates,index_dict=rajouter_un_index_table(nom_table_duplicates,index_dict,duplicates)
    colonnes_duplicates=nettoyer_noms_colonnes(list(duplicates.columns))
    creer_table_si_absente(connection,nom_table_duplicates, colonnes_duplicates,dtype)
    inserer_donnees(connection,nom_table_duplicates, colonnes_duplicates, duplicates.values.tolist())
  
    print(f"Traitement du fichier : {chemin_fichier} terminé.")

    return df, index_dict

# %%
def parcourir_arborescence(connection,chemin_racine, db_path,liste_nom_table,dtype,index_dict,liste_col_a_supprimer, filt=None):
    """
    Parcourt récursivement l'arborescence et traite chaque fichier CSV trouvé.
    """
    
    for racine, sous_repertoires, fichiers in os.walk(chemin_racine):
         for fichier in fichiers:
            if filt is not None and not filt(fichier):
                continue
            if fichier.endswith(".csv"):
                chemin_fichier = os.path.join(racine, fichier)
                df, index_dict=traiter_fichier(connection,chemin_fichier, fichier,liste_nom_table,dtype, index_dict,liste_col_a_supprimer)
    return df, index_dict

# %%
if __name__ == '__main__':
    short_import = True # Vrai si on fait un import partiel

    chemin_racine = "Crimes au Royaume-Uni"
    #chemin_racine = "C:/Users/Admin.local/Documents/projetint/files"

    db_name = "crime" + ('_short' if short_import else "") 
    test = False
    if test:
        db_name += '_test'

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

    filt = (lambda s: "2019" in s) if short_import else None

    # Dictionnaire pour mémoriser l'index pour chaque table
    index_dict = {}

    # Connexion à la base de données 'crime'
    connection = create_connection("127.0.0.1", "root", "", db_name)

    if connection:
        df, index_dict=parcourir_arborescence(connection,chemin_racine, db_name,liste_nom_table,dtype,
                                              index_dict,liste_col_a_supprimer, filt=filt)
        # Fermeture de la connexion
        if connection.is_connected():
            connection.commit()
            connection.close()
            print("Connexion MariaDB fermée")
    

    # %%
    print(index_dict)

    # %%



