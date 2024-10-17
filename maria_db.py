import os
import pandas as pd
import numpy as np
import mariadb


# %%
def get_connection_and_cursor(db_name="crime"):
    """
    Crée une connection et un curseur vers la base MariaDB locale
    """
    conn = mariadb.connect(user="root", password="", host="localhost", database=db_name)
    cur = conn.cursor()
    return conn, cur


# %%
def creer_table_si_absente(cursor, nom_table, colonnes):
    """
    Crée une table dans la base de données si elle n'existe pas déjà,
    en utilisant les colonnes extraites de l'en-tête du fichier CSV.
    """
    colonnes_avec_types = ", ".join([f"{colonne} TEXT" for colonne in colonnes])
    requete_creation = (
        f"CREATE TABLE IF NOT EXISTS {nom_table} ({colonnes_avec_types});"
    )
    cursor.execute(requete_creation)


# %%
def inserer_donnees(cursor, nom_table, colonnes, donnees):
    """
    Insère les données dans la table.
    """
    placeholders = ", ".join(["?" for _ in colonnes])
    colonnes_str = ", ".join(colonnes)
    requete_insertion = (
        f"INSERT INTO {nom_table} ({colonnes_str}) VALUES ({placeholders});"
    )
    # Insert data in smaller chunks
    CHUNK_SIZE = 1000
    for i in range(0, len(donnees), CHUNK_SIZE):
        chunk = donnees[i : i + CHUNK_SIZE]
        try:
            for line in chunk:
                cursor.execute(requete_insertion, line)
            # Fails for mysterious reasons
            # cursor.executemany(requete_insertion, chunk)
        except mariadb.ProgrammingError as e:
            print(f"Error during bulk insert: {e}")
            print(f"Data chunk: {chunk[:5]}")  # Print the first few rows for debugging


# %%
def traiter_fichier(chemin_fichier, connection):
    """
    Traite un fichier CSV en créant une table correspondante et en y insérant les données.
    """
    # nom_table = os.path.splitext(os.path.basename(chemin_fichier))[0]  # Utilise le nom du fichier pour la table
    df = pd.read_csv(chemin_fichier, sep=",")  # Charge le fichier CSV avec pandas
    colonnes = list(df.columns)  # Remplace les espaces par des underscores
    colonnes = [colonne.replace(" ", "") for colonne in colonnes]
    colonnes = [colonne.replace("-", "") for colonne in colonnes]
    colonnes = [colonne.replace("_", "") for colonne in colonnes]
    cursor = connection.cursor()

    df = df.replace([np.nan], [None])

    # définir le nom de la table en fonction de l'en-tête du fichier
    liste_nom_table = ["outcomes", "stop-and-search", "street"]
    for nom_table in liste_nom_table:
        if nom_table in chemin_fichier:
            nom_table = nom_table.replace("-", "")
            break
    else:
        nom_table = "autre"

    print(f"nom de la table : {nom_table}")
    # Créer la table si elle n'existe pas
    creer_table_si_absente(cursor, nom_table, colonnes)

    # Insérer les données dans la table
    inserer_donnees(cursor, nom_table, colonnes, df.values.tolist())

    # Commit les changements
    connection.commit()

    print(f"Traitement du fichier : {chemin_fichier} terminé.")


# %%
def parcourir_arborescence(chemin_racine, db_connector):
    """
    Parcourt récursivement l'arborescence et traite chaque fichier CSV trouvé.
    """
    print(f"Parcours de l'arborescence à partir de : {chemin_racine}")

    for racine, sous_repertoires, fichiers in os.walk(chemin_racine):
        print(f"Répertoire courant : {racine}")
        for sous_repertoire in sous_repertoires:
            print(f"Répertoire trouvé : {sous_repertoire}")
        for fichier in fichiers:
            if fichier.endswith(".csv"):
                chemin_fichier = os.path.join(racine, fichier)
                traiter_fichier(chemin_fichier, db_connector)


if __name__ == "__main__":
    # TODO: read from command line or config file
    chemin_racine = "Crimes au Royaume-Uni"
    db_name = "crime"
    db_connector, _ = get_connection_and_cursor(db_name)
    parcourir_arborescence(chemin_racine, db_connector)
