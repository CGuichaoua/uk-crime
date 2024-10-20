import sqlite3


# Fonction de connexion à SQLite
def create_connection(db_file: str) -> sqlite3.Connection:
    try:
        connection = sqlite3.connect(db_file, timeout=10)
        print(f"Connexion réussie à la base de données {db_file}")
        return connection
    except sqlite3.Error as e:
        print(f"Erreur : '{e}'")
        return None
