from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

def create_connection(host:str, user:str, password:str, db_name:str, port:int=3306):
    """
    Crée une connexion à la base de données MariaDB en utilisant SQLAlchemy.
    
    Args:
    host (str): Adresse de l'hôte de la base de données.
    user (str): Nom d'utilisateur pour la base de données.
    password (str): Mot de passe pour la base de données.
    db_name (str): Nom de la base de données.
    port (int): Port de la base de données.
    
    Returns:
    session: Une session SQLAlchemy pour interagir avec la base de données.
    engine: Le moteur SQLAlchemy.
    """
    try:
        DATABASE_URL = f"mariadb+mariadbconnector://{user}:{password}@{host}:{port}/{db_name}"
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        conn=session.connection()
        print("Connexion réussie à la base de données MariaDB")
        return conn, engine
    except SQLAlchemyError as e:
        print(f"Erreur lors de la connexion à la base de données : {e}")
        return None, None
    
# Fonction de nettoyage pour fermer la connexion à la base de données
def close_connection(conn,engine):
    if conn:
        conn.close()
        print("Connexion à la base de données fermée")
    if engine:
        engine.dispose()
        print("Engine fermé")
