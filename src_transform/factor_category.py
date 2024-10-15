import pandas as pd
import sqlalchemy


def get_labels(column, table, engine):
    """
    Récupère les labels des catégories de la colonne à factoriser.
    """
    query = f'SELECT DISTINCT `{column}` FROM `{table}`'
    return pd.read_sql_query(query, engine)[column]


def connect_maria(db_name):
    """
    Connecte une base de données MariaDB via SQLalchemy
    """
    user="root"
    pwd=""
    host="localhost"
    port="3306"
    return sqlalchemy.create_engine(
        f"mariadb+mariadbconnector://{user}:{pwd}@{host}:{port}/{db_name}")

def create_labels_table(table_name:str, labels:pd.Series, engine:sqlalchemy.Engine) -> None:
    """
    Ecrit la table de correspondance entre les id des catégories et leurs libellés dans la BDD.
    """
    labels.index = labels.index.rename("id")
    labels.to_sql(table_name, engine, dtype={"id": sqlalchemy.types.Integer})

def make_reverse_index(labels:pd.Series):
    """
    Renvoie une lambda qui mappe chaque libellé à son index.
    """
    reverse_index = {label: idx for idx, label in labels.items()}
    return lambda label: reverse_index[label]

def replace_categorical_column(column_name:str, table:str, labels:pd.Series, engine:sqlalchemy.Engine, suffix:str="_"):
    """
    Met à jour une table pour utiliser les identifiants de catégorie à la place des libellés, avec un suffixe optionnel.
    """
    reverse_index = make_reverse_index(labels)
    df = pd.read_sql_table(table, engine)
    df[column_name] = df[column_name].map(reverse_index)
    df.to_sql(table+suffix, engine, if_exists='replace', dtype={column_name:sqlalchemy.types.Integer})