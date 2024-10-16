import pandas as pd
import sqlalchemy
from collections.abc import Iterable
from collections.abc import Callable

def get_labels(column:str, table:str, engine:sqlalchemy.Engine):
    """
    Récupère les labels des catégories de la colonne à factoriser.
    """
    query = f'SELECT DISTINCT `{column}` FROM `{table}`'
    return pd.read_sql_query(query, engine)[column]


def connect_maria(db_name:str):
    """
    Connecte une base de données MariaDB via SQLalchemy
    """
    user="root"
    pwd=""
    host="localhost"
    port="3306"
    return sqlalchemy.create_engine(
        f"mariadb+pymysql://{user}:{pwd}@{host}:{port}/{db_name}")

def create_labels_table(table_name:str, labels:pd.Series, engine:sqlalchemy.Engine) -> None:
    """
    Ecrit la table de correspondance entre les id des catégories et leurs libellés dans la BDD.
    """
    labels.index = labels.index.rename("id")
    labels.to_sql(table_name, engine, dtype={"id": sqlalchemy.types.Integer}, if_exists='replace')

def make_reverse_index(labels:pd.Series):
    """
    Renvoie une lambda qui mappe chaque libellé à son index.
    """
    reverse_index = {label: idx for idx, label in labels.items()}
    return lambda label: reverse_index[label]


def replace_categorical_columns(table:str, label_sets:dict[str, pd.Series], engine:sqlalchemy.Engine, suffix:str="_"):
    """
    Met à jour une table pour utiliser les identifiants de catégorie à la place des libellés, avec un suffixe optionnel.
    """
    df = pd.read_sql_table(table, engine)
    
    for column_name, labels in label_sets.items():
        reverse_index = make_reverse_index(labels)
        df[column_name] = df[column_name].map(reverse_index)
    
    df.to_sql(table+suffix, engine, if_exists='replace', dtype={column_name:sqlalchemy.types.Integer}, chunksize=100000)

def factor_categories(column_names:Iterable[str], table_name:str, engine:sqlalchemy.Engine,
                       column_to_table_name:Callable[[str], str]=lambda x:x):
    """
    Sépare plusieurs colonnes catégorielles en autant de tables de référence et une colonne d'identifiants.
    """
    label_sets = {}
    for column_name in column_names:
        labels = get_labels(column_name, table_name, engine)
        create_labels_table(column_to_table_name(column_name), labels, engine)
        label_sets[column_name] = labels

    replace_categorical_columns(table_name, label_sets, engine)


if __name__ == "__main__":
    engine = connect_maria("crime-sample")
    factor_categories(["Age range", "Selfdefinedethnicity"], "stopandsearch", engine,
        lambda x:x.lower()+'s')