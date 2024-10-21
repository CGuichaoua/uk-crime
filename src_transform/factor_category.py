import pandas as pd
import sqlalchemy
from collections.abc import Iterable
from collections.abc import Callable

def get_labels(column:str, tables:Iterable[str], engine:sqlalchemy.Engine):
    """
    Récupère les labels des catégories de la colonne à factoriser.
    """
    subqueries = [f'SELECT DISTINCT `{column}` FROM `{table}` WHERE `{column}` IS NOT NULL' for table in tables]
    query = "\nUNION\n".join(subqueries)
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
    labels.to_sql(table_name, engine, dtype={"id": sqlalchemy.types.Integer}, if_exists='fail')

def make_reverse_index(labels:pd.Series):
    """
    Renvoie une lambda qui mappe chaque libellé à son index.
    """
    reverse_index = {label.lower(): idx for idx, label in labels.items() if label is not None}
    return lambda label: None if label is None else reverse_index[label.lower()]


def replace_categorical_columns(table:str, label_maps:dict[str, Callable[[str],int]], engine:sqlalchemy.Engine):
    """
    Met à jour une table pour utiliser les identifiants de catégorie à la place des libellés, avec un suffixe optionnel.
    """
    df = pd.read_sql_table(table, engine)
    types = {col['name']:col['type'] 
             for col in sqlalchemy.inspect(engine).get_columns(table)
    }
    for column_name, reverse_index in label_maps.items():
        df[column_name] = df[column_name].map(reverse_index)
        types[column_name] = sqlalchemy.types.Integer
    
    df.to_sql(table+suffix, engine, if_exists='replace', dtype=types, chunksize=100000, index=False)

def factor_categories(column_names, table_names, engine,
                      column_to_table_name:Callable[[str], str]=lambda x:x):
    """
    Sépare plusieurs colonnes présentes dans les même tables en autant de tables de référence et remplace les valeurs par des identifiants.
    """
    label_maps = {}
    for column_name in column_names:
        labels = get_labels(column_name, table_names, engine)
        create_labels_table(column_to_table_name(column_name), labels, engine)
        label_maps[column_name] = make_reverse_index(labels)
    
    for table_name in table_names:
        replace_categorical_columns(table_name, label_maps, engine)


#%%
if __name__ == "__main__":
    engine = connect_maria("crime_short_test")
    print("Connected to DB")
    category_columns = [
        (("outcomes_temp", "street_temp"), [
            "Reportedby",
            "Location",
        ]),
        (("stopandsearch_temp",), [
            "Type",
            "Policingoperation",
            "Agerange",
            "Selfdefinedethnicity",
            "Officerdefinedethnicity",
            "Legislation",
            "Objectofsearch",
            "Outcome",
            "Gender",
        ]),
        (("outcomes_temp_",), ["Outcometype"]),
        (("street_temp_",),[
            "Crimetype",
            "Lastoutcomecategory",
        ]
        
    }
    for tables, column_names in category_columns.items():
        factor_categories(column_names, tables, engine,
            lambda x:x.lower()+'_ref')