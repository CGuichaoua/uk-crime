import pandas as pd
import sqlalchemy
from unidecode import unidecode
from collections.abc import Iterable
from collections.abc import Callable

def standardize_string(s: str):
    return unidecode(s).lower()

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
    with engine.connect() as conn:
        conn.execute(sqlalchemy.text(f"ALTER TABLE `{table_name}` ADD PRIMARY KEY (`{labels.index.name}`);"))
        conn.execute(sqlalchemy.text(f"ALTER TABLE `{table_name}` ADD UNIQUE (`{labels.name}`);"))

def make_reverse_index(labels:pd.Series):
    """
    Renvoie une lambda qui mappe chaque libellé à son index.
    """
    reverse_index = {standardize_string(label): idx for idx, label in labels.items() if label is not None}
    return lambda label: None if label is None else reverse_index[standardize_string(label)]


def replace_categorical_columns(table:str, label_maps:dict[str, Callable[[str],int]], engine:sqlalchemy.Engine,
                                suffix:str='_') -> str:
    """
    Met à jour une table pour utiliser les identifiants de catégorie à la place des libellés, avec un suffixe optionnel. Renvoie le nom de la table créée
    """
    df = pd.read_sql_table(table, engine)
    types = {col['name']:col['type'] 
             for col in sqlalchemy.inspect(engine).get_columns(table)
    }
    for column_name, reverse_index in label_maps.items():
        df[column_name] = df[column_name].map(reverse_index)
        types[column_name] = sqlalchemy.types.Integer
    new_table_name = table if table.endswith(suffix) else table+suffix
    df.to_sql(new_table_name, engine, if_exists='replace', dtype=types, chunksize=100000, index=False)
    return new_table_name


def factor_categories(column_names, table_names, engine,
                      column_to_table_name:Callable[[str], str]=lambda x:x,
                      verbose=False):
    """
    Sépare plusieurs colonnes présentes dans les même tables en autant de tables de référence et remplace les valeurs par des identifiants.
    """
    label_maps = {}
    for column_name in column_names:
        reference_table_name = column_to_table_name(column_name)
        if verbose:
            print(f"Building reference table {reference_table_name} for {column_name}")
        if sqlalchemy.inspect(engine).has_table(reference_table_name):
            if verbose:
                print(f"Found table for {reference_table_name}, skipping build and reading instead")
            labels = pd.read_sql_table(reference_table_name, engine)
            labels = labels[labels.columns[1]]  # Récupérer la Series depuis la DataFrame
        else:
            labels = get_labels(column_name, table_names, engine)
            create_labels_table(reference_table_name, labels, engine)
        label_maps[column_name] = make_reverse_index(labels)
    
    for table_name in table_names:
        new_table_name = replace_categorical_columns(table_name, label_maps, engine)


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
        ])
        
    ]
    for tables, column_names in category_columns:
        factor_categories(column_names, tables, engine,
            lambda x:x.lower()+'_ref')