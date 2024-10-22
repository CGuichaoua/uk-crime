"""Code to factorize categorical labels out into a reference table."""
import math
from collections.abc import Iterable
from collections.abc import Callable
from unidecode import unidecode
import pandas as pd
import sqlalchemy
from sqlalchemy.dialects.mysql import TINYINT, SMALLINT, MEDIUMINT, INTEGER, BIGINT
from sqlalchemy.types import SchemaType
from sqlalchemy import Engine as SqlEngine

def standardize_string(s: str):
    """
    Convertit une chaine en sa version minuscule sans accents.
    """
    return unidecode(s).lower()

def get_labels(column:str, tables:Iterable[str], engine:SqlEngine):
    """
    Récupère les labels des catégories de la colonne à factoriser.
    """
    subqueries = [f'SELECT DISTINCT `{column}` FROM `{table}` \
                  WHERE `{column}` IS NOT NULL' for table in tables]
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


def min_int_size(nb_line:int, safety_factor:int=2):
    """
    Renvoie la taille de données appropriée au nombre de lignes passé.
    """
    sizes = [(TINYINT(unsigned=True), 8),
             (SMALLINT(unsigned=True), 16),
             (MEDIUMINT(unsigned=True), 24),
             (INTEGER(unsigned=True), 32),
             (BIGINT(unsigned=True), 64)
             ]
    log_len = math.log2(nb_line if nb_line != 0 else 1) + math.log(safety_factor)
    try:
        return next(type for (type, max_size) in sizes if max_size > log_len)
    except StopIteration as e:
        raise ValueError("Table too big to index") from e



def create_labels_table(table_name:str, labels:pd.Series, engine:SqlEngine) -> None:
    """
    Ecrit la table de correspondance entre les id des catégories et leurs libellés dans la BDD.
    Renvoie le type de donnée 
    """
    labels.index = labels.index.rename("id")
    dtype = min_int_size(len(labels), safety_factor=2)
    labels.to_sql(table_name, engine, dtype={"id": dtype}, if_exists='fail')
    with engine.connect() as conn:
        conn.execute(sqlalchemy.text(f"ALTER TABLE `{table_name}` \
                                     ADD PRIMARY KEY (`{labels.index.name}`);"))
        conn.execute(sqlalchemy.text(f"ALTER TABLE `{table_name}` \
                                     ADD UNIQUE (`{labels.name}`);"))
    return dtype

def make_reverse_index(labels:pd.Series):
    """
    Renvoie une lambda qui mappe chaque libellé à son index.
    """
    reverse_index = {standardize_string(label): idx
                     for idx, label in labels.items()
                     if label is not None}
    return lambda label: None if label is None else reverse_index[standardize_string(label)]


def restore_constraints(new_table:str, constraint_pk, constraints_fk, engine):
    """Restores the pre-existing constraints to the new table."""
    queries = [f"ALTER TABLE `{new_table}` ADD PRIMARY KEY \
               ({", ".join(constraint_pk["constrained_columns"])}) "]
    queries.extend(
        f"ALTER TABLE `{new_table}` \
            ADD FOREIGN KEY ({", ".join(foreign_key['constrained_columns'])})\
            REFERENCES {foreign_key['referred_table']}\
            ({', '.join(foreign_key['referred_columns'])})"
        for foreign_key in constraints_fk)
    with engine.connect() as conn:
        for query in queries:
            conn.execute(sqlalchemy.text(query))

def replace_categorical_columns(old_table:str,
                                label_maps:dict[str, tuple[Callable[[str],int], SchemaType]],
                                engine:SqlEngine, suffix:str='_') -> str:
    """
    Met à jour une table pour utiliser les identifiants de catégorie à la place des libellés.
    Renvoie le nom de la table créée, avec un suffixe optionnel, ignoré s'il est déjà présent
    """
    inspector = sqlalchemy.inspect(engine)
    df = pd.read_sql_table(old_table, engine)
    dtypes = {col['name']:col['type']
             for col in inspector.get_columns(old_table)
    }
    constraint_pk = inspector.get_pk_constraint(old_table)
    constraints_fk = inspector.get_foreign_keys(old_table)
    for column_name, (reverse_index, dtype) in label_maps.items():
        if dtypes[column_name] == sqlalchemy.types.Integer:
            print(f"{column_name} is already an Integer type. Skipping reverse index.")
            continue
        df[column_name] = df[column_name].map(reverse_index)
        dtypes[column_name] = dtype
    new_table_name = old_table if old_table.endswith(suffix) else old_table+suffix
    df.to_sql(new_table_name, engine, if_exists='replace', dtype=dtypes,
              chunksize=100000, index=False)
    restore_constraints(new_table_name, constraint_pk, constraints_fk, engine)
    return new_table_name


def add_foreign_keys(column_names, table_name, engine,
                    column_to_table_name:Callable[[str], str]=lambda x:x,
                    verbose=False):
    """
    Ajoute les foreign key correspondant aux colonnes factorisées
    """
    with engine.connect() as conn:
        for column_name in column_names:
            query = f"ALTER TABLE `{table_name}` \
                ADD CONSTRAINT `fk_{table_name}_{column_name}` FOREIGN KEY (`{column_name}`) \
                REFERENCES `{column_to_table_name(column_name)}`(`id`) \
                ON DELETE CASCADE;"
            if verbose:
                print(query)
            conn.execute(sqlalchemy.text(query))



def factor_categories(column_names, table_names, engine,
                      column_to_table_name:Callable[[str], str]=lambda x:x,
                      verbose=False):
    """
    Sépare les labels des colonnes catégorielle vers des tables de référence.
    Remplace les valeurs par des identifiants et crée des tables de correspondance.
    """
    label_maps = {}
    inspector = sqlalchemy.inspect(engine)
    for column_name in column_names:
        reference_table_name = column_to_table_name(column_name)
        if verbose:
            print(f"Building reference table {reference_table_name} for {column_name}")
        if inspector.has_table(reference_table_name):
            if verbose:
                print(f"Found table for {reference_table_name}, skipping build and reading instead")
            labels = pd.read_sql_table(reference_table_name, engine)
            labels = labels[labels.columns[1]]  # Récupérer la Series depuis la DataFrame
            columns = inspector.get_columns(reference_table_name)
            dtype = next(column["type"] for column in columns if column["name"]=='id')
        else:
            labels = get_labels(column_name, table_names, engine)
            dtype = create_labels_table(reference_table_name, labels, engine)
        label_maps[column_name] = make_reverse_index(labels), dtype

    for table_name in table_names:
        new_table_name = replace_categorical_columns(table_name, label_maps, engine)
        add_foreign_keys(column_names, new_table_name, engine, column_to_table_name, verbose)


def main():
    """Main pour ce fichier."""
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
            lambda x:x.lower()+'_ref', verbose=True)

#%%
if __name__ == "__main__":
    main()
