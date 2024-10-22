"""Code to factorize categorical labels out into a reference table."""
import math
from collections.abc import Iterable
from collections.abc import Callable
from unidecode import unidecode
import pandas as pd
import sqlalchemy
from sqlalchemy.dialects.mysql import TINYINT, SMALLINT, MEDIUMINT, INTEGER, BIGINT, VARCHAR
from sqlalchemy.types import SchemaType
from sqlalchemy import Engine as SqlEngine
from sqlalchemy import Table,ForeignKeyConstraint, PrimaryKeyConstraint, Column, ForeignKey

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


def connect_maria(db_name:str, user="root", pwd="", host="localhost", port="3306"):
    """
    Connecte une base de données MariaDB via SQLalchemy
    """
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



def create_labels_table(table_name:str, labels:pd.Series, engine:SqlEngine) -> SchemaType:
    """
    Ecrit la table de correspondance entre les id des catégories et leurs libellés dans la BDD.
    Renvoie le type de donnée 
    """
    metadata = sqlalchemy.MetaData()
    dtype = min_int_size(len(labels), safety_factor=2)
    table = Table(table_name, metadata,
                  Column('id', dtype), # primary_key=True),
                  Column(labels.name, VARCHAR(255), unique=True))
    with engine.connect() as conn:
        table.create(conn)
        labels.index = labels.index.rename("id")
        labels.to_sql(table_name, conn, if_exists='append', index=True, chunksize=100)
        conn.execute(sqlalchemy.text(f"ALTER TABLE `{table_name}` \
                                     ADD PRIMARY KEY (`{labels.index.name}`);"))
        conn.commit()

    return dtype

def make_reverse_index(labels:pd.Series):
    """
    Renvoie une lambda qui mappe chaque libellé à son index.
    """
    reverse_index = {standardize_string(label): idx
                     for idx, label in labels.items()
                     if label is not None}
    return lambda label: None if label is None else reverse_index[standardize_string(label)]


def replace_categorical_columns(old_table_name:str, 
                                label_maps:dict[str, tuple[Callable[[str],int], SchemaType]], 
                                engine:SqlEngine,
                                column_to_table_name:Callable[[str], str]=lambda x:x,
                                hard_clean = False,
                                suffix:str='_') -> str:
    """
    Met à jour une table pour utiliser les identifiants de catégorie à la place des libellés.
    Renvoie le nom de la table créée, avec un suffixe optionnel, ignoré s'il est déjà présent
    """
    inspector = sqlalchemy.inspect(engine)
    for column_name, (_, dtype) in label_maps.items():
        new_column_name = column_name + "ID"
        if any(new_column_name==column['name'] for column in inspector.get_columns(old_table_name)):
            if not any(column_name==column['name'] for column in inspector.get_columns(old_table_name)):
                print(f"Column {old_table_name}.{column_name} already processed. Skipping.")
                continue
        ref_table_name = column_to_table_name(column_name)
        
        add_query = sqlalchemy.text(f"""
            ALTER TABLE `{old_table_name}`
            ADD IF NOT EXISTS `{new_column_name}` {dtype.compile(engine.dialect)},
            ADD CONSTRAINT `fk_{new_column_name}` FOREIGN KEY IF NOT EXISTS (`{new_column_name}`)
            REFERENCES `{ref_table_name}`(id)
        """)
        
        update_query = sqlalchemy.text(f"""
            UPDATE `{old_table_name}`
            JOIN `{ref_table_name}`
            ON {old_table_name}.`{column_name}` = {ref_table_name}.`{column_name}`
            SET {old_table_name}.`{new_column_name}` = {ref_table_name}.`id`;
        """)

        drop_query = sqlalchemy.text(f"""
            ALTER `{old_table_name}`
            DROP `{column_name}`
        """)

        with engine.connect() as conn:
            print(add_query)
            conn.execute(add_query)
            print(update_query)
            conn.execute(update_query)
            if hard_clean:
                print(drop_query)
                conn.execute(drop_query)
            conn.commit()


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
        replace_categorical_columns(table_name, label_maps, engine, column_to_table_name)


def main():
    """Main pour ce fichier."""
    db_name = "crime_short_test"
    engine = connect_maria(db_name)
    print("Connected to DB " + db_name)
    category_columns = [
        (("outcomes_temp", "street_temp"), [
            "Reportedby",
            "Location",
            # "LSOAcode"
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
        (("outcomes_temp",), ["Outcometype"]),
        (("street_temp",),[
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
