from importlib.resources import files
import pandas as pd
import sqlite3
import json
import re
from collections import ChainMap
from pprint import pprint
from pydantic import BaseModel


class EasySQL:
    def __init__(self):
        pass    
    
    
    
def load_file(file_path: str, dtype: str) -> object:
    """
    Load a file into a dictionary or DataFrame.
    :param file_path: The path to the file.
    :param dtype: The type of the file.
    :return: The loaded data.
    """
    match dtype:
        case 'json':
            with open(file_path, 'r') as file:
                return json.load(file)
        case 'csv':
            return read_csv(file_path)
        case 'text':
            with open(file_path, 'r') as file:
                return file.read()
        case 'xlsx':
            return pd.read_excel(file_path).to_dict()
        case 'db':
            return inquire_database(file_path, '*')
        case 'pickle':
            return pd.read_pickle(file_path)

def save_file(file_path: str, data: object, dtype: str) -> None:
    """
    Save data to a file.
    :param file_path: The path to the file.
    :param data: The data to save.
    :param dtype: The type of the file.
    """
    match dtype:
        case 'json':
            with open(file_path, 'w') as file:
                json.dump(data, file, indent=4)
        case 'csv':
            data.to_csv(file_path, index=False)
        case 'text':
            with open(file_path, 'w') as file:
                file.write(data)
        case 'xlsx':
            data.to_excel(file_path, index=False)
        case 'db':
            pass
        case 'pickle':
            data.to_pickle(file_path)

def determine_file_type(*args) -> tuple[object, str]:
    """
    Determine the type of file based on the content of the text file.
    :param args: The file paths.
    :return: A tuple containing the loaded file and the file name.
    """
    pattern = re.compile(r'\.\w+$')
    extensions = [
        '.py', '.rs', '.sql', '.java', '.c',
        '.cpp', '.html', '.css', '.js', '.ts',
        '.php', '.go', '.swift', '.kt', '.rb',
        '.pl', '.sh', '.bash', '.ps1', '.bat',
        '.vbs', '.vba', '.r', '.scala', '.dart',
        '.lua', '.perl', '.asm', '.h', '.hpp',
        '.cs', '.m', '.vb', '.f', '.f90', '.f95',
        '.f03', '.f08', '.f77', '.f95', '.f03',
        '.f08', '.f77', '.f90'
    ]

    for file in args:
        if pattern.search(file):
            match file:
                case _ if '.csv' in file:
                    return load_file(file, 'csv'), file
                case _ if '.json' in file:
                    return load_file(file, 'json'), file
                case _ if '.txt' in file:
                    return load_file(file, 'text'), file
                case _ if '.xlsx' in file:
                    return load_file(file, 'xlsx'), file
                case _ if '.db' in file:
                    return load_file(file, 'db'), file
                case _ if '.pickle' in file:
                    return load_file(file, 'pickle'), file
                case _ if any(file.endswith(ext) for ext in extensions):
                    return load_file(file, 'text'), file
                case _:
                    pass
        else:
            return None, f'File type not supported {file}'

def read_csv(file_path: str) -> pd.DataFrame:
    """
    Read a CSV file into a DataFrame.
    :param file_path: The path to the CSV file.
    :return: The DataFrame.
    """
    df = pd.read_csv(file_path, header=0)
    df.attrs['name'] = f'{file_path}'
    return df

def read_json(file_path: str) -> pd.DataFrame:
    """
    Read a JSON file into a DataFrame.
    :param file_path: The path to the JSON file.
    :return: The DataFrame.
    """
    df = pd.read_json(file_path)
    df.attrs['name'] = f'{file_path}'
    return df

def list_tables(database_file: str) -> list[str]:
    """
    List the tables in the SQLite database.
    :param database_file: The path to the SQLite database file.
    :return: The list of tables in the database.
    """
    with sqlite3.connect(database_file) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        return [table[0] for table in tables]

def inquire_database(database_file: str, columns: str, table_name: str, *args) -> list:
    """
    Inquire the SQLite database with the given columns and arguments.
    :param database_file: The path to the SQLite database file.
    :param columns: The columns to select.
    :param table_name: The table name to query.
    :param args: The arguments to filter the results.
    :return: The results of the query.
    """
    with sqlite3.connect(database_file) as conn:
        cursor = conn.cursor()

        # Build SQL query
        if not args:
            query = f"SELECT {columns} FROM {table_name}"
        else:
            xwhere = ' AND '.join(args)
            query = f"SELECT {columns} FROM {table_name} WHERE {xwhere}"
        print(query)
        cursor.execute(query)
        return cursor.fetchall()

def create_sql_from_pandas(df: pd.DataFrame) -> tuple:
    """
    Create a SQLite database and import the given DataFrame.
    :param df: The DataFrame to import.
    :return: The file name, table name, and columns SQL.
    """
    file_name = df.attrs['name']
    file_name = [file_name.replace(ext, '.db') for ext in ['.csv', '.txt', '.xlsx'] if ext in file_name][-1]

    with sqlite3.connect(file_name) as conn:
        cursor = conn.cursor()
        table_name, _ = file_name.split('.')
        # Convert df to SQL
        schema = {col: 'TEXT' for col in df.columns}
        columns_sql = ',\n  '.join([f"{col} {dtype}" for col, dtype in schema.items()])
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
        {columns_sql}
        );
        """
        insert_records_sql = f"""
        INSERT INTO {table_name} ({', '.join(schema.keys())})
        VALUES ({', '.join(['?' for _ in schema])});
        """
        # Execute the table creation
        cursor.execute(create_table_sql)
        # Execute the records insertion
        cursor.executemany(insert_records_sql, df.values.tolist())
        conn.commit()

        print(f"Table '{{table_name}}' created successfully in {file_name}")
    return file_name, table_name, columns_sql

def list_columns(database_file: str, table_name: str) -> list[str]:
    """
    List the columns in the SQLite database.
    :param database_file: The path to the SQLite database file.
    :param table_name: The table name to list the columns.
    :return: The list of columns in the table.
    """
    with sqlite3.connect(database_file) as conn:
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        return [column[1] for column in columns]

def read_excel(file_path: str) -> pd.DataFrame:
    """
    Read an Excel file into a DataFrame.
    :param file_path: The path to the Excel file.
    :return: The DataFrame.
    """
    df = pd.read_excel(file_path, header=0)
    df.attrs['name'] = f'{file_path}'
    return df

def merge_list_to_dict(keylist: list[str], valuelist: list[str]) -> dict:
    return  dict(zip(keylist, valuelist))

def get_the_sql_db_schema(database_file: str) -> dict:
    """
    Get the details of the SQLite database.
    :param database_file: The path to the SQLite database file.
    :return: The details of the SQLite database.
    """
    
    """
    Connects to a SQLite database and extracts its schema as a dictionary.
    
    Args:
        database_file (str): Path to the SQLite .db file.

    Returns:
        dict: Schema representation with tables, columns, and types.
    """
    schema = {}
    
    conn = sqlite3.connect(database_file)
    cursor = conn.cursor()
    
    # Get list of tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ")
    tables = cursor.fetchall()

    for (table_name,) in tables:
        # Get table creation SQL (optional, for full config)
        cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
        create_stmt = cursor.fetchone()[0]

        # Get column info
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()

        schema[table_name] = {
            'create_sql': create_stmt,
            'columns': [
                {
                    'cid': col[0],
                    'name': col[1],
                    'type': col[2],
                    'notnull': bool(col[3]),
                    'default': col[4],
                    'primary_key': bool(col[5])
                }
                for col in columns
            ]
        }

    conn.close()
    return schema

def get_table_names_and_column_names(database_file: str) -> dict:
    """
    Get the schema of the SQLite database.
    :param database_file: The path to the SQLite database file.
    :return: The schema of the SQLite database in dictionary format.
    """
    table_names : list[str] = list()
    column_names : list[list[str]] = list()

    table_names = list_tables(database_file)
    for table in table_names:
        column_names.append( list_columns(database_file=database_file, table_name=table) )

    return  merge_list_to_dict(table_names, column_names)


def main():
 
    database_file = 'datafiles/finance_tracker.db'
    mydbschema = get_the_sql_db_schema(database_file)
    #mydbschema = get_table_names_and_column_names(database_file)
    save_file(file_path=database_file,data=mydbschema,dtype='json')

if __name__ == "__main__":
    main()
