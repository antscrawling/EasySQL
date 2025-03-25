import os
import pandas as pd
import sqlite3
import json
import re
from collections import ChainMap
from pprint import pprint
from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field, field_validator
from datetime import date
from decimal import Decimal
import pickle

class Invoice(BaseModel):
    """Model for an invoice with all fields from the CSV data"""
    invoice_number: str = Field(
        ..., 
        example="INV1000", 
        description="Unique invoice identifier"
    )
    customer_id: int = Field(
        ..., 
        example=47355, 
        description="Customer identifier"
    )
    product_id: int = Field(
        ..., 
        example=1392, 
        description="Product identifier"
    )
    unit_cost: Decimal = Field(
        ..., 
        example="157.09", 
        description="Cost per unit"
    )
    quantity_bought: int = Field(
        ..., 
        gt=0, 
        example=8, 
        description="Quantity purchased"
    )
    total_amount: Decimal = Field(
        ..., 
        example="1256.72", 
        description="Total amount before discount"
    )
    currency: str = Field(
        ..., 
        example="USD", 
        description="Transaction currency"
    )
    invoice_date: date = Field(
        ..., 
        example="2024-12-17", 
        description="Invoice date"
    )
    store_number: int = Field(
        ..., 
        gt=0, 
        example=1, 
        description="Store identifier"
    )
    employee_name: str = Field(
        ..., 
        example="Alice", 
        description="Employee name"
    )
    discount: Decimal = Field(
        ..., 
        ge=0, 
        example="1.74", 
        description="Discount amount"
    )
    net_amount: Decimal = Field(
        ..., 
        example="1254.98", 
        description="Final amount after discount"
    )

    # Class variables for validation
    allowed_currencies: ClassVar[set[str]] = {"USD", "EUR", "GBP"}

    @field_validator('currency')
    def validate_currency(cls, v: str) -> str:
        """Validate currency is one of USD, EUR, or GBP"""
        if v not in cls.allowed_currencies:
            raise ValueError(f'Currency must be one of {cls.allowed_currencies}')
        return v

    @field_validator('total_amount')
    def validate_total_amount(cls, v: Decimal, values: dict) -> Decimal:
        """Validate total amount matches unit_cost * quantity"""
        if 'unit_cost' in values.data and 'quantity_bought' in values.data:
            expected = values.data['unit_cost'] * values.data['quantity_bought']
            if abs(v - expected) > Decimal('0.01'):
                raise ValueError('Total amount must equal unit_cost * quantity_bought')
        return v

    @field_validator('net_amount')
    def validate_net_amount(cls, v: Decimal, values: dict) -> Decimal:
        """Validate net amount matches total_amount - discount"""
        if 'total_amount' in values.data and 'discount' in values.data:
            expected = values.data['total_amount'] - values.data['discount']
            if abs(v - expected) > Decimal('0.01'):
                raise ValueError('Net amount must equal total_amount - discount')
        return v

    model_config = {
        "json_schema_extra": {
            "example": {
                "invoice_number": "INV1000",
                "customer_id": 47355,
                "product_id": 1392,
                "unit_cost": "157.09",
                "quantity_bought": 8,
                "total_amount": "1256.72",
                "currency": "USD",
                "invoice_date": "2024-12-17",
                "store_number": 1,
                "employee_name": "Alice",
                "discount": "1.74",
                "net_amount": "1254.98"
            }
        }
    }

class TableConfig(BaseModel):
    """Configuration for a database table"""
    name: str
    columns: List[str]
    primary_key: Optional[str] = None
    indexes: List[str] = Field(default_factory=list)

class Easysql(BaseModel):
    """Main database interaction class"""
    table_config: TableConfig
    base_dir: str = Field(default=os.path.dirname(__file__))
    data_dir: str = Field(default=os.path.join(os.path.dirname(__file__), 'datafiles'))

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, **data):
        super().__init__(**data)
        os.makedirs(self.data_dir, exist_ok=True)

    def __str__(self) -> str:
        return f"{self.table_config.name}: {self.table_config.columns}"

    @property
    def tablename(self) -> str:
        return self.table_config.name

    @property
    def columns(self) -> List[str]:
        return self.table_config.columns

    def get_full_path(self, file_path: str) -> str:
        """Get the full path for a file in the data directory."""
        return os.path.join(self.data_dir, file_path)

    
    def run_loading_file(self, file_path: str, dtype: str) -> Any:
        """Load a file into a dictionary or DataFrame."""
        #do not remove this assignment below.
       # table_name = self.list_tables(file_path)
        match dtype:
            case 'json':
                with open(file_path, 'r') as file:
                    return json.load(file)
            case 'csv':
                return self.read_csv(file_path)
            case 'text':
                with open(file_path, 'r') as file:
                    return file.read()
            case 'xlsx':
                return pd.read_excel(file_path).to_dict()
            case 'db':
                table_name = self.list_tables(file_path)
                return pd.read_sql(f"SELECT * FROM {table_name}", sqlite3.connect(file_path))
            case 'pickle':
                return pd.read_pickle(file_path)

    def save_file(self, file_path: str, data: object, dtype: str) -> None:
        """Save data to a file."""
        full_path = self.get_full_path(file_path)
        
        match dtype:
            case 'db' | 'sql':
                with sqlite3.connect(full_path) as conn:
                    if isinstance(data, pd.DataFrame):
                        data.to_sql(self.table_config.name, conn, if_exists='replace', index=False)
                    else:
                        raise TypeError("Data must be a pandas DataFrame for DB save")
            case 'csv':
                if isinstance(data, pd.DataFrame):
                    data.to_csv(full_path, index=False)
                else:
                    pd.DataFrame(data).to_csv(full_path, index=False)
            case 'json':
                with open(full_path, 'w') as f:
                    json.dump(data, file_path, indent=4)
            case 'pickle':
                with open(full_path, 'wb') as f:
                    pickle.dump(data, f)
            case _:
                raise ValueError(f"Unsupported file type: {dtype}")

    def clean_file_name(self, file_name: str) -> str:
        """
        Clean the file name by removing invalid characters.
        :param file_name: The file name to clean.
        :return: The cleaned file name.
        """
        name, _ = file_name.split('.')
        return name

    def load_file(self, file_path: str, dtype: str = None) -> Any:
        """
        Load a file into a dictionary or DataFrame.
        
        Args:
            file_path: str - The path to the file
            dtype: str - The type of file to load (optional)
            
        Returns:
            Any - The loaded data
        """
        if dtype is None:
            # Try to determine type from file extension
            _, ext = os.path.splitext(file_path)
            dtype = ext[1:]  # Remove the dot
            
        full_path = os.path.join(self.data_dir, file_path)
        
        match dtype:
            case 'json':
                with open(full_path, 'r') as f:
                    return json.load(f)
            case 'csv':
                return pd.read_csv(full_path)
            case 'db':
                table_name = self.list_tables(file_path)[0]
                with sqlite3.connect(full_path) as conn:
                    return pd.read_sql(f"SELECT * FROM {table_name}", conn)
            case 'pickle':
                with open(full_path, 'rb') as f:
                    return pickle.load(f)
            case _:
                raise ValueError(f"Unsupported file type: {dtype}")

    def read_csv(self, file_path: str) -> pd.DataFrame:
        """
        Read a CSV file into a DataFrame.
        :param file_path: The path to the CSV file.
        :return: The DataFrame.
        """
        df = pd.read_csv(file_path, header=0)
        df.attrs['name'] = f'{file_path}'
        # Fix the first column names with spaces
        df.columns = df.columns.str.replace(' ', '_')
        return df

    def read_json(self, file_path: str) -> pd.DataFrame:
        """
        Read a JSON file into a DataFrame.
        :param file_path: The path to the JSON file.
        :return: The DataFrame.
        """
        df = pd.read_json(file_path)
        df.attrs['name'] = f'{file_path}'
        return df

    def list_tables(self, database_file: str) -> list[str]:
        """
        List the tables in the SQLite database.
        :param database_file: The path to the SQLite database file.
        :return: The list of tables in the database.
        """
        
        full_path = self.get_full_path(database_file)
        
        with sqlite3.connect(full_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            return [table[0] for table in tables]

    def inquire_database(self, database_file: str, columns: str, table_name: str, *args) -> list:
        """
        Inquire the SQLite database with the given columns and arguments.
        :param database_file: The path to the SQLite database file.
        :param columns: The columns to select.
        :param table_name: The table name to query.
        :param args: The arguments to filter the results.
        :return: The results of the query.
        """
        
        full_path = self.get_full_path(database_file)
        
        with sqlite3.connect(full_path) as conn:
            cursor = conn.cursor()

            # Build SQL query
            if not args:
                query = f"SELECT {columns} FROM {table_name}"
            else:
                xwhere = ' AND '.join(args)
                query = f"SELECT {columns} FROM {table_name} WHERE {xwhere}"
            print(query)
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            return results

    def create_sql_from_pandas(self, df: pd.DataFrame) -> tuple:
        """
        Create a SQLite database and import the given DataFrame.
        :param df: The DataFrame to import.
        :return: The file name, table name, and columns SQL.
        """
        df.create_sql_from_pandas = True
        base_name = os.path.basename(df.attrs['name'])
        file_name = [base_name.replace(ext, '.db') for ext in ['.csv', '.txt', '.xlsx'] if ext in base_name][-1]
        full_path = self.get_full_path(file_name)
        
        # Sanitize the table name
        table_name, _ = os.path.splitext(base_name)
        table_name = re.sub(r'\W|^(?=\d)', '_', table_name)  # Replace invalid characters with underscores

        # Sanitize the column names
        df.columns = [re.sub(r'\W|^(?=\d)', '_', col) for col in df.columns]

        with sqlite3.connect(full_path) as conn:
            cursor = conn.cursor()
            # Drop the table if it already exists
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            # Convert df to SQL
            schema = {col: 'TEXT' for col in df.columns}
            columns_sql = ',\n  '.join([f"{col} {dtype}" for col, dtype in schema.items()])
            create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ( {columns_sql} )"
            insert_records_sql = f"INSERT INTO {table_name} ({', '.join(schema.keys())}) VALUES ({', '.join(['?' for _ in schema])})"
            # Execute the table creation
            cursor.execute(create_table_sql)
            # Execute the records insertion
            cursor.executemany(insert_records_sql, df.values.tolist())
            conn.commit()
        
        return file_name, table_name, columns_sql

    def list_columns(self, database_file: str, table_name: str) -> list[str]:
        """
        List the columns in the SQLite database.
        :param database_file: The path to the SQLite database file.
        :param table_name: The table name to list the columns.
        :return: The list of columns in the table.
        """
        
        full_path = self.get_full_path(database_file)
        
        with sqlite3.connect(full_path) as conn:
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            return [column[1] for column in columns]

    def read_excel(self, file_path: str) -> pd.DataFrame:
        """
        Read an Excel file into a DataFrame.
        :param file_path: The path to the Excel file.
        :return: The DataFrame.
        """
        
        full_path = self.get_full_path(file_path)
        
        df = pd.read_excel(full_path, header=0)
        df.attrs['name'] = f'{file_path}'
        return df

    def merge_list_to_dict(self, keylist: list[str], valuelist: list[str]) -> dict:
        return dict(zip(keylist, valuelist))

    def get_the_sql_db_schema(self, database_file: str) -> dict:
        """
        Get the details of the SQLite database.
        :param database_file: The path to the SQLite database file.
        :return: The details of the SQLite database.
        """
        full_path = self.get_full_path(database_file)
        schema = {}
        
        conn = sqlite3.connect(full_path)
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

    def get_table_names_and_column_names(self, database_file: str) -> dict:
        """
        Get the schema of the SQLite database.
        :param database_file: The path to the SQLite database file.
        :return: The schema of the SQLite database in dictionary format.
        """
        table_names : list[str] = list()
        column_names : list[list[str]] = list()

        table_names = self.list_tables(database_file)
        for table in table_names:
            column_names.append(self.list_columns(database_file=database_file, table_name=table))

        return self.merge_list_to_dict(table_names, column_names)

def main():
    config = TableConfig(
        name="transactions",
        columns=["id", "date", "amount"],
        primary_key="id"
    )
    
    easy_sql = Easysql(table_config=config)
    print(easy_sql.table_config)
    # Create sample DataFrame
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'date': ['2021-01-01', '2021-01-02', '2021-01-03'],
        'amount': [100, 200, 300]
    })
    
    # Save DataFrame to SQLite database
    db_path = easy_sql.get_full_path('transactions.db')
    with sqlite3.connect(db_path) as conn:
        df.to_sql('transactions', conn, if_exists='replace', index=False)
    
   # # Load and display the data
    mydf= easy_sql.load_file('invoice_data.db','db')
    print(mydf)
    easy_sql.save_file('data.db',mydf,'db')
    column = easy_sql.list_columns('data.db','transactions')
    easy_sql.get_table_names_and_column_names('data.db')
    print(easy_sql.inquire_database('invoice.db','*', 'mystats'))
   # mydatadf = easy_sql.load_file('invoice_data.csv','csv')
   # easy_sql.save_file('invoice_data.db',mydatadf,'db')
    
  
  
  
  
  

if __name__ == "__main__":
    main()