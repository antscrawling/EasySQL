import unittest
import os
import pandas as pd
import json
import sqlite3
from Models import (
    load_file, save_file, clean_file_name, determine_file_type,
    read_csv, read_json, list_tables, inquire_database,
    create_sql_from_pandas, list_columns, read_excel,
    merge_list_to_dict, get_the_sql_db_schema, get_table_names_and_column_names
)

class TestModels(unittest.TestCase):

    def setUp(self):
        # Setup code to create test files in the datafiles directory
        self.base_dir = os.path.dirname(__file__)
        self.data_dir = os.path.join(self.base_dir, 'datafiles')
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Create a sample CSV file
        self.csv_file_path = os.path.join(self.data_dir, 'test.csv')
        df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        df.to_csv(self.csv_file_path, index=False)
        
        # Create a sample JSON file
        self.json_file_path = os.path.join(self.data_dir, 'test.json')
        with open(self.json_file_path, 'w') as f:
            json.dump([{'col1': 1, 'col2': 3}, {'col1': 2, 'col2': 4}], f)
        
        # Create a sample SQLite database
        self.db_file_path = os.path.join(self.data_dir, 'test.db')
        conn = sqlite3.connect(self.db_file_path)
        conn.execute('CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT)')
        conn.execute('INSERT INTO test (name) VALUES ("Alice")')
        conn.commit()
        conn.close()

    def tearDown(self):
        # Cleanup code to remove test files from the datafiles directory
        os.remove(self.csv_file_path)
        os.remove(self.json_file_path)
        os.remove(self.db_file_path)

    def test_load_file_csv(self):
        df = load_file('test.csv', 'csv')
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.shape, (2, 2))

    def test_load_file_json(self):
        data = load_file('test.json', 'json')
        self.assertIsInstance(data, list)
        self.assertEqual(data[0]['col1'], 1)

    def test_save_file_csv(self):
        df = pd.DataFrame({'col1': [5, 6], 'col2': [7, 8]})
        save_file('test.csv', df, 'csv')
        df_loaded = pd.read_csv(self.csv_file_path)
        self.assertTrue(df.equals(df_loaded))

    def test_clean_file_name(self):
        cleaned_name = clean_file_name('test file.csv')
        self.assertEqual(cleaned_name, 'test file')

    def test_determine_file_type(self):
        df, file_name = determine_file_type(self.csv_file_path)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(file_name, self.csv_file_path)

    def test_read_csv(self):
        df = read_csv(self.csv_file_path)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.shape, (2, 2))

    def test_read_json(self):
        df = read_json(self.json_file_path)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.shape, (2, 2))

    def test_list_tables(self):
        tables = list_tables(self.db_file_path)
        self.assertIn('test', tables)

    def test_inquire_database(self):
        results = inquire_database(self.db_file_path, '*', 'test')
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0][1], 'Alice')

    def test_create_sql_from_pandas(self):
        df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        df.attrs['name'] = 'test.csv'
        file_name, table_name, columns_sql = create_sql_from_pandas(df)
        self.assertTrue(os.path.exists(file_name))
        self.assertEqual(table_name, 'test')
        self.assertIn('col1 TEXT', columns_sql)

    def test_list_columns(self):
        columns = list_columns(self.db_file_path, 'test')
        self.assertIn('name', columns)

    def test_read_excel(self):
        # Create a sample Excel file
        excel_file_path = os.path.join(self.data_dir, 'test.xlsx')
        df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        df.to_excel(excel_file_path, index=False)
        df_loaded = read_excel(excel_file_path)
        self.assertIsInstance(df_loaded, pd.DataFrame)
        os.remove(excel_file_path)

    def test_merge_list_to_dict(self):
        keys = ['a', 'b']
        values = [1, 2]
        result = merge_list_to_dict(keys, values)
        self.assertEqual(result, {'a': 1, 'b': 2})

    def test_get_the_sql_db_schema(self):
        schema = get_the_sql_db_schema(self.db_file_path)
        self.assertIn('test', schema)

    def test_get_table_names_and_column_names(self):
        schema = get_table_names_and_column_names(self.db_file_path)
        self.assertIn('test', schema)
        self.assertIn('name', schema['test'])

if __name__ == '__main__':
    unittest.main()