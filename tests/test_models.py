import unittest
import os
import pandas as pd
import sqlite3
from Models import Easysql, TableConfig, Innvoice

class TestEasysql(unittest.TestCase):
    """Test cases for the Easysql class"""

    def setUp(self):
        """Set up test fixtures"""
        self.config = TableConfig(
            name="test_table",
            columns=["id", "name", "amount"],
            primary_key="id"
        )
        self.easy_sql = Easysql(table_config=self.config)
        
        # Create sample data
        self.sample_df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Test1', 'Test2', 'Test3'],
            'amount': [100, 200, 300]
        })
        
        # Create test files
        self.create_test_files()
        
        # Add cleanup
        self.addCleanup(self.cleanup_test_files)

    def create_test_files(self):
        """Create test files for different formats"""
        # CSV
        self.csv_path = os.path.join(self.easy_sql.data_dir, 'test.csv')
        self.sample_df.to_csv(self.csv_path, index=False)
        
        # JSON
        self.json_path = os.path.join(self.easy_sql.data_dir, 'test.json')
        self.sample_df.to_json(self.json_path)
        
        # SQLite
        self.db_path = os.path.join(self.easy_sql.data_dir, 'test.db')
        with sqlite3.connect(self.db_path) as conn:
            self.sample_df.to_sql('test_table', conn, index=False, if_exists='replace')

    def cleanup_test_files(self):
        """Clean up test files"""
        test_files = ['test.csv', 'test.json', 'test.db']
        for file in test_files:
            path = os.path.join(self.easy_sql.data_dir, file)
            if os.path.exists(path):
                os.remove(path)

    def test_initialization(self):
        """Test proper initialization"""
        self.assertEqual(self.easy_sql.tablename, "test_table")
        self.assertEqual(self.easy_sql.columns, ["id", "name", "amount"])
        self.assertTrue(os.path.exists(self.easy_sql.data_dir))

    def test_load_file(self):
        """Test file loading for different formats"""
        # Test CSV
        df_csv = self.easy_sql.load_file('test.csv', 'csv')
        self.assertIsInstance(df_csv, pd.DataFrame)
        self.assertEqual(len(df_csv), 3)

        # Test JSON
        df_json = self.easy_sql.load_file('test.json', 'json')
        self.assertIsInstance(df_json, dict)

        # Test DB
        df_db = self.easy_sql.load_file('test.db', 'db')
        self.assertIsInstance(df_db, pd.DataFrame)
        self.assertEqual(len(df_db), 3)

    def test_save_file(self):
        """Test file saving for different formats"""
        # Test CSV
        self.easy_sql.save_file('new_test.csv', self.sample_df, 'csv')
        self.assertTrue(os.path.exists(os.path.join(self.easy_sql.data_dir, 'new_test.csv')))

        # Test DB
        self.easy_sql.save_file('new_test.db', self.sample_df, 'db')
        self.assertTrue(os.path.exists(os.path.join(self.easy_sql.data_dir, 'new_test.db')))

    def test_list_tables(self):
        """Test listing tables from database"""
        tables = self.easy_sql.list_tables('test.db')
        self.assertIn('test_table', tables)

    def test_list_columns(self):
        """Test listing columns from table"""
        columns = self.easy_sql.list_columns('test.db', 'test_table')
        self.assertEqual(set(columns), {'id', 'name', 'amount'})

    def test_inquire_database(self):
        """Test database queries"""
        results = self.easy_sql.inquire_database('test.db', '*', 'test_table')
        self.assertEqual(len(results), 3)

    def test_create_sql_from_pandas(self):
        """Test creating SQL database from DataFrame"""
        self.sample_df.attrs['name'] = 'test_conversion.csv'
        file_name, table_name, columns_sql = self.easy_sql.create_sql_from_pandas(self.sample_df)
        self.assertTrue(os.path.exists(os.path.join(self.easy_sql.data_dir, file_name)))
        self.assertIn('id TEXT', columns_sql)

if __name__ == '__main__':
    unittest.main()