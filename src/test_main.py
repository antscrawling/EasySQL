import unittest
import os
import pandas as pd
from main import main
from Models import Easysql, TableConfig

class TestMain(unittest.TestCase):
    """Test cases for the main script functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.config = TableConfig(
            name="test_stats",
            columns=["name", "age", "occupation", "street", "city", "state", "zip"],
            primary_key="name"
        )
        self.easy_sql = Easysql(table_config=self.config)
        
        # Create test files directory if it doesn't exist
        os.makedirs(self.easy_sql.data_dir, exist_ok=True)
        
        # Add cleanup
        self.addCleanup(self.cleanup_test_files)

    def cleanup_test_files(self):
        """Clean up test files after each test"""
        test_files = [
            'mystats.pickle',
            'invoice_data.csv',
            'invoice.db'
        ]
        for file in test_files:
            file_path = os.path.join(self.easy_sql.data_dir, file)
            if os.path.exists(file_path):
                os.remove(file_path)

    def test_data_creation(self):
        """Test that data files are created correctly"""
        main()  # Run the main function
        
        # Check if files exist
        self.assertTrue(os.path.exists(os.path.join(self.easy_sql.data_dir, 'mystats.pickle')))
        self.assertTrue(os.path.exists(os.path.join(self.easy_sql.data_dir, 'invoice_data.csv')))
        self.assertTrue(os.path.exists(os.path.join(self.easy_sql.data_dir, 'invoice.db')))

    def test_invoice_data_content(self):
        """Test that invoice data is created with correct content"""
        main()  # Run the main function
        
        # Load and check invoice data
        invoice_path = os.path.join(self.easy_sql.data_dir, 'invoice_data.csv')
        df = pd.read_csv(invoice_path)
        
        # Check structure
        expected_columns = ['Invoice_Number', 'Date', 'Customer', 'Amount']
        self.assertListEqual(list(df.columns), expected_columns)
        
        # Check content
        self.assertEqual(len(df), 3)  # Should have 3 rows
        self.assertEqual(df['Customer'].iloc[0], 'John Doe')
        self.assertEqual(df['Amount'].iloc[0], 1500.00)

    def test_pickle_data_content(self):
        """Test that pickle data is saved correctly"""
        main()  # Run the main function
        
        # Load and check pickle data
        pickle_path = os.path.join(self.easy_sql.data_dir, 'mystats.pickle')
        data = self.easy_sql.load_file(pickle_path, 'pickle')
        
        # Check content
        self.assertEqual(data['name'], 'John Doe')
        self.assertEqual(data['age'], '35')
        self.assertEqual(data['occupation'], 'Plumber')

if __name__ == '__main__':
    unittest.main()