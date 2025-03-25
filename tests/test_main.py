import unittest
import os
import pandas as pd
from main import main
from Models import Easysql, TableConfig

class TestMain(unittest.TestCase):
    """Test cases for the main script"""

    def setUp(self):
        """Set up test fixtures"""
        self.config = TableConfig(
            name="transactions",
            columns=["id", "date", "amount"],
            primary_key="id"
        )
        self.easy_sql = Easysql(table_config=self.config)
        
        # Add cleanup
        self.addCleanup(self.cleanup_test_files)

    def cleanup_test_files(self):
        """Clean up test files"""
        test_files = [
            'transactions.db',
            'invoice_data.db',
            'data.db',
            'invoice.db'
        ]
        for file in test_files:
            path = os.path.join(self.easy_sql.data_dir, file)
            if os.path.exists(path):
                os.remove(path)

    def test_main_execution(self):
        """Test main function execution"""
        try:
            main()
            self.assertTrue(os.path.exists(os.path.join(self.easy_sql.data_dir, 'transactions.db')))
        except Exception as e:
            self.fail(f"Main execution failed with error: {str(e)}")

if __name__ == '__main__':
    unittest.main()