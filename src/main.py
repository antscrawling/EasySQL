from Models import Easysql, TableConfig
from collections import ChainMap
import pandas as pd
import os

def main():
    # Create table configuration
    config = TableConfig(
        name="mystats",
        columns=["name", "age", "occupation", "street", "city", "state", "zip"],
        primary_key="name"
    )
    
    # Create a new instance of the EasySQL class with config
    mysql = Easysql(table_config=config)
    
    myusa_address: dict[str, str] = {
        'street': '123 Main Street',
        'city': 'Springfield',
        'state': 'IL',
        'zip': '62701'
    }
    
    mycan_address: dict[str, str] = {
        'street': '123 Rue Principale',
        'city': 'Montreal',
        'province': 'Quebec',
        'postal_code': 'H1A 1A1'
    }
    
    mystats: dict[str, str] = {
        'name': 'John Doe',
        'age': '35',
        'occupation': 'Plumber'
    }
    
    # Create a ChainMap with the dictionaries
    mytotal = ChainMap({}, myusa_address, mycan_address, mystats)
    
    # Convert ChainMap to DataFrame
    mydf = pd.DataFrame([dict(mytotal)])
    
    # Save the ChainMap data as pickle
    mysql.save_file('mystats.pickle', mytotal, 'pickle')
    
    try:
        # Create sample invoice data if it doesn't exist
        if not os.path.exists(mysql.get_full_path('invoice_data.csv')):
            sample_df = pd.DataFrame({
                'Invoice_Number': ['INV-001', 'INV-002', 'INV-003'],
                'Date': ['2024-03-24', '2024-03-24', '2024-03-24'],
                'Customer': ['John Doe', 'Jane Smith', 'Bob Johnson'],
                'Amount': [1500.00, 2300.00, 950.00]
            })
            mysql.save_file('invoice_data.csv', sample_df, 'csv')
        
        # Load the invoice data
        invoice_data = mysql.load_file('invoice_data.csv', 'csv')
        print("\nInvoice Data:")
        print(invoice_data)
        
        # Save as SQLite database
        mysql.save_file('invoice.db', invoice_data, 'db')
        
        print("\nPerson Data:")
        print(dict(mytotal))
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == '__main__':
    main()
