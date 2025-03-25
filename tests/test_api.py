import unittest
from fastapi.testclient import TestClient
from datetime import date
from decimal import Decimal
import json
import os
import pandas as pd
from ..src.EasySQL_API import app, get_db
from ..src.Models import Easysql, TableConfig

class TestEasySQLAPI(unittest.TestCase):
    """Test cases for the EasySQL API endpoints"""

    def setUp(self):
        """Set up test fixtures"""
        self.client = TestClient(app)
        
        # Create test database
        self.config = TableConfig(
            name="invoices",
            columns=[
                "invoice_number", "customer_id", "product_id", "unit_cost",
                "quantity_bought", "total_amount", "currency", "invoice_date",
                "store_number", "employee_name", "discount", "net_amount"
            ],
            primary_key="invoice_number"
        )
        self.db = Easysql(table_config=self.config)
        
        # Create sample data
        self.sample_invoice = {
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
        
        # Create test database with sample data
        df = pd.DataFrame([self.sample_invoice])
        self.db.save_file('invoice_data.db', df, 'db')
        
        # Add cleanup
        self.addCleanup(self.cleanup_test_files)

    def cleanup_test_files(self):
        """Clean up test files after each test"""
        if os.path.exists(self.db.get_full_path('invoice_data.db')):
            os.remove(self.db.get_full_path('invoice_data.db'))

    def test_read_invoices(self):
        """Test GET /invoices/ endpoint"""
        response = self.client.get("/invoices/")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["invoice_number"], "INV1000")

    def test_read_invoice(self):
        """Test GET /invoices/{invoice_number} endpoint"""
        response = self.client.get("/invoices/INV1000")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["invoice_number"], "INV1000")
        self.assertEqual(data["employee_name"], "Alice")

    def test_create_invoice(self):
        """Test POST /invoices/ endpoint"""
        new_invoice = self.sample_invoice.copy()
        new_invoice["invoice_number"] = "INV1001"
        response = self.client.post(
            "/invoices/",
            json=new_invoice
        )
        self.assertEqual(response.status_code, 201)
        data = response.json()
        self.assertEqual(data["invoice_number"], "INV1001")

    def test_update_invoice(self):
        """Test PUT /invoices/{invoice_number} endpoint"""
        update_data = self.sample_invoice.copy()
        update_data["employee_name"] = "Bob"
        response = self.client.put(
            "/invoices/INV1000",
            json=update_data
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["employee_name"], "Bob")

    def test_delete_invoice(self):
        """Test DELETE /invoices/{invoice_number} endpoint"""
        response = self.client.delete("/invoices/INV1000")
        self.assertEqual(response.status_code, 204)
        
        # Verify deletion
        response = self.client.get("/invoices/INV1000")
        self.assertEqual(response.status_code, 404)

    def test_invalid_invoice_creation(self):
        """Test invoice creation with invalid data"""
        invalid_invoice = self.sample_invoice.copy()
        invalid_invoice["currency"] = "JPY"  # Invalid currency
        response = self.client.post(
            "/invoices/",
            json=invalid_invoice
        )
        self.assertEqual(response.status_code, 400)

    def test_invoice_not_found(self):
        """Test requesting non-existent invoice"""
        response = self.client.get("/invoices/INVALID")
        self.assertEqual(response.status_code, 404)

if __name__ == '__main__':
    unittest.main()