import unittest
from models import Invoice
from decimal import Decimal
from datetime import date

class TestInvoice(unittest.TestCase):
    """Test cases for the Invoice model"""

    def setUp(self):
        """Set up test fixtures"""
        self.valid_data = {
            "invoice_number": "INV1000",
            "customer_id": 47355,
            "product_id": 1392,
            "unit_cost": Decimal("157.09"),
            "quantity_bought": 8,
            "total_amount": Decimal("1256.72"),
            "currency": "USD",
            "date": "2024-12-17",
            "store_number": 1,
            "employee_name": "Alice",
            "discount": Decimal("1.74"),
            "net_amount": Decimal("1254.98")
        }

    def test_valid_invoice(self):
        """Test creating a valid invoice"""
        invoice = Invoice(**self.valid_data)
        self.assertEqual(invoice.invoice_number, "INV1000")
        self.assertEqual(invoice.net_amount, Decimal("1254.98"))

    def test_invalid_currency(self):
        """Test invoice with invalid currency"""
        invalid_data = self.valid_data.copy()
        invalid_data["currency"] = "JPY"
        with self.assertRaises(ValueError):
            Invoice(**invalid_data)

if __name__ == '__main__':
    unittest.main()