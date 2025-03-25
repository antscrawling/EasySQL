import unittest
from Models import Innvoice
from datetime import date

class TestInvoice(unittest.TestCase):
    """Test cases for the Invoice model"""

    def setUp(self):
        """Set up test fixtures"""
        self.valid_invoice_data = {
            "Invoice_Number": "INV-001",
            "Date": "2024-03-24",
            "Customer": "John Doe",
            "Amount": 1500.00
        }

    def test_valid_invoice_creation(self):
        """Test creating a valid invoice"""
        invoice = Innvoice(**self.valid_invoice_data)
        self.assertEqual(invoice.Invoice_Number, "INV-001")
        self.assertEqual(invoice.Amount, 1500.00)

    def test_invalid_amount(self):
        """Test invoice with invalid amount"""
        invalid_data = self.valid_invoice_data.copy()
        invalid_data["Amount"] = -100
        with self.assertRaises(ValueError):
            Innvoice(**invalid_data)

    def test_invoice_schema(self):
        """Test invoice schema example"""
        invoice = Innvoice(**self.valid_invoice_data)
        schema = invoice.schema()
        self.assertIn("example", schema["example"])
        self.assertEqual(schema["example"]["Invoice_Number"], "INV-001")

if __name__ == '__main__':
    unittest.main()