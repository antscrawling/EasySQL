from fastapi import FastAPI, HTTPException, Depends, status
from typing import List, Optional
from pydantic import BaseModel, Field, field_validator
from Models import Easysql, TableConfig, Invoice as ModelInvoice
import pandas as pd
from datetime import date
from decimal import Decimal

class InvoiceBase(BaseModel):
    """Base invoice model for API operations"""
    invoice_number: str = Field(..., example="INV1000", description="Unique invoice identifier")
    customer_id: int = Field(..., example=47355, description="Customer identifier")
    product_id: int = Field(..., example=1392, description="Product identifier")
    unit_cost: Decimal = Field(..., example="157.09", description="Cost per unit")
    quantity_bought: int = Field(..., gt=0, example=8, description="Quantity purchased")
    total_amount: Decimal = Field(..., example="1256.72", description="Total amount before discount")
    currency: str = Field(..., example="USD", description="Transaction currency")
    invoice_date: date = Field(..., example="2024-12-17", description="Invoice date")
    store_number: int = Field(..., gt=0, example=1, description="Store identifier")
    employee_name: str = Field(..., example="Alice", description="Employee name")
    discount: Decimal = Field(..., ge=0, example="1.74", description="Discount amount")
    net_amount: Decimal = Field(..., example="1254.98", description="Final amount after discount")

    # Validators
    @field_validator('currency')
    def validate_currency(cls, v: str) -> str:
        allowed_currencies = {"USD", "EUR", "GBP"}
        if v not in allowed_currencies:
            raise ValueError(f'Currency must be one of {allowed_currencies}')
        return v

    @field_validator('total_amount')
    def validate_total_amount(cls, v: Decimal, values: dict) -> Decimal:
        if 'unit_cost' in values.data and 'quantity_bought' in values.data:
            expected = values.data['unit_cost'] * values.data['quantity_bought']
            if abs(v - expected) > Decimal('0.01'):
                raise ValueError('Total amount must equal unit_cost * quantity_bought')
        return v

    @field_validator('net_amount')
    def validate_net_amount(cls, v: Decimal, values: dict) -> Decimal:
        if 'total_amount' in values.data and 'discount' in values.data:
            expected = values.data['total_amount'] - values.data['discount']
            if abs(v - expected) > Decimal('0.01'):
                raise ValueError('Net amount must equal total_amount - discount')
        return v

class InvoiceCreate(InvoiceBase):
    """Model for creating new invoices"""
    pass

class InvoiceUpdate(InvoiceBase):
    """Model for updating existing invoices"""
    pass

class InvoiceInDB(InvoiceBase):
    """Model for invoices as stored in database"""
    id: int = Field(..., description="Database ID")
    
    class Config:
        orm_mode = True

# Initialize FastAPI app with metadata
app = FastAPI(
    title="EasySQL API",
    description="RESTful API for EasySQL invoice operations",
    version="1.0.0"
)

# Database configuration
def get_db():
    """Database dependency injection"""
    config = TableConfig(
        name="invoices",
        columns=[
            "invoice_number", "customer_id", "product_id", "unit_cost",
            "quantity_bought", "total_amount", "currency", "invoice_date",
            "store_number", "employee_name", "discount", "net_amount"
        ],
        primary_key="invoice_number"
    )
    db = Easysql(table_config=config)
    try:
        yield db
    finally:
        pass

@app.get("/invoices/", response_model=List[InvoiceInDB])
async def read_invoices(
    skip: int = Field(0, ge=0),
    limit: int = Field(10, ge=1, le=100),
    db: Easysql = Depends(get_db)
):
    """Read all invoices with pagination"""
    try:
        df = db.load_file('invoice_data.db', 'db')
        records = df.iloc[skip:skip+limit].to_dict('records')
        return [InvoiceInDB(id=i, **record) for i, record in enumerate(records, start=1)]
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

@app.get("/invoices/{invoice_number}", response_model=InvoiceInDB)
async def read_invoice(invoice_number: str, db: Easysql = Depends(get_db)):
    """Read a specific invoice"""
    try:
        df = db.load_file('invoice_data.db', 'db')
        invoice = df[df['invoice_number'] == invoice_number]
        if invoice.empty:
            raise HTTPException(status_code=404, detail="Invoice not found")
        return InvoiceInDB(id=invoice.index[0] + 1, **invoice.iloc[0].to_dict())
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

@app.post("/invoices/", response_model=InvoiceInDB, status_code=201)
async def create_invoice(invoice: InvoiceCreate, db: Easysql = Depends(get_db)):
    """Create a new invoice"""
    try:
        df = db.load_file('invoice_data.db', 'db')
        if invoice.invoice_number in df['invoice_number'].values:
            raise HTTPException(status_code=400, detail="Invoice number already exists")
        
        new_row = pd.DataFrame([invoice.dict()])
        df = pd.concat([df, new_row], ignore_index=True)
        db.save_file('invoice_data.db', df, 'db')
        
        return InvoiceInDB(id=len(df), **invoice.dict())
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.put("/invoices/{invoice_number}", response_model=InvoiceInDB)
async def update_invoice(
    invoice_number: str,
    invoice_update: InvoiceUpdate,
    db: Easysql = Depends(get_db)
):
    """Update an existing invoice"""
    try:
        df = db.load_file('invoice_data.db', 'db')
        if invoice_number not in df['invoice_number'].values:
            raise HTTPException(status_code=404, detail="Invoice not found")
        
        mask = df['invoice_number'] == invoice_number
        for key, value in invoice_update.dict().items():
            df.loc[mask, key] = value
            
        db.save_file('invoice_data.db', df, 'db')
        
        updated_invoice = df[df['invoice_number'] == invoice_number].to_dict('records')[0]
        return InvoiceInDB(id=mask.idxmax() + 1, **updated_invoice)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/invoices/{invoice_number}", status_code=204)
async def delete_invoice(invoice_number: str, db: Easysql = Depends(get_db)):
    """Delete an invoice"""
    try:
        df = db.load_file('invoice_data.db', 'db')
        if invoice_number not in df['invoice_number'].values:
            raise HTTPException(status_code=404, detail="Invoice not found")
        
        df = df[df['invoice_number'] != invoice_number]
        db.save_file('invoice_data.db', df, 'db')
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

def main():
    """Run the FastAPI application"""
    import uvicorn
    uvicorn.run("EasySQL_API:app", host="0.0.0.0", port=8000, reload=True)

if __name__ == "__main__":
    main()