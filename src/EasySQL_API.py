from fastapi import FastAPI, HTTPException, Depends
from typing import List, Optional
from pydantic import BaseModel
from Models import Easysql, TableConfig
import pandas as pd

# Define data models for API
class InvoiceBase(BaseModel):
    Invoice_Number: str
    Date: str
    Customer: str
    Amount: float

class InvoiceCreate(InvoiceBase):
    pass

class InvoiceUpdate(InvoiceBase):
    pass

class InvoiceInDB(InvoiceBase):
    id: int
    class Config:
        orm_mode = True

# Initialize FastAPI app
app = FastAPI(title="EasySQL API", description="API for EasySQL operations")

# Database dependency
def get_db():
    config = TableConfig(
        name="invoices",
        columns=["Invoice_Number", "Date", "Customer", "Amount"],
        primary_key="Invoice_Number"
    )
    db = Easysql(table_config=config)
    try:
        yield db
    finally:
        pass  # Add cleanup if needed

@app.get("/invoices/", response_model=List[InvoiceInDB])
async def read_invoices(skip: int = 0, limit: int = 10, db: Easysql = Depends(get_db)):
    """Get all invoices with pagination"""
    try:
        df = db.load_file('invoice_data.csv', 'csv')
        records = df.iloc[skip:skip+limit].to_dict('records')
        return [InvoiceInDB(id=i, **record) for i, record in enumerate(records, start=1)]
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

@app.get("/invoices/{invoice_number}")
async def read_invoice(invoice_number: str, db: Easysql = Depends(get_db)):
    """Get a specific invoice by number"""
    try:
        df = db.load_file('invoice_data.csv', 'csv')
        invoice = df[df['Invoice_Number'] == invoice_number]
        if invoice.empty:
            raise HTTPException(status_code=404, detail="Invoice not found")
        return invoice.to_dict('records')[0]
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

@app.post("/invoices/", response_model=InvoiceInDB)
async def create_invoice(invoice: InvoiceCreate, db: Easysql = Depends(get_db)):
    """Create a new invoice"""
    try:
        df = db.load_file('invoice_data.csv', 'csv')
        if invoice.Invoice_Number in df['Invoice_Number'].values:
            raise HTTPException(status_code=400, detail="Invoice number already exists")
        
        new_row = pd.DataFrame([invoice.dict()])
        df = pd.concat([df, new_row], ignore_index=True)
        db.save_file('invoice_data.csv', df, 'csv')
        
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
        df = db.load_file('invoice_data.csv', 'csv')
        if invoice_number not in df['Invoice_Number'].values:
            raise HTTPException(status_code=404, detail="Invoice not found")
        
        # Update the invoice
        mask = df['Invoice_Number'] == invoice_number
        for key, value in invoice_update.dict().items():
            df.loc[mask, key] = value
            
        db.save_file('invoice_data.csv', df, 'csv')
        
        updated_invoice = df[df['Invoice_Number'] == invoice_number].to_dict('records')[0]
        return InvoiceInDB(id=mask.idxmax() + 1, **updated_invoice)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

def main():
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

if __name__ == "__main__":
    main()