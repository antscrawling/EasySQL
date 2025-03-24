from fastapi import FastAPI, HTTPException, Depends, status
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from Models import Easysql, TableConfig
import pandas as pd
from datetime import date

# Define data models for API
class InvoiceBase(BaseModel):
    Invoice_Number: str = Field(..., example="INV-001", description="Unique invoice number")
    Date: str = Field(..., example=date.today().isoformat(), description="Invoice date")
    Customer: str = Field(..., example="John Doe", description="Customer name")
    Amount: float = Field(..., gt=0, example=1500.00, description="Invoice amount")

    class Config:
        schema_extra = {
            "example": {
                "Invoice_Number": "INV-001",
                "Date": "2024-03-24",
                "Customer": "John Doe",
                "Amount": 1500.00
            }
        }

class InvoiceCreate(InvoiceBase):
    pass

class InvoiceUpdate(InvoiceBase):
    pass

class InvoiceInDB(InvoiceBase):
    id: int = Field(..., description="Database ID")
    
    class Config:
        orm_mode = True

# Initialize FastAPI app with metadata
app = FastAPI(
    title="EasySQL API",
    description="RESTful API for EasySQL invoice operations",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json"
)

# Database configuration
def get_db():
    """Database dependency injection"""
    config = TableConfig(
        name="invoices",
        columns=["Invoice_Number", "Date", "Customer", "Amount"],
        primary_key="Invoice_Number"
    )
    db = Easysql(table_config=config)
    try:
        yield db
    finally:
        pass

@app.get(
    "/invoices/",
    response_model=List[InvoiceInDB],
    status_code=status.HTTP_200_OK,
    summary="List all invoices",
    response_description="List of invoices"
)
async def read_invoices(
    skip: int = Field(0, ge=0, description="Number of records to skip"),
    limit: int = Field(10, ge=1, le=100, description="Number of records to return"),
    db: Easysql = Depends(get_db)
):
    """
    Retrieve a list of invoices with pagination support.
    
    - **skip**: Number of records to skip (pagination offset)
    - **limit**: Maximum number of records to return
    """
    try:
        df = db.load_file('invoice_data.csv', 'csv')
        records = df.iloc[skip:skip+limit].to_dict('records')
        return [InvoiceInDB(id=i, **record) for i, record in enumerate(records, start=1)]
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )

@app.get(
    "/invoices/{invoice_number}",
    response_model=InvoiceInDB,
    status_code=status.HTTP_200_OK,
    summary="Get invoice by number"
)
async def read_invoice(
    invoice_number: str = Field(..., description="Invoice number to retrieve"),
    db: Easysql = Depends(get_db)
):
    """
    Retrieve a specific invoice by its number.
    
    - **invoice_number**: The unique invoice number to look up
    """
    try:
        df = db.load_file('invoice_data.csv', 'csv')
        invoice = df[df['Invoice_Number'] == invoice_number]
        if invoice.empty:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Invoice not found"
            )
        return InvoiceInDB(id=invoice.index[0] + 1, **invoice.iloc[0].to_dict())
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )

@app.post(
    "/invoices/",
    response_model=InvoiceInDB,
    status_code=status.HTTP_201_CREATED,
    summary="Create new invoice"
)
async def create_invoice(
    invoice: InvoiceCreate,
    db: Easysql = Depends(get_db)
):
    """
    Create a new invoice.
    
    - **invoice**: The invoice data to create
    """
    try:
        df = db.load_file('invoice_data.csv', 'csv')
        if invoice.Invoice_Number in df['Invoice_Number'].values:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invoice number already exists"
            )
        
        new_row = pd.DataFrame([invoice.dict()])
        df = pd.concat([df, new_row], ignore_index=True)
        db.save_file('invoice_data.csv', df, 'csv')
        
        return InvoiceInDB(id=len(df), **invoice.dict())
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@app.put(
    "/invoices/{invoice_number}",
    response_model=InvoiceInDB,
    status_code=status.HTTP_200_OK,
    summary="Update existing invoice"
)
async def update_invoice(
    invoice_number: str,
    invoice_update: InvoiceUpdate,
    db: Easysql = Depends(get_db)
):
    """
    Update an existing invoice.
    
    - **invoice_number**: The invoice number to update
    - **invoice_update**: The updated invoice data
    """
    try:
        df = db.load_file('invoice_data.csv', 'csv')
        if invoice_number not in df['Invoice_Number'].values:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Invoice not found"
            )
        
        mask = df['Invoice_Number'] == invoice_number
        for key, value in invoice_update.dict().items():
            df.loc[mask, key] = value
            
        db.save_file('invoice_data.csv', df, 'csv')
        
        updated_invoice = df[df['Invoice_Number'] == invoice_number].to_dict('records')[0]
        return InvoiceInDB(id=mask.idxmax() + 1, **updated_invoice)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

def main():
    """Run the FastAPI application"""
    import uvicorn
    uvicorn.run(
        "EasySQL_API:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )

if __name__ == "__main__":
    main()