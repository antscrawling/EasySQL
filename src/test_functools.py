from functools import wraps, callable , time

from django.test import TestCase
from django.urls import reverse
from django.utils import timezone
from rest_framework.test import APIClient
from rest_framework import status
from .models import Task
from .serial import Task
    
    
class TestTaskAPI(TestCase):
    """Test cases for the Task API"""
    
    


   # try:
   #     db.insert_data(invoice.dict())
   #     return invoice
   # except Exception as e:
   #     raise HTTPException(status_code=400, detail=str(e))