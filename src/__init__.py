import os
import sys
from Models import Invoice, Easysql
from datetime import date
from fastapi import FastAPI, HTTPException, Depends
from typing import List, Optional
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from tests.test_api import *

current_dir = os.path.dirname(os.path.abspath(__file__))    
data_files = (os.path.join(current_dir, 'datafiles'),)