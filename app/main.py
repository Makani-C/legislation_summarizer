import os
import sys

from fastapi import FastAPI
from datetime import datetime
from typing import Optional
from configparser import ConfigParser
from pydantic import BaseModel

from sqlalchemy import Column, Integer, String, DateTime, select
from sqlalchemy.orm import declarative_base

filepath = os.path.realpath(__file__)
sys.path.append(os.path.dirname(os.path.dirname(filepath)))

from utils.database_connection import PostgresRDS


# Create a FastAPI app
app = FastAPI()

# Read the database credentials from config.ini
config = ConfigParser()
config.read("config.ini")

# Initialize the database connector
db_connector = PostgresRDS(
    host=config.get("rds", "rds_host"),
    port=config.getint("rds", "rds_port"),
    user=config.get("rds", "rds_user"),
    password=config.get("rds", "rds_password"),
    database=config.get("rds", "rds_database")
)

Base = declarative_base()


class BillsORM(Base):
    __tablename__ = "bills"

    bill_id = Column(Integer, primary_key=True)
    state_code = Column(String)
    session_id = Column(Integer)
    body_id = Column(Integer)
    status_id = Column(Integer)
    pdf_link = Column(String)
    text = Column(String, nullable=True)
    summary_text = Column(String)
    updated_at = Column(DateTime)


# Pydantic model for the Bill object
class Bill(BaseModel):
    bill_id: int
    state_code: str
    session_id: int
    body_id: int
    status_id: int
    pdf_link: str
    summary_text: str
    updated_at: datetime

    class Config:
        orm_mode = True


# Define a route to get all bills
@app.get("/bills", response_model=list[Bill])
def get_bills(
        bill_id: Optional[int] = None,
        limit: int = 10
):
    db_connector.connect()

    query = db_connector.session.query(BillsORM)

    if bill_id is not None:
        query = query.filter(BillsORM.bill_id == bill_id)
    if limit:
        query = query.limit(limit)

    results = db_connector.execute_orm_query(query)

    return results


@app.get("/full_bill_text")
def get_full_bill_text(
        bill_id: int
) -> str:
    query = select(BillsORM.text).where(BillsORM.bill_id == bill_id)

    result = db_connector.execute_orm_query(query)

    return result[0]
