import os
import sys

from fastapi import FastAPI, Depends
from datetime import datetime
from typing import Optional
from configparser import ConfigParser
from pydantic import BaseModel

from sqlalchemy import select
from sqlalchemy.orm import Session

filepath = os.path.realpath(__file__)
root_dir = os.path.dirname(os.path.dirname(filepath))
sys.path.append(root_dir)

from database import connection, orm

# Create a FastAPI app
app = FastAPI()

# Read the database credentials from config.ini
config = ConfigParser()
config.read(f"{root_dir}/config.ini")

# Initialize the database connector
db_connector = connection.PostgresDB(
    host=config.get("rds", "rds_host"),
    port=config.getint("rds", "rds_port"),
    user=config.get("rds", "rds_user"),
    password=config.get("rds", "rds_password"),
    database=config.get("rds", "rds_database")
)
SessionLocal = db_connector.sessionmaker()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


class LegislativeBody(BaseModel):
    body_id: int
    state_id: int
    role_id: int
    full_name: str
    abbr_name: str
    updated_at: datetime

    class Config:
        orm_mode = True


# Pydantic model for the Bill object
class Bill(BaseModel):
    bill_id: int
    state_code: str
    session_id: int
    body: LegislativeBody
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
        limit: int = 10,
        db: Session = Depends(get_db)
):

    query = db.query(orm.Bill)

    if bill_id is not None:
        query = query.filter(orm.Bill.bill_id == bill_id)
    if limit:
        query = query.limit(limit)

    results = db.execute(query).scalars().all()

    return results



@app.get("/full_bill_text")
def get_full_bill_text(
        bill_id: int
) -> str:
    query = select(orm.Bill.text).where(orm.Bill.bill_id == bill_id)

    result = db_connector.execute_orm_query(query)

    return result[0]
