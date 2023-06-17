from fastapi import FastAPI
from datetime import datetime
from configparser import ConfigParser
from pydantic import BaseModel
from typing import Optional
from database_connection import PostgresRDS

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


# Pydantic model for the Bill object
class Bill(BaseModel):
    bill_id: int
    state_code: str
    session_id: int
    body_id: int
    status_id: int
    pdf_link: str
    text: Optional[str]
    summary_text: str
    updated_at: datetime


# Define a route to get all bills
@app.get("/bills")
def get_bills(bill_id: Optional[int] = None, limit: int = 10, include_full_text: bool = False):
    columns = [
        "bill_id", "state_code", "session_id", "body_id", "status_id",
        "pdf_link", "summary_text", "updated_at"
    ]
    if include_full_text:
        columns.append("text")

    select_clause = f"SELECT {', '.join(columns)} FROM bills"
    filter_clause = f"WHERE bill_id = :bill_id" if bill_id else ""
    limit_clause = f"LIMIT :limit" if limit else ""

    query = f"{select_clause} {filter_clause} {limit_clause}"

    params = {"bill_id": bill_id, "limit": limit}

    rows = db_connector.execute_query(query, params)
    bills = [Bill(**row) for row in rows]

    return bills
