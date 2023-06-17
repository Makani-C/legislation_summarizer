from fastapi import FastAPI
from datetime import datetime
from configparser import ConfigParser
from pydantic import BaseModel
from typing import Optional

from database_connection import PostgresRDS, connection_required

# Create a FastAPI app
app = FastAPI()

# Read the database credentials from config.ini
config = ConfigParser()
config.read("config.ini")


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


# Initialize the database connector
db_connector = PostgresRDS(
    host=config.get("rds", "rds_host"),
    port=config.get("rds", "rds_port"),
    user=config.get("rds", "rds_user"),
    password=config.get("rds", "rds_password"),
    database=config.get("rds", "rds_database")
)


# Define a route to get all bills
@app.get("/bills")
def get_bills(limit: int = 10, include_full_text: bool = False):
    columns = [
        "bill_id", "state_code", "session_id", "body_id", "status_id",
        "pdf_link", "text", "updated_at"
    ]
    if include_full_text:
        columns.append("summary_text")
    query = f"SELECT {', '.join(columns)} FROM bills LIMIT {limit};"
    rows = db_connector.execute_query(query)
    bills = [Bill(**row) for row in rows]

    return bills
