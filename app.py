from fastapi import FastAPI
from configparser import ConfigParser
from pydantic import BaseModel

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
    text: str
    summary_text: str
    updated_at: str


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
@connection_required
def get_bills():
    query = "SELECT * FROM bills"
    rows = db_connector.execute_query(query)
    bills = [Bill(**row) for row in rows]
    return bills


# Run the app with uvicorn server
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
