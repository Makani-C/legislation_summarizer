from fastapi import FastAPI, Depends
from datetime import datetime
from typing import Optional
from pydantic import BaseModel

from sqlalchemy import select
from sqlalchemy.orm import Session

from database import connectors, orm

# Create a FastAPI app
app = FastAPI()

# Initialize the database connector
db_connector = connectors.RDSConnection()
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
    summary_text: Optional[str] = ""
    updated_at: datetime

    class Config:
        orm_mode = True


# Define a route to get all bills
@app.get("/bills", response_model=list[Bill])
def get_bills(
    bill_id: Optional[int] = None,
    state_abbr: Optional[str] = None,
    limit: int = 10,
    db: Session = Depends(get_db),
):
    query = db.query(orm.Bill)

    if bill_id is not None:
        query = query.filter(orm.Bill.bill_id == bill_id)
    elif state_abbr is not None:
        query = query.filter(orm.Bill.state_code == state_abbr)
    if limit:
        query = query.limit(limit)

    results = db.execute(query).scalars().all()

    return results


@app.get("/full_bill_text")
def get_full_bill_text(bill_id: int) -> str:
    query = select(orm.Bill.text).where(orm.Bill.bill_id == bill_id)

    result = db_connector.execute_orm_query(query)

    return result[0]
