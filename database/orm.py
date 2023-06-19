from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.orm import declarative_base


Base = declarative_base()


class BaseORM(Base):
    __tablename__ = None

    updated_at = Column(DateTime)


class Bills(BaseORM):
    __tablename__ = "bills"

    bill_id = Column(Integer, primary_key=True)
    state_code = Column(String)
    session_id = Column(Integer)
    body_id = Column(Integer)
    status_id = Column(Integer)
    pdf_link = Column(String)
    text = Column(String, nullable=True)
    summary_text = Column(String)


class LegislativeBody(BaseORM):
    __tablename__ = "legislative_body"

    body_id = Column(Integer, primary_key=True)
    state_id = Column(String)
    role_id = Column(Integer)
    body_short = Column(String)
    body_name = Column(String)
    role_short = Column(String)
    role_name = Column(String)
