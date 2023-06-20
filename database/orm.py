from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.orm import declarative_base, relationship


Base = declarative_base()


class Bill(Base):
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

    body = relationship("LegislativeBody", back_populates="bill")


class LegislativeBody(Base):
    __tablename__ = "legislative_bodies"

    body_id = Column(Integer, primary_key=True)
    state_id = Column(String)
    role_id = Column(Integer)
    full_name = Column(String)
    abbr_name = Column(String)
    updated_at = Column(DateTime)

    bill = relationship("Bill", back_populates="body")
