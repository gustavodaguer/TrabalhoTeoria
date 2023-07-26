import datetime

from sqlalchemy import Column, Integer, String, DateTime

from .database import Base

class History(Base):
    __tablename__ = "history"
    id = Column(Integer, primary_key=True, index=True)
    query = Column(String)
    result = Column(String)
    created_at = Column(DateTime,
                        default=datetime.datetime.utcnow)
