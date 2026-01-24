"""SQLAlchemy models for flight tracking."""

from sqlalchemy import Column, Integer, String, DateTime, Index, UniqueConstraint
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


class Flight(Base):
    """Represents a single flight leg."""

    __tablename__ = 'flights'

    id = Column(Integer, primary_key=True)
    icao = Column(String(6), nullable=False, index=True)
    start_time = Column(DateTime(timezone=True), nullable=False, index=True)
    end_time = Column(DateTime(timezone=True), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint('icao', 'start_time', name='uix_icao_start_time'),
        Index('ix_start_time', 'start_time'),
    )

    def __repr__(self):
        return f"<Flight(icao={self.icao}, start={self.start_time}, end={self.end_time})>"
