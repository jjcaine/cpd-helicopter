"""SQLAlchemy models for flight tracking."""

from sqlalchemy import (
    Column, Integer, String, DateTime, Index, UniqueConstraint,
    ForeignKey, Float, Boolean, SmallInteger
)
from sqlalchemy.orm import declarative_base, relationship
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

    telemetry = relationship("FlightTelemetry", back_populates="flight", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint('icao', 'start_time', name='uix_icao_start_time'),
        Index('ix_start_time', 'start_time'),
    )

    def __repr__(self):
        return f"<Flight(icao={self.icao}, start={self.start_time}, end={self.end_time})>"


class FlightTelemetry(Base):
    """Represents a single telemetry point for a flight."""

    __tablename__ = 'flight_telemetry'

    id = Column(Integer, primary_key=True)
    flight_id = Column(Integer, ForeignKey('flights.id', ondelete='CASCADE'), nullable=False, index=True)
    timestamp = Column(DateTime(timezone=True), nullable=False)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    altitude = Column(Integer, nullable=True)
    altitude_ground = Column(Boolean, default=False)
    ground_speed = Column(Float, nullable=True)
    track = Column(Float, nullable=True)
    vertical_rate = Column(Integer, nullable=True)
    flags = Column(SmallInteger, nullable=True)
    geo_altitude = Column(Integer, nullable=True)
    geo_vertical_rate = Column(Integer, nullable=True)
    ias = Column(Integer, nullable=True)
    roll_angle = Column(Float, nullable=True)

    flight = relationship("Flight", back_populates="telemetry")

    __table_args__ = (
        Index('ix_telemetry_flight_timestamp', 'flight_id', 'timestamp'),
    )

    def __repr__(self):
        return f"<FlightTelemetry(flight_id={self.flight_id}, timestamp={self.timestamp})>"
