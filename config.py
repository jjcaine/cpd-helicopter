"""Configuration management for CPD Helicopter Flight Tracker."""

import os
from dotenv import load_dotenv

load_dotenv()

# Tracked aircraft ICAO codes
TRACKED_AIRCRAFT = [
    'ad389e',
    'ad3c55'
]

# Database configuration
DATABASE_CONFIG = {
    'user': os.getenv('DB_USER', 'doadmin'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST', 'db-postgresql-nyc3-13586-do-user-12959197-0.j.db.ondigitalocean.com'),
    'port': os.getenv('DB_PORT', '25060'),
    'database': os.getenv('DB_NAME', 'defaultdb'),
    'sslmode': os.getenv('DB_SSLMODE', 'require')
}


def get_database_url():
    """Construct PostgreSQL connection URL from configuration."""
    return (
        f"postgresql://{DATABASE_CONFIG['user']}:{DATABASE_CONFIG['password']}"
        f"@{DATABASE_CONFIG['host']}:{DATABASE_CONFIG['port']}/{DATABASE_CONFIG['database']}"
        f"?sslmode={DATABASE_CONFIG['sslmode']}"
    )
