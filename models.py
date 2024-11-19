from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from typing import Optional
import os
from dotenv import load_dotenv
import logging

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get database URL from environment variable or use default SQLite database
DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///./data/cars.db')

# Ensure data directory exists
os.makedirs('./data', exist_ok=True)

# Create SQLAlchemy engine
engine = create_engine(DATABASE_URL, echo=True)  # Added echo=True for debugging

# Create declarative base
Base = declarative_base()

# Create SessionLocal class
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

class Car(Base):
    """SQLAlchemy model for car listings"""
    __tablename__ = "cars"

    id = Column(Integer, primary_key=True, index=True)
    listing_id = Column(String, unique=True, index=True)
    title = Column(String)
    price = Column(Float)
    year = Column(Integer)
    mileage = Column(Float)
    body_type = Column(String)
    color = Column(String)
    engine_size = Column(Float)
    fuel_type = Column(String)
    transmission = Column(String)
    description = Column(Text)
    location = Column(String)
    brand = Column(String)  # Added missing columns
    model = Column(String)  # Added missing columns
    seller_type = Column(String)
    images = Column(Text)  # Store as JSON string
    url = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

def init_db():
    """Initialize database and create tables."""
    try:
        # Drop all tables first to ensure clean state
        Base.metadata.drop_all(bind=engine)
        # Create all tables
        Base.metadata.create_all(bind=engine)
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        raise

def get_db():
    """Get database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def save_car_to_db(car: Car) -> None:
    """Save a car instance to the database."""
    session = SessionLocal()
    try:
        # Check if car already exists
        existing_car = session.query(Car).filter_by(listing_id=car.listing_id).first()
        
        if existing_car:
            # Update existing car
            for key, value in car.__dict__.items():
                if not key.startswith('_'):
                    setattr(existing_car, key, value)
            existing_car.updated_at = datetime.utcnow()
            logger.info(f"Updated existing car {car.listing_id}")
        else:
            # Add new car
            car.created_at = datetime.utcnow()
            car.updated_at = datetime.utcnow()
            session.add(car)
            logger.info(f"Added new car {car.listing_id}")
        
        session.commit()
        logger.info(f"Successfully saved car {car.listing_id} to database")
        
    except Exception as e:
        logger.error(f"Error saving car to database: {str(e)}")
        session.rollback()
        raise
    finally:
        session.close()
