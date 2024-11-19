from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from typing import Optional
import os
from dotenv import load_dotenv

load_dotenv()

# Get database URL from environment variable or use default SQLite database
DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///./data/cars.db')

# Create SQLAlchemy engine
engine = create_engine(DATABASE_URL)

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
    seller_type = Column(String)
    images = Column(Text)  # Store as JSON string
    url = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

def init_db():
    """Initialize the database, creating all tables"""
    Base.metadata.create_all(bind=engine)

def get_db():
    """Get database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def save_car_to_db(car_data: dict, db_session) -> Optional[Car]:
    """Save car data to database"""
    try:
        # Convert price to float
        price_str = car_data.get('price', '0').replace('$', '').replace(',', '').strip()
        price = float(price_str) if price_str.replace('.', '').isdigit() else 0.0

        # Create new car instance
        car = Car(
            listing_id=car_data.get('listing_id'),
            title=car_data.get('title'),
            price=price,
            year=car_data.get('year'),
            mileage=float(car_data.get('mileage', 0)),
            body_type=car_data.get('body_type'),
            color=car_data.get('color'),
            engine_size=float(car_data.get('engine_size', 0)),
            fuel_type=car_data.get('fuel_type'),
            transmission=car_data.get('transmission'),
            description=car_data.get('description'),
            location=car_data.get('location'),
            seller_type=car_data.get('seller_type'),
            images=str(car_data.get('images', [])),
            url=car_data.get('url')
        )

        # Add to session and commit
        db_session.add(car)
        db_session.commit()
        db_session.refresh(car)
        return car
    except Exception as e:
        db_session.rollback()
        print(f"Error saving car to database: {str(e)}")
        return None
