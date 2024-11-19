import pandas as pd
from models import init_db, Car, save_car_to_db
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def import_csv_to_db(csv_path: str):
    """Import CSV data into the database."""
    try:
        # Initialize database
        init_db()
        logger.info("Database initialized")

        # Read CSV file
        df = pd.read_csv(csv_path)
        logger.info(f"Read {len(df)} records from CSV")

        # Process each row
        for idx, row in df.iterrows():
            try:
                # Create Car instance
                car = Car(
                    listing_id=str(row['listing_id']),
                    title=row['title'],
                    price=float(row['price']) if pd.notna(row['price']) else 0.0,
                    description=row['description'],
                    location=row['location'],
                    brand=row['brand'],
                    model=row['model'],
                    year=int(row['year']) if pd.notna(row['year']) else 0,
                    body_type=row['body_type'],
                    color=row['color'],
                    engine_size=float(row['engine_size']) if pd.notna(row['engine_size']) else 0.0,
                    mileage=float(row['mileage']) if pd.notna(row['mileage']) else 0.0,
                    transmission=row['transmission'],
                    fuel_type=row['fuel_type'],
                    seller_type=row['seller_type'],
                    images=row['images'],
                    url=row['url']
                )
                
                # Save to database
                save_car_to_db(car)
                
                # Show progress
                if (idx + 1) % 10 == 0:
                    logger.info(f"Processed {idx + 1} records")
                
            except Exception as e:
                logger.error(f"Error processing row {idx}: {str(e)}")
                continue

        logger.info("CSV import completed successfully")

    except Exception as e:
        logger.error(f"Error importing CSV: {str(e)}")
        raise

if __name__ == "__main__":
    csv_path = os.path.join("data", "turbo_az_listings.csv")
    import_csv_to_db(csv_path)
