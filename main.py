import pandas as pd
import requests
import logging
import logging.config
import os
import json
from time import sleep
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
from datetime import datetime

from models import init_db, Car, save_car_to_db
from config import (
    BASE_URL, REQUEST_HEADERS, REQUEST_TIMEOUT, 
    DELAY_BETWEEN_REQUESTS, MAX_RETRIES,
    SELECTORS, LABEL_MAPPING, LOGGING_CONFIG,
    OUTPUT_DIRECTORY, CSV_FILENAME, DEFAULT_MAX_PAGES
)

# Initialize logging
logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)

def fetch_page(url: str) -> Optional[str]:
    """Fetch page content with retries."""
    for attempt in range(MAX_RETRIES):
        try:
            # Info: Normal operation status
            logger.info(f"Fetching URL: {url}")
            
            response = requests.get(url, headers=REQUEST_HEADERS, timeout=REQUEST_TIMEOUT)
            
            # Debug: Detailed information for troubleshooting
            logger.debug(f"Response headers: {dict(response.headers)}")
            
            if response.status_code == 200:
                # Info: Successful operation
                logger.info(f"Successfully fetched {url}")
                return response.text
            elif response.status_code == 429:
                # Warning: Rate limiting (important but not fatal)
                logger.warning(f"Rate limit hit while fetching {url}, waiting before retry")
                sleep(DELAY_BETWEEN_REQUESTS * 2)
            else:
                # Error: Request failed but we can retry
                logger.error(f"Failed to fetch {url}, status code: {response.status_code}")
        
        except requests.exceptions.Timeout:
            # Warning: Timeout can be temporary
            logger.warning(f"Timeout while fetching {url}, attempt {attempt + 1} of {MAX_RETRIES}")
        except requests.exceptions.RequestException as e:
            # Error: More serious connection issues
            logger.error(f"Request failed for {url}: {str(e)}")
        
        if attempt < MAX_RETRIES - 1:
            sleep(DELAY_BETWEEN_REQUESTS)
        else:
            # Critical: All retries failed
            logger.critical(f"All attempts to fetch {url} failed after {MAX_RETRIES} retries")
    
    return None

def process_page(url: str) -> pd.DataFrame:
    """Process a single page and extract car details."""
    # Info: Starting new operation
    logger.info(f"Processing page: {url}")
    
    all_cars_data = []
    html = fetch_page(url)
    
    if not html:
        # Warning: No data but not necessarily an error
        logger.warning(f"No HTML content retrieved for {url}")
        return pd.DataFrame()

    soup = BeautifulSoup(html, 'html.parser')
    car_containers = soup.find_all(class_=SELECTORS['car_container'].split('.')[-1])
    
    # Debug: Detailed processing information
    logger.debug(f"Found {len(car_containers)} car containers in HTML")
    
    if not car_containers:
        # Warning: Unexpected but not fatal
        logger.warning(f"No car containers found on page {url}")

    for idx, container in enumerate(car_containers, 1):
        try:
            # Debug: Processing progress
            logger.debug(f"Processing car {idx} of {len(car_containers)}")
            
            properties_div = container.find(class_=SELECTORS['properties_column'].split('.')[-1])
            if properties_div:
                car_details = parse_car_details(properties_div)
                all_cars_data.append(car_details)
            else:
                # Warning: Missing data for one item
                logger.warning(f"No properties div found for car {idx}")
        
        except Exception as e:
            # Error: Processing failed for one item
            logger.error(f"Error processing car {idx}: {str(e)}")

    # Info: Operation completion status
    logger.info(f"Successfully processed {len(all_cars_data)} cars from {url}")
    return pd.DataFrame(all_cars_data)

def get_all_listing_ids(page_number: int) -> List[str]:
    """Get all listing IDs from a specific page."""
    url = f"{BASE_URL}?page={page_number}"
    html = fetch_page(url)
    if not html:
        return []

    soup = BeautifulSoup(html, 'html.parser')
    listing_ids = []
    product_links = soup.find_all(class_=SELECTORS['product_link'].split('.')[-1])
    logger.info(f"Found {len(product_links)} product links")

    for link in product_links:
        href = link.get('href')
        if href:
            try:
                listing_id = href.split('/')[2].split('-')[0]
                listing_ids.append(listing_id)
            except IndexError:
                logger.error(f"Could not parse listing ID from href: {href}")

    return listing_ids

def parse_car_details(soup) -> Dict[str, str]:
    properties = {}
    property_items = soup.find_all(class_=SELECTORS['property_item'].split('.')[-1])
    logger.debug(f"Found {len(property_items)} property items")

    for item in property_items:
        try:
            label = item.find(class_=SELECTORS['property_name'].split('.')[-1]).text.strip()
            value = item.find(class_=SELECTORS['property_value'].split('.')[-1]).text.strip()
            english_label = LABEL_MAPPING.get(label, label)
            properties[english_label] = value
        except Exception as e:
            logger.error(f"Error parsing property item: {str(e)}")

    return properties

def extract_price(price_text: str) -> float:
    """Extract numeric price value from text."""
    try:
        if not price_text:
            return 0.0
        # Remove currency and spaces, convert to float
        price = float(''.join(filter(str.isdigit, price_text)))
        return price
    except Exception as e:
        logger.error(f"Error extracting price: {str(e)}")
        return 0.0

def extract_engine_size(engine_text: str) -> float:
    """Extract engine size from text."""
    try:
        if not engine_text:
            return 0.0
        # Extract numeric value from string like "1.6 L"
        engine_size = float(''.join(c for c in engine_text.split()[0] if c.isdigit() or c == '.'))
        return engine_size
    except Exception as e:
        logger.error(f"Error extracting engine size: {str(e)}")
        return 0.0

def extract_fuel_type(properties: Dict, title: str) -> str:
    """Extract fuel type from properties or title."""
    try:
        # First try to get from properties
        fuel_type = properties.get('fuel_type')
        if fuel_type:
            return fuel_type

        # Common fuel types in Azerbaijani
        fuel_types = {
            'Benzin': 'Gasoline',
            'Dizel': 'Diesel',
            'Qaz': 'Gas',
            'Elektro': 'Electric',
            'Hibrid': 'Hybrid'
        }

        # Try to get from engine_size property which sometimes contains fuel type
        engine_info = properties.get('engine_size', '').lower()
        if 'hibrid' in engine_info:
            return 'Hybrid'
        elif 'elektro' in engine_info:
            return 'Electric'
        elif 'dizel' in engine_info or 'diesel' in engine_info:
            return 'Diesel'
        elif 'benzin' in engine_info:
            return 'Gasoline'
        elif 'qaz' in engine_info:
            return 'Gas'

        # Try to extract from title
        title_lower = title.lower()
        for az_type, en_type in fuel_types.items():
            if az_type.lower() in title_lower:
                return en_type

        # Default to most common fuel type if nothing found
        return 'Gasoline'

    except Exception as e:
        logger.error(f"Error extracting fuel type: {str(e)}")
        return 'Gasoline'  # Default to most common fuel type

def scrape_specific_listing(url: str) -> Dict:
    """Scrape details from a specific car listing URL."""
    logger.info(f"Scraping specific listing: {url}")
    
    try:
        html = fetch_page(url)
        if not html:
            logger.error(f"Failed to fetch page: {url}")
            return {}
            
        soup = BeautifulSoup(html, 'lxml')
        
        # Extract basic information
        title_elem = soup.select_one('h1.product-title')
        price_elem = soup.select_one('div.product-price__i')
        description = soup.select_one('div.product-description')
        
        if not title_elem:
            logger.error("No title found, skipping listing")
            return {}
            
        title = title_elem.text.strip()
        
        # Get detailed properties
        properties = parse_car_details(soup)
        if not properties:
            logger.error("No properties found, skipping listing")
            return {}
        
        # Extract images
        image_elements = soup.select('div.product-photos__img img')
        images = [img.get('src') for img in image_elements if img.get('src')]
        
        # Process car data
        car_data = {
            'title': title,
            'price': extract_price(price_elem.text.strip() if price_elem else None),
            'description': description.text.strip() if description else None,
            'location': properties.get('location'),
            'brand': properties.get('brand'),
            'model': properties.get('model'),
            'year': int(properties.get('year', 0)),
            'body_type': properties.get('body_type'),
            'color': properties.get('color'),
            'engine_size': extract_engine_size(properties.get('engine_size')),
            'mileage': float(properties.get('mileage', '0').replace(' ', '').replace('km', '')),
            'transmission': properties.get('transmission'),
            'fuel_type': extract_fuel_type(properties, title),
            'seller_type': 'Dealer' if soup.select_one('div.shop-contact') else 'Private',
            'images': json.dumps(images) if images else None,
            'url': url
        }
        
        # Log extracted data
        logger.info(f"Extracted car data: {car_data}")
        return car_data
        
    except Exception as e:
        logger.error(f"Error scraping listing {url}: {str(e)}")
        return {}

def save_to_database(car_data: Dict) -> None:
    """Save car data to database."""
    try:
        if not car_data or not car_data.get('listing_id'):
            logger.error("Invalid car data or missing listing_id")
            return

        # Create Car instance
        car = Car(
            listing_id=car_data['listing_id'],
            title=car_data['title'],
            price=car_data['price'],
            description=car_data['description'],
            location=car_data['location'],
            brand=car_data['brand'],
            model=car_data['model'],
            year=car_data['year'],
            body_type=car_data['body_type'],
            color=car_data['color'],
            engine_size=car_data['engine_size'],
            mileage=car_data['mileage'],
            transmission=car_data['transmission'],
            fuel_type=car_data['fuel_type'],
            seller_type=car_data['seller_type'],
            images=car_data['images'],
            url=car_data['url']
        )
        
        # Save to database
        save_car_to_db(car)
        logger.info(f"Successfully saved car {car_data['listing_id']} to database")
        
    except Exception as e:
        logger.error(f"Error saving car to database: {str(e)}")
        raise  # Re-raise to handle in main

def get_total_pages() -> int:
    """Get the total number of pages available."""
    html = fetch_page(BASE_URL)
    if not html:
        return 0
    
    soup = BeautifulSoup(html, 'lxml')
    pagination = soup.find('div', class_='pagination')
    if not pagination:
        return 1
        
    try:
        last_page = pagination.find_all('a')[-2].text.strip()
        return int(last_page)
    except (IndexError, ValueError):
        logger.error("Could not determine total pages")
        return 0

def display_data_summary(df: pd.DataFrame) -> None:
    """Display a summary of the collected data."""
    print("\n" + "="*50)
    print("SCRAPING RESULTS SUMMARY")
    print("="*50)
    
    # Basic statistics
    print(f"\nTotal cars collected: {len(df)}")
    
    if not df.empty:
        # Brand distribution
        if 'brand' in df.columns:
            print("\nTop 5 Brands:")
            print(df['brand'].value_counts().head())
        
        # Price statistics
        if 'price' in df.columns:
            print("\nPrice Statistics:")
            print(df['price'].describe())
        
        # Year distribution
        if 'year' in df.columns:
            print("\nYear Distribution:")
            print(df['year'].value_counts().sort_index().head())
        
        # Sample entries
        print("\nSample Entries (5 random cars):")
        sample_columns = ['brand', 'model', 'year', 'price', 'mileage']
        available_columns = [col for col in sample_columns if col in df.columns]
        print(df[available_columns].sample(min(5, len(df))).to_string())
    
    print("\n" + "="*50)

def main():
    """Main function to run the scraper."""
    try:
        logger.info("Starting the car scraper")
        
        # Create data directory if it doesn't exist
        os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)
        
        # Initialize database
        init_db()
        logger.info("Database initialized")
        
        # Get total pages
        total_pages = get_total_pages()
        logger.info(f"Total pages to scrape: {total_pages}")
        
        if total_pages == 0:
            logger.error("Could not determine total pages. Exiting.")
            return
            
        # Initialize list to store all car details
        all_cars = []
        saved_to_db = 0
        
        # Set the maximum pages to scrape
        max_pages = min(total_pages, DEFAULT_MAX_PAGES) if DEFAULT_MAX_PAGES else total_pages
        
        # Iterate through pages
        for page in range(1, max_pages + 1):
            logger.info(f"Processing page {page}/{max_pages}")
            
            # Get listing IDs from the current page
            listing_ids = get_all_listing_ids(page)
            
            # Process each listing
            for listing_id in listing_ids:
                try:
                    listing_url = f"{BASE_URL}/{listing_id}"
                    car_details = scrape_specific_listing(listing_url)
                    
                    if car_details:
                        car_details['listing_id'] = listing_id
                        all_cars.append(car_details)
                        
                        # Save to database
                        save_to_database(car_details)
                        saved_to_db += 1
                        
                        # Show real-time progress
                        print(f"\rCars collected: {len(all_cars)} (Saved to DB: {saved_to_db})", end="")
                    
                    # Respect rate limiting
                    sleep(DELAY_BETWEEN_REQUESTS)
                    
                except Exception as e:
                    logger.error(f"Error processing listing {listing_id}: {str(e)}")
                    continue
            
            # Save progress to CSV after each page
            if all_cars:
                df = pd.DataFrame(all_cars)
                df.to_csv(os.path.join(OUTPUT_DIRECTORY, CSV_FILENAME), index=False)
                logger.info(f"Saved {len(all_cars)} cars to CSV")

        # Display final summary
        if all_cars:
            print(f"\nFinal Statistics:")
            print(f"Total cars collected: {len(all_cars)}")
            print(f"Total cars saved to database: {saved_to_db}")
            display_data_summary(pd.DataFrame(all_cars))
        
        logger.info("Scraping completed successfully")
        
    except Exception as e:
        logger.critical(f"Unexpected error in main process: {str(e)}")
        raise

if __name__ == '__main__':
    main()