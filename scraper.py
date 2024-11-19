import pandas as pd
import requests
import logging
import logging.config
import os
from time import sleep
from bs4 import BeautifulSoup
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, field
from copy import deepcopy
from datetime import datetime

from models import init_db, SessionLocal, save_car_to_db, Car

from config import (
    BASE_URL, REQUEST_HEADERS, REQUEST_TIMEOUT,
    DELAY_BETWEEN_REQUESTS, MAX_RETRIES,
    SELECTORS, LABEL_MAPPING, LOGGING_CONFIG,
    OUTPUT_DIRECTORY, CSV_FILENAME, DEFAULT_MAX_PAGES
)


# Initialize logging
logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)


@dataclass
class ScrapingConfig:
    """Configuration class for scraping parameters."""
    base_url: str = BASE_URL
    headers: Dict[str, str] = field(default_factory=lambda: deepcopy(REQUEST_HEADERS))
    timeout: int = REQUEST_TIMEOUT
    delay: int = DELAY_BETWEEN_REQUESTS
    max_retries: int = MAX_RETRIES
    output_dir: str = OUTPUT_DIRECTORY
    csv_filename: str = CSV_FILENAME


class TurboAzScraper:
    """A web scraper for Turbo.az car listings using class methods to avoid instance state."""

    def __init__(self):
        """Initialize the scraper and create output directory."""
        self.config = ScrapingConfig()
        os.makedirs(self.config.output_dir, exist_ok=True)
        self._cars_data: List[Dict[str, Any]] = []
        self.db = SessionLocal()

    def __del__(self):
        """Cleanup database session."""
        if hasattr(self, 'db'):
            self.db.close()

    @classmethod
    def create_scraper(cls) -> 'TurboAzScraper':
        """Factory method to create a scraper instance."""
        return cls()

    def fetch_page(self, url: str) -> Optional[str]:
        """
        Fetch page content with retries.

        Args:
            url: The URL to fetch

        Returns:
            Optional[str]: The page content if successful, None otherwise
        """
        for attempt in range(self.config.max_retries):
            try:
                logger.info(f"Fetching URL: {url}")
                response = requests.get(
                    url,
                    headers=self.config.headers,
                    timeout=self.config.timeout
                )
                response.raise_for_status()
                logger.info(f"Successfully fetched {url}")
                return response.text

            except requests.RequestException as e:
                logger.error(f"Attempt {attempt + 1}/{self.config.max_retries} failed: {str(e)}")
                if attempt < self.config.max_retries - 1:
                    sleep(self.config.delay * (attempt + 1))
                    continue
        return None

    def get_total_pages(self) -> int:
        """
        Get the total number of pages available.

        Returns:
            int: Total number of pages, 0 if unable to determine
        """
        html = self.fetch_page(self.config.base_url)
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

    def get_listing_ids(self, page_number: int) -> List[str]:
        """
        Get all listing IDs from a specific page.

        Args:
            page_number: The page number to scrape

        Returns:
            List[str]: List of listing IDs found on the page
        """
        url = f"{self.config.base_url}?page={page_number}"
        html = self.fetch_page(url)
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

    def parse_car_details(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Parse car details from the properties section."""
        properties = {}
        property_items = soup.find_all(class_=SELECTORS['property_item'].split('.')[-1])
        logger.debug(f"Found {len(property_items)} property items")

        for item in property_items:
            try:
                name_elem = item.find(class_=SELECTORS['property_name'].split('.')[-1])
                value_elem = item.find(class_=SELECTORS['property_value'].split('.')[-1])

                if name_elem and value_elem:
                    name = name_elem.text.strip()
                    value = value_elem.text.strip()
                    field_name = LABEL_MAPPING.get(name, name)
                    properties[field_name] = value
                    logger.info(f"Parsed property: {name} -> {field_name}: {value}")

            except Exception as e:
                logger.error(f"Error parsing property item: {str(e)}")

        logger.info(f"All parsed properties: {properties}")
        return properties

    def extract_price(self, soup):
        """Extract price from the listing"""
        try:
            price_element = soup.select_one('div.product-price__i')
            if price_element:
                price_text = price_element.get_text(strip=True)
                # Remove currency and spaces, convert to float
                price = float(''.join(filter(str.isdigit, price_text)))
                return price
            return 0.0
        except Exception as e:
            logger.error(f"Error extracting price: {str(e)}")
            return 0.0

    def extract_engine_size(self, properties):
        """Extract engine size from properties"""
        try:
            engine_text = properties.get('engine_size', '')
            if engine_text:
                # Extract numeric value from string like "1.6 L"
                engine_size = float(''.join(c for c in engine_text if c.isdigit() or c == '.'))
                return engine_size
            return 0.0
        except Exception as e:
            logger.error(f"Error extracting engine size: {str(e)}")
            return 0.0

    def extract_fuel_type(self, properties, title):
        """Extract fuel type from properties or title"""
        try:
            # First try to get from properties
            fuel_type = properties.get('fuel_type')
            if fuel_type:
                return fuel_type

            # If not in properties, try to extract from title
            fuel_types = {
                'Benzin': 'Gasoline',
                'Dizel': 'Diesel',
                'Qaz': 'Gas',
                'Elektro': 'Electric',
                'Hibrid': 'Hybrid'
            }
            
            for az_type, en_type in fuel_types.items():
                if az_type in title:
                    return en_type
                    
            return None
        except Exception as e:
            logger.error(f"Error extracting fuel type: {str(e)}")
            return None

    def extract_seller_type(self, soup):
        """Extract seller type from the listing"""
        try:
            # Check for shop/dealer indicators
            shop_contact = soup.select_one('div.shop-contact')
            if shop_contact:
                return 'Dealer'
                
            # Check for private seller indicators
            seller_info = soup.select_one('div.product-owner__info')
            if seller_info:
                return 'Private'
                
            return None
        except Exception as e:
            logger.error(f"Error extracting seller type: {str(e)}")
            return None

    def extract_images(self, soup):
        """Extract image URLs from the listing"""
        try:
            image_elements = soup.select('div.product-photos__img img')
            images = []
            for img in image_elements:
                src = img.get('src')
                if src:
                    images.append(src)
            return images if images else None
        except Exception as e:
            logger.error(f"Error extracting images: {str(e)}")
            return None

    def extract_description(self, soup):
        """Extract description safely"""
        try:
            desc_elem = soup.select_one('div.product-description')
            return desc_elem.get_text(strip=True) if desc_elem else None
        except Exception as e:
            logger.error(f"Error extracting description: {str(e)}")
            return None

    def process_car_data(self, soup, url):
        """Process and extract all car data from a listing page"""
        try:
            # Extract basic information
            title_elem = soup.select_one('h1.product-title')
            if not title_elem:
                logger.error("Could not find title element")
                return None
                
            title = title_elem.get_text(strip=True)
            listing_id = url.split('/')[-1]
            
            # Extract properties
            properties = self.parse_car_details(soup)
            if not properties:
                logger.error("No properties found")
                return None

            # Convert year and mileage
            try:
                year = int(properties.get('year', 0))
            except (ValueError, TypeError):
                year = 0
                
            try:
                mileage = float(properties.get('mileage', '0').replace(' ', '').replace('km', ''))
            except (ValueError, TypeError):
                mileage = 0.0

            # Build car data dictionary
            car_data = {
                'title': title,
                'price': self.extract_price(soup),
                'description': self.extract_description(soup),
                'location': properties.get('location'),
                'brand': properties.get('brand'),
                'model': properties.get('model'),
                'year': year,
                'body_type': properties.get('body_type'),
                'color': properties.get('color'),
                'engine_size': self.extract_engine_size(properties),
                'mileage': mileage,
                'transmission': properties.get('transmission'),
                'listing_id': listing_id,
                'url': url,
                'fuel_type': self.extract_fuel_type(properties, title),
                'seller_type': self.extract_seller_type(soup),
                'images': self.extract_images(soup)
            }
            
            # Log the extracted data
            logger.info(f"Extracted car data: {car_data}")
            return car_data
            
        except Exception as e:
            logger.error(f"Error processing car data: {str(e)}")
            return None

    def scrape_listing(self, listing_id: str) -> Dict[str, Any]:
        """Scrape details from a specific car listing."""
        url = f"{self.config.base_url}/{listing_id}"
        logger.info(f"Scraping listing: {url}")

        try:
            html = self.fetch_page(url)
            if not html:
                logger.error(f"Failed to fetch page: {url}")
                return {}

            soup = BeautifulSoup(html, 'lxml')
            return self.process_car_data(soup, url)
        except Exception as e:
            logger.error(f"Error scraping listing {url}: {str(e)}")
            return {}

    def save_data(self) -> None:
        """Save scraped data to database and CSV."""
        try:
            if not self._cars_data:
                logger.warning("No data to save")
                return

            logger.info(f"Attempting to save {len(self._cars_data)} cars to database")
            
            # Create data directory if it doesn't exist
            data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
            os.makedirs(data_dir, exist_ok=True)
            
            # Initialize database
            init_db()
            
            # Save to database
            saved_count = 0
            for car_data in self._cars_data:
                if not car_data:
                    continue
                    
                try:
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
                        url=car_data['url'],
                        fuel_type=car_data['fuel_type'],
                        seller_type=car_data['seller_type'],
                        images=str(car_data['images']) if car_data['images'] else None
                    )
                    
                    # Save to database
                    save_car_to_db(car)
                    saved_count += 1
                    logger.info(f"Successfully saved car {car_data['listing_id']} to database")
                except Exception as e:
                    logger.error(f"Error saving car {car_data.get('listing_id', 'unknown')} to database: {str(e)}")
                    continue
            
            logger.info(f"Successfully saved {saved_count} cars to database")
            
            # Save to CSV
            if saved_count > 0:
                csv_file = os.path.join(data_dir, f'cars_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
                df = pd.DataFrame(self._cars_data)
                df.to_csv(csv_file, index=False, encoding='utf-8')
                logger.info(f"Successfully saved data to CSV: {csv_file}")
            
            logger.info("Data saving completed successfully")
            
        except Exception as e:
            logger.error(f"Error in save_data: {str(e)}")
            raise

    def scrape(self) -> None:
        """Main scraping function."""
        try:
            logger.info("Starting scraping process")
            self._cars_data = []
            
            total_pages = self.get_total_pages()
            logger.info(f"Total pages to scrape: {total_pages}")
            
            for page in range(1, total_pages + 1):
                logger.info(f"Scraping page {page}/{total_pages}")
                
                listing_ids = self.get_listing_ids(page)
                logger.info(f"Found {len(listing_ids)} listings on page {page}")
                
                for listing_id in listing_ids:
                    try:
                        car_data = self.scrape_listing(listing_id)
                        if car_data:
                            self._cars_data.append(car_data)
                            logger.info(f"Successfully scraped car {listing_id}. Total cars: {len(self._cars_data)}")
                        sleep(self.config.delay)
                    except Exception as e:
                        logger.error(f"Error scraping listing {listing_id}: {str(e)}")
                        continue
                
                # Save progress periodically
                if len(self._cars_data) >= 20:  # Save every 20 cars
                    self.save_data()
                    self._cars_data = []  # Clear after saving
                    logger.info("Saved progress and cleared buffer")
            
            # Final save
            if self._cars_data:
                self.save_data()
                logger.info("Scraping completed successfully")
            
        except Exception as e:
            logger.error(f"Error in scrape: {str(e)}")
            if self._cars_data:  # Try to save what we have
                self.save_data()
            raise
        finally:
            self.db.close()

    def run(self, max_pages: Optional[int] = None) -> None:
        """
        Run the scraper.

        Args:
            max_pages: Optional maximum number of pages to scrape
        """
        try:
            logger.info("Starting the car scraper")
            init_db()  # Initialize database tables

            total_pages = self.get_total_pages()
            if max_pages:
                total_pages = min(total_pages, max_pages)

            logger.info(f"Will scrape {total_pages} pages")

            self.scrape()

            logger.info("Scraping completed successfully")

        except KeyboardInterrupt:
            logger.info("Scraping interrupted by user")
            if self._cars_data:
                self.save_data()
        except Exception as e:
            logger.critical(f"Unexpected error: {str(e)}")
            if self._cars_data:
                self.save_data()
            raise
        finally:
            self.db.close()


def main():
    """Entry point of the script."""
    scraper = TurboAzScraper.create_scraper()
    scraper.run(max_pages=DEFAULT_MAX_PAGES)


if __name__ == '__main__':
    main()
