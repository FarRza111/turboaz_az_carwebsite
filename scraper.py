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

    @staticmethod
    def parse_car_details(soup: BeautifulSoup) -> Dict[str, str]:
        """
        Parse car details from the properties section.
        
        Args:
            soup: BeautifulSoup object of the car listing page
            
        Returns:
            Dict[str, str]: Dictionary of car properties
        """
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
                    
            except Exception as e:
                logger.error(f"Error parsing property item: {str(e)}")

        return properties

    def scrape_listing(self, listing_id: str) -> Dict[str, Any]:
        """
        Scrape details from a specific car listing.
        
        Args:
            listing_id: The ID of the listing to scrape
            
        Returns:
            Dict[str, Any]: Dictionary containing the car's details
        """
        url = f"{self.config.base_url}/{listing_id}"
        logger.info(f"Scraping specific listing: {url}")
        
        try:
            html = self.fetch_page(url)
            if not html:
                logger.error(f"Failed to fetch page: {url}")
                return {}
                
            soup = BeautifulSoup(html, 'lxml')
            
            # Extract basic information
            title = soup.select_one('h1.product-title')
            price = soup.select_one('div.product-price__i')
            description = soup.select_one('div.product-description')
            
            # Initialize the result dictionary
            result = {
                'title': title.text.strip() if title else None,
                'price': price.text.strip() if price else None,
                'description': description.text.strip() if description else None,
            }
            
            # Get detailed properties
            properties = self.parse_car_details(soup)
            result.update(properties)
            
            return result
            
        except Exception as e:
            logger.error(f"Error scraping listing {url}: {str(e)}")
            return {}

    @staticmethod
    def display_data_summary(df: pd.DataFrame) -> None:
        """
        Display a summary of the collected data.
        
        Args:
            df: DataFrame containing the scraped car data
        """
        print("\n" + "="*50)
        print("SCRAPING RESULTS SUMMARY")
        print("="*50)
        
        print(f"\nTotal cars collected: {len(df)}")
        
        if not df.empty:
            if 'brand' in df.columns:
                print("\nTop 5 Brands:")
                print(df['brand'].value_counts().head())
            
            if 'price' in df.columns:
                print("\nPrice Statistics:")
                print(df['price'].describe())
            
            if 'year' in df.columns:
                print("\nYear Distribution:")
                print(df['year'].value_counts().sort_index().head())
            
            print("\nSample Entries (5 random cars):")
            sample_columns = ['brand', 'model', 'year', 'price', 'mileage']
            available_columns = [col for col in sample_columns if col in df.columns]
            print(df[available_columns].sample(min(5, len(df))).to_string())
        
        print("\n" + "="*50)

    def save_data(self) -> None:
        """Save scraped data to CSV file and display summary."""
        df = pd.DataFrame(self._cars_data)
        output_path = os.path.join(self.config.output_dir, self.config.csv_filename)
        df.to_csv(output_path, index=False)
        logger.info(f"Saved {len(self._cars_data)} cars to {output_path}")
        self.display_data_summary(df)

    def run(self, max_pages: Optional[int] = None) -> None:
        """
        Run the scraper.
        
        Args:
            max_pages: Optional maximum number of pages to scrape
        """
        try:
            logger.info("Starting the car scraper")
            
            total_pages = self.get_total_pages()
            if max_pages:
                total_pages = min(total_pages, max_pages)
                
            logger.info(f"Will scrape {total_pages} pages")
            
            for page in range(1, total_pages + 1):
                logger.info(f"Processing page {page}/{total_pages}")
                listing_ids = self.get_listing_ids(page)
                
                for listing_id in listing_ids:
                    car_data = self.scrape_listing(listing_id)
                    if car_data:
                        self._cars_data.append(car_data)
                        logger.info(f"Total cars collected: {len(self._cars_data)}")
                    sleep(self.config.delay)
                
                # Save progress after each page
                if self._cars_data:
                    self.save_data()
                    
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

def main():
    """Entry point of the script."""
    scraper = TurboAzScraper.create_scraper()
    scraper.run(max_pages=DEFAULT_MAX_PAGES)

if __name__ == '__main__':
    main()
