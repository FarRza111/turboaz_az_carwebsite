import pandas as pd
import requests
import logging
import logging.config
import os
from time import sleep
from bs4 import BeautifulSoup
from typing import List, Dict, Optional

from config import (
    BASE_URL, REQUEST_HEADERS, REQUEST_TIMEOUT, 
    DELAY_BETWEEN_REQUESTS, MAX_RETRIES,
    SELECTORS, LABEL_MAPPING, LOGGING_CONFIG,
    OUTPUT_DIRECTORY, CSV_FILENAME, DEFAULT_MAX_PAGES
)

# Initialize logging
logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)

class TurboAzScraper:
    def __init__(self):
        """Initialize the TurboAz scraper."""
        self.base_url = BASE_URL
        self.headers = REQUEST_HEADERS
        self.timeout = REQUEST_TIMEOUT
        self.delay = DELAY_BETWEEN_REQUESTS
        self.max_retries = MAX_RETRIES
        self.output_dir = OUTPUT_DIRECTORY
        self.csv_filename = CSV_FILENAME
        self.all_cars = []
        
        # Create output directory
        os.makedirs(self.output_dir, exist_ok=True)

    def fetch_page(self, url: str) -> Optional[str]:
        """Fetch page content with retries."""
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Fetching URL: {url}")
                response = requests.get(
                    url,
                    headers=self.headers,
                    timeout=self.timeout
                )
                response.raise_for_status()
                logger.info(f"Successfully fetched {url}")
                return response.text
            
            except requests.RequestException as e:
                logger.error(f"Attempt {attempt + 1}/{self.max_retries} failed: {str(e)}")
                if attempt < self.max_retries - 1:
                    sleep(self.delay * (attempt + 1))
                    continue
                return None

    def get_total_pages(self) -> int:
        """Get the total number of pages available."""
        html = self.fetch_page(self.base_url)
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
        """Get all listing IDs from a specific page."""
        url = f"{self.base_url}?page={page_number}"
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

    def parse_car_details(self, soup) -> Dict[str, str]:
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
                    
                    # Map Azerbaijani labels to English if available
                    field_name = LABEL_MAPPING.get(name, name)
                    properties[field_name] = value
                    
            except Exception as e:
                logger.error(f"Error parsing property item: {str(e)}")

        return properties

    def scrape_listing(self, listing_id: str) -> Dict:
        """Scrape details from a specific car listing."""
        url = f"{self.base_url}/{listing_id}"
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

    def display_data_summary(self, df: pd.DataFrame) -> None:
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

    def save_progress(self) -> None:
        """Save current progress to CSV file."""
        df = pd.DataFrame(self.all_cars)
        df.to_csv(os.path.join(self.output_dir, self.csv_filename), index=False)
        logger.info(f"Saved {len(self.all_cars)} cars to CSV")

    def run(self, max_pages: Optional[int] = None) -> None:
        """Run the scraper."""
        try:
            logger.info("Starting the car scraper")
            
            # Get total pages
            total_pages = self.get_total_pages()
            logger.info(f"Total pages to scrape: {total_pages}")
            
            if total_pages == 0:
                logger.error("Could not determine total pages. Exiting.")
                return
                
            # Set the maximum pages to scrape
            max_pages = min(total_pages, max_pages or DEFAULT_MAX_PAGES or total_pages)
            
            # Iterate through pages
            for page in range(1, max_pages + 1):
                logger.info(f"Processing page {page}/{max_pages}")
                
                # Get listing IDs from the current page
                listing_ids = self.get_listing_ids(page)
                
                # Process each listing
                for listing_id in listing_ids:
                    car_details = self.scrape_listing(listing_id)
                    
                    if car_details:
                        car_details['listing_id'] = listing_id
                        car_details['page_number'] = page
                        self.all_cars.append(car_details)
                        
                        # Show real-time progress
                        print(f"\rCars collected: {len(self.all_cars)}", end="")
                    
                    # Respect rate limiting
                    sleep(self.delay)
                
                # Save progress after each page
                self.save_progress()
            
            # Load the final dataset and display summary
            final_df = pd.read_csv(os.path.join(self.output_dir, self.csv_filename))
            self.display_data_summary(final_df)
            
            logger.info("Scraping completed successfully")
            
        except Exception as e:
            logger.critical(f"Unexpected error in main process: {str(e)}")
            raise

def main():
    """Main entry point."""
    scraper = TurboAzScraper()
    scraper.run()

if __name__ == '__main__':
    main()
