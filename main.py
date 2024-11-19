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

def scrape_specific_listing(url: str) -> Dict:
    """
    Scrape details from a specific car listing URL.
    
    Args:
        url (str): The complete URL of the car listing
        
    Returns:
        Dict: Dictionary containing the car details
    """
    logger.info(f"Scraping specific listing: {url}")
    
    try:
        html = fetch_page(url)
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
        properties = parse_car_details(soup)
        result.update(properties)
        
        return result
        
    except Exception as e:
        logger.error(f"Error scraping listing {url}: {str(e)}")
        return {}

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
        
        # Get total pages
        total_pages = get_total_pages()
        logger.info(f"Total pages to scrape: {total_pages}")
        
        if total_pages == 0:
            logger.error("Could not determine total pages. Exiting.")
            return
            
        # Initialize list to store all car details
        all_cars = []
        
        # Set the maximum pages to scrape (adjust as needed)
        max_pages = min(total_pages, DEFAULT_MAX_PAGES) if DEFAULT_MAX_PAGES else total_pages
        
        # Iterate through pages
        for page in range(1, max_pages + 1):
            logger.info(f"Processing page {page}/{max_pages}")
            
            # Get listing IDs from the current page
            listing_ids = get_all_listing_ids(page)
            
            # Process each listing
            for listing_id in listing_ids:
                listing_url = f"{BASE_URL}/{listing_id}"
                car_details = scrape_specific_listing(listing_url)
                
                if car_details:
                    car_details['listing_id'] = listing_id
                    car_details['page_number'] = page
                    all_cars.append(car_details)
                    
                    # Show real-time progress
                    print(f"\rCars collected: {len(all_cars)}", end="")
                
                # Respect rate limiting
                sleep(DELAY_BETWEEN_REQUESTS)
            
            # Save progress after each page
            df = pd.DataFrame(all_cars)
            df.to_csv(os.path.join(OUTPUT_DIRECTORY, CSV_FILENAME), index=False)
            logger.info(f"Saved {len(all_cars)} cars to CSV")
        
        # Load the final dataset and display summary
        final_df = pd.read_csv(os.path.join(OUTPUT_DIRECTORY, CSV_FILENAME))
        display_data_summary(final_df)
        
        logger.info("Scraping completed successfully")
        
    except Exception as e:
        logger.critical(f"Unexpected error in main process: {str(e)}")
        raise

if __name__ == '__main__':
    # Create output directory if it doesn't exist
    os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)
    
    # Run the main scraping process
    main()