"""
Configuration settings for the web scraper.
"""

# Base URLs
BASE_URL = "https://turbo.az/autos"
LISTING_URL = f"{BASE_URL}"  # For individual listing pages

# Request Settings
REQUEST_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}

REQUEST_TIMEOUT = 10  # seconds

# Scraping Settings
DELAY_BETWEEN_REQUESTS = 1  # seconds
MAX_RETRIES = 3
DEFAULT_MAX_PAGES = None  # Set to None to scrape all pages, or a number to limit pages

# HTML Selectors
SELECTORS = {
    'car_container': 'div.products-i',
    'properties_column': 'div.product-properties__column',
    'property_item': 'div.product-properties__i',
    'property_name': 'label.product-properties__i-name',
    'property_value': 'span.product-properties__i-value',
    'product_link': 'a.products-i__link'
}

# Label Mappings (Azerbaijani to English)
LABEL_MAPPING = {
    'Şəhər': 'city',
    'Marka': 'brand',
    'Model': 'model',
    'Buraxılış ili': 'year',
    'Ban növü': 'body_type',
    'Rəng': 'color',
    'Mühərrik': 'engine',
    'Yürüş': 'mileage'
}

# Logging Configuration
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
            'level': 'INFO',
        },
        'file': {
            'class': 'logging.FileHandler',
            'filename': 'scraper.log',
            'formatter': 'standard',
            'level': 'INFO',
        }
    },
    'loggers': {
        '': {  # Root logger
            'handlers': ['console', 'file'],
            'level': 'INFO',
        }
    }
}

# Output Settings
OUTPUT_DIRECTORY = "data"
CSV_FILENAME = "turbo_az_listings.csv"
