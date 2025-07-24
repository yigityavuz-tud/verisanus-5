# engine/main_scraper.py
import os
import sys
import logging
import yaml
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from database.db_manager import DatabaseManager
from utils.excel_reader import ExcelReader
from scrapers.apify_client import ApifyClient

class ReviewScraper:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.excel_reader = ExcelReader()
        self.apify_client = None
        self.config = None
        self.logger = self._setup_logging()
    
    def _setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('scraper.log'),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger(__name__)
    
    def _load_config(self):
        """Load configuration from YAML file"""
        try:
            with open('config.yaml', 'r') as f:
                self.config = yaml.safe_load(f)
            self.logger.info("Configuration loaded successfully")
            return True
        except FileNotFoundError:
            self.logger.error("Config file 'config.yaml' not found")
            return False
        except yaml.YAMLError as e:
            self.logger.error(f"Error parsing config file: {e}")
            return False
    
    def _load_tokens(self):
        """Load API tokens from files specified in config"""
        try:
            # Ensure config is loaded
            if not self.config:
                if not self._load_config():
                    return None, None
            
            # Load MongoDB connection string
            with open('tokens/mongodb_connection.txt', 'r') as f:
                mongodb_connection = f.read().strip()
            
            # Load Apify API token from config-specified file
            apify_token_file = self.config['google_maps']['api_settings']['apify_token_file']
            with open(apify_token_file, 'r') as f:
                apify_token = f.read().strip()
            
            self.logger.info(f"Loaded Apify token from: {apify_token_file}")
            
            return mongodb_connection, apify_token
            
        except FileNotFoundError as e:
            self.logger.error(f"Token file not found: {e}")
            return None, None
        except KeyError as e:
            self.logger.error(f"Config key not found: {e}")
            return None, None
    
    def initialize(self):
        """Initialize all components"""
        self.logger.info("Initializing Review Scraper...")
        
        # Load configuration
        if not self._load_config():
            self.logger.error("Failed to load configuration")
            return False
        
        # Load tokens
        mongodb_connection, apify_token = self._load_tokens()
        if not mongodb_connection or not apify_token:
            self.logger.error("Failed to load required tokens")
            return False
        
        # Connect to database
        if not self.db_manager.connect(mongodb_connection):
            return False
        
        # Initialize Apify client
        self.apify_client = ApifyClient(apify_token)
        
        self.logger.info("Initialization complete")
        return True
    
    def load_establishments_from_excel(self, excel_path: str):
        """Load establishments from Excel file"""
        self.logger.info(f"Loading establishments from: {excel_path}")
        
        establishments = self.excel_reader.read_establishments(excel_path)
        if not establishments:
            self.logger.error("No establishments found in Excel file")
            return []
        
        # Process each establishment
        processed_establishments = []
        for est in establishments:
            # Check if establishment already exists
            existing = self.db_manager.get_establishment_by_url(est['google_url'])
            
            if existing:
                self.logger.info(f"Establishment already exists: {est['display_name']}")
                processed_establishments.append({
                    'id': str(existing['_id']),
                    'display_name': existing['display_name'],
                    'google_url': existing['google_url'],
                    'website': existing['website'],
                    'existing': True
                })
            else:
                # Create new establishment
                establishment_id = self.db_manager.create_establishment(
                    est['display_name'],
                    est['google_url'],
                    est['website']
                )
                processed_establishments.append({
                    'id': establishment_id,
                    'display_name': est['display_name'],
                    'google_url': est['google_url'],
                    'website': est['website'],
                    'existing': False
                })
        
        return processed_establishments
    
    def scrape_establishment_reviews(self, establishment: dict):
        """Scrape reviews for a single establishment"""
        self.logger.info(f"Scraping reviews for: {establishment['display_name']}")
        
        # Scrape Google reviews
        self.logger.info("Scraping Google Maps reviews...")
        google_reviews = self.apify_client.scrape_google_reviews(establishment['google_url'])
        google_count = self.db_manager.save_google_reviews(establishment['id'], google_reviews)
        
        # Update establishment with Google scrape info
        self.db_manager.update_establishment_scrape_info(
            establishment['id'], 
            'google', 
            google_count
        )
        
        # Scrape Trustpilot reviews
        self.logger.info("Scraping Trustpilot reviews...")
        trustpilot_reviews = self.apify_client.scrape_trustpilot_reviews(establishment['website'])
        trustpilot_count = self.db_manager.save_trustpilot_reviews(establishment['id'], trustpilot_reviews)
        
        # Update establishment with Trustpilot scrape info
        self.db_manager.update_establishment_scrape_info(
            establishment['id'], 
            'trustpilot', 
            trustpilot_count
        )
        
        self.logger.info(f"Completed scraping for {establishment['display_name']}: "
                        f"Google={google_count}, Trustpilot={trustpilot_count}")
        
        return {
            'establishment_id': establishment['id'],
            'google_reviews': google_count,
            'trustpilot_reviews': trustpilot_count
        }
    
    def run_full_scrape(self, excel_path: str):
        """Run complete scraping process"""
        if not self.initialize():
            self.logger.error("Failed to initialize scraper")
            return
        
        try:
            # Load establishments from Excel
            establishments = self.load_establishments_from_excel(excel_path)
            if not establishments:
                return
            
            self.logger.info(f"Starting scrape for {len(establishments)} establishments")
            
            results = []
            for i, establishment in enumerate(establishments, 1):
                self.logger.info(f"Processing {i}/{len(establishments)}: {establishment['display_name']}")
                
                try:
                    result = self.scrape_establishment_reviews(establishment)
                    results.append(result)
                    
                except Exception as e:
                    self.logger.error(f"Error scraping {establishment['display_name']}: {e}")
                    continue
            
            # Summary
            total_google = sum(r['google_reviews'] for r in results)
            total_trustpilot = sum(r['trustpilot_reviews'] for r in results)
            
            self.logger.info(f"Scraping complete! Total reviews: Google={total_google}, Trustpilot={total_trustpilot}")
            
        except Exception as e:
            self.logger.error(f"Error during scraping process: {e}")
        
        finally:
            self.db_manager.close_connection()

def main():
    """Main entry point"""
    scraper = ReviewScraper()
    
    # Excel file path (relative to engine folder)
    excel_path = "../database/establishments.xlsx"
    
    # Check if Excel file exists
    if not os.path.exists(excel_path):
        print(f"Excel file not found: {excel_path}")
        return
    
    # Run the scraper
    scraper.run_full_scrape(excel_path)

if __name__ == "__main__":
    main()