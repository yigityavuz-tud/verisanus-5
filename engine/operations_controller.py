# engine/operations_controller.py
import os
import sys
import argparse
import logging
from pathlib import Path
from typing import List, Optional

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from database.db_manager import DatabaseManager
from utils.excel_reader import ExcelReader
from scrapers.apify_client import ApifyClient

class OperationsController:
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.db_manager = DatabaseManager()
        self.excel_reader = ExcelReader()
        self.apify_client = None
        self.logger = self._setup_logging()
    
    def _setup_logging(self):
        """Setup logging configuration"""
        level = logging.DEBUG if self.verbose else logging.INFO
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('operations.log'),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger(__name__)
    
    def _load_tokens(self):
        """Load API tokens from files"""
        try:
            # Load MongoDB connection string
            with open('tokens/mongodb_connection.txt', 'r') as f:
                mongodb_connection = f.read().strip()
            
            # Load Apify API token
            with open('tokens/apify_token_dev2.txt', 'r') as f:
                apify_token = f.read().strip()
            
            return mongodb_connection, apify_token
            
        except FileNotFoundError as e:
            self.logger.error(f"Token file not found: {e}")
            return None, None
    
    def initialize(self):
        """Initialize database connection and API clients"""
        self.logger.info("Initializing operations controller...")
        
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
    
    def scrape_reviews(self, excel_path: str) -> bool:
        """Scrape reviews from establishments in Excel file"""
        self.logger.info(f"Starting review scraping from: {excel_path}")
        
        if not os.path.exists(excel_path):
            self.logger.error(f"Excel file not found: {excel_path}")
            return False
        
        try:
            # Load establishments from Excel
            establishments = self.excel_reader.read_establishments(excel_path)
            if not establishments:
                self.logger.error("No establishments found in Excel file")
                return False
            
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
            
            self.logger.info(f"Starting scrape for {len(processed_establishments)} establishments")
            
            # Scrape reviews for each establishment
            results = []
            for i, establishment in enumerate(processed_establishments, 1):
                self.logger.info(f"Processing {i}/{len(processed_establishments)}: {establishment['display_name']}")
                
                try:
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
                    
                    results.append({
                        'establishment_id': establishment['id'],
                        'google_reviews': google_count,
                        'trustpilot_reviews': trustpilot_count
                    })
                    
                    self.logger.info(f"Completed scraping for {establishment['display_name']}: "
                                   f"Google={google_count}, Trustpilot={trustpilot_count}")
                    
                except Exception as e:
                    self.logger.error(f"Error scraping {establishment['display_name']}: {e}")
                    continue
            
            # Summary
            total_google = sum(r['google_reviews'] for r in results)
            total_trustpilot = sum(r['trustpilot_reviews'] for r in results)
            
            self.logger.info(f"Scraping complete! Total reviews: Google={total_google}, Trustpilot={total_trustpilot}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error during scraping process: {e}")
            return False
    
    def unify_reviews(self, establishment_ids: Optional[List[str]] = None, quick: bool = False) -> bool:
        """Unify reviews from Google and Trustpilot collections"""
        self.logger.info("Starting review unification...")
        
        try:
            # Create indexes if they don't exist
            self.db_manager.create_unified_reviews_indexes()
            
            # Run incremental unification
            unified_count = self.db_manager.unify_reviews_incremental(establishment_ids)
            
            if not quick:
                # Show statistics
                stats = self.db_manager.get_unified_reviews_stats()
                self.logger.info(f"Unification statistics: {stats}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error during unification: {e}")
            return False
    
    def standardize_reviews(self, establishment_ids: Optional[List[str]] = None, quick: bool = False) -> bool:
        """Standardize reviews with language translation"""
        self.logger.info("Starting review language standardization...")
        
        try:
            # Create indexes if they don't exist
            self.db_manager.create_ls_unified_reviews_indexes()
            
            # Run incremental standardization
            standardization_results = self.db_manager.standardize_reviews_incremental(establishment_ids)
            
            if not quick:
                # Show statistics
                stats = self.db_manager.get_ls_unified_reviews_stats()
                self.logger.info(f"Language standardization statistics: {stats}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error during language standardization: {e}")
            return False
    
    def show_statistics(self) -> bool:
        """Show database statistics"""
        try:
            self.logger.info("Fetching database statistics...")
            
            # Unified reviews stats
            unified_stats = self.db_manager.get_unified_reviews_stats()
            
            # Language standardized reviews stats
            ls_stats = self.db_manager.get_ls_unified_reviews_stats()
            
            # Raw collection stats
            google_count = self.db_manager.db.google.count_documents({})
            trustpilot_count = self.db_manager.db.trustpilot.count_documents({})
            establishments_count = self.db_manager.db.establishments.count_documents({})
            
            print("\n" + "="*60)
            print("DATABASE STATISTICS")
            print("="*60)
            print(f"Establishments: {establishments_count}")
            print(f"Google reviews (raw): {google_count}")
            print(f"Trustpilot reviews (raw): {trustpilot_count}")
            print(f"Total raw reviews: {google_count + trustpilot_count}")
            
            print("\nUnified Reviews:")
            print(f"Total unified: {unified_stats.get('total_reviews', 0)}")
            
            for platform_stat in unified_stats.get('platform_breakdown', []):
                platform = platform_stat['_id']
                count = platform_stat['count']
                avg_rating = platform_stat.get('avg_rating', 0)
                print(f"  {platform.capitalize()}: {count} reviews (avg rating: {avg_rating:.2f})")
            
            print("\nLanguage Standardized Reviews:")
            print(f"Total standardized: {ls_stats.get('total_reviews', 0)}")
            
            for platform_stat in ls_stats.get('platform_breakdown', []):
                platform = platform_stat['_id']
                count = platform_stat['count']
                avg_rating = platform_stat.get('avg_rating', 0)
                owner_responses = platform_stat.get('has_owner_response', 0)
                print(f"  {platform.capitalize()}: {count} reviews (avg rating: {avg_rating:.2f}, owner responses: {owner_responses})")
            
            if ls_stats.get('response_language_breakdown'):
                print("\nOwner Response Languages:")
                for lang_stat in ls_stats.get('response_language_breakdown', []):
                    language = lang_stat['_id']
                    count = lang_stat['count']
                    print(f"  {language}: {count} responses")
            
            print("="*60)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error fetching statistics: {e}")
            return False
    
    def scrape_and_unify(self, excel_path: str, quick_unify: bool = False) -> bool:
        """Combined operation: scrape reviews then unify them"""
        self.logger.info("Starting scrape and unify operation...")
        
        # First scrape
        if not self.scrape_reviews(excel_path):
            self.logger.error("Scraping failed, aborting unification")
            return False
        
        # Then unify
        if not self.unify_reviews(quick=quick_unify):
            self.logger.error("Unification failed")
            return False
        
        self.logger.info("Scrape and unify operation completed successfully")
        return True
    
    def scrape_unify_and_standardize(self, excel_path: str, quick_unify: bool = False, quick_standardize: bool = False) -> bool:
        """Combined operation: scrape reviews, unify them, then standardize them"""
        self.logger.info("Starting scrape, unify, and standardize operation...")
        
        # First scrape
        if not self.scrape_reviews(excel_path):
            self.logger.error("Scraping failed, aborting remaining operations")
            return False
        
        # Then unify
        if not self.unify_reviews(quick=quick_unify):
            self.logger.error("Unification failed, aborting standardization")
            return False
        
        # Finally standardize
        if not self.standardize_reviews(quick=quick_standardize):
            self.logger.error("Standardization failed")
            return False
        
        self.logger.info("Scrape, unify, and standardize operation completed successfully")
        return True
    
    def cleanup(self):
        """Clean up resources"""
        if self.db_manager:
            self.db_manager.close_connection()

def main():
    parser = argparse.ArgumentParser(description='Review Scraper Operations Controller')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose logging')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Scrape command
    scrape_parser = subparsers.add_parser('scrape', help='Scrape reviews from establishments')
    scrape_parser.add_argument('--excel', required=True, help='Path to Excel file with establishments')
    
    # Unify command
    unify_parser = subparsers.add_parser('unify', help='Unify reviews from raw collections')
    unify_parser.add_argument('--establishments', help='Comma-separated establishment IDs to process')
    unify_parser.add_argument('--quick', action='store_true', help='Quick mode with minimal output')
    
    # Standardize command
    standardize_parser = subparsers.add_parser('standardize', help='Standardize reviews with language translation')
    standardize_parser.add_argument('--establishments', help='Comma-separated establishment IDs to process')
    standardize_parser.add_argument('--quick', action='store_true', help='Quick mode with minimal output')
    
    # Stats command
    subparsers.add_parser('stats', help='Show database statistics')
    
    # Combined commands
    combined_parser = subparsers.add_parser('scrape-and-unify', help='Scrape reviews then unify them')
    combined_parser.add_argument('--excel', required=True, help='Path to Excel file with establishments')
    combined_parser.add_argument('--quick-unify', action='store_true', help='Use quick mode for unification')
    
    full_pipeline_parser = subparsers.add_parser('full-pipeline', help='Scrape, unify, and standardize reviews')
    full_pipeline_parser.add_argument('--excel', required=True, help='Path to Excel file with establishments')
    full_pipeline_parser.add_argument('--quick-unify', action='store_true', help='Use quick mode for unification')
    full_pipeline_parser.add_argument('--quick-standardize', action='store_true', help='Use quick mode for standardization')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Initialize controller
    controller = OperationsController(verbose=args.verbose)
    
    try:
        if not controller.initialize():
            print("Failed to initialize operations controller")
            return
        
        success = False
        
        if args.command == 'scrape':
            success = controller.scrape_reviews(args.excel)
        
        elif args.command == 'unify':
            establishment_ids = None
            if args.establishments:
                establishment_ids = [id.strip() for id in args.establishments.split(',')]
            success = controller.unify_reviews(establishment_ids, args.quick)
        
        elif args.command == 'standardize':
            establishment_ids = None
            if args.establishments:
                establishment_ids = [id.strip() for id in args.establishments.split(',')]
            success = controller.standardize_reviews(establishment_ids, args.quick)
        
        elif args.command == 'stats':
            success = controller.show_statistics()
        
        elif args.command == 'scrape-and-unify':
            success = controller.scrape_and_unify(args.excel, args.quick_unify)
        
        elif args.command == 'full-pipeline':
            success = controller.scrape_unify_and_standardize(
                args.excel, 
                args.quick_unify, 
                args.quick_standardize
            )
        
        if success:
            print(f"\n✅ {args.command.upper()} operation completed successfully!")
        else:
            print(f"\n❌ {args.command.upper()} operation failed!")
            
    except KeyboardInterrupt:
        print("\n\n⚠️ Operation interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
    finally:
        controller.cleanup()

if __name__ == "__main__":
    main()