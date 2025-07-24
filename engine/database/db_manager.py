# database/db_manager.py
import os
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from datetime import datetime
from typing import Optional, Dict, List
import logging

class DatabaseManager:
    def __init__(self):
        self.client = None
        self.db = None
        self.logger = self._setup_logging()
        
    def _setup_logging(self):
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(__name__)
    
    def connect(self, connection_string: str, database_name: str = "review_scraper"):
        """Connect to MongoDB Atlas"""
        try:
            self.client = MongoClient(connection_string)
            self.db = self.client[database_name]
            
            # Test the connection
            self.client.admin.command('ping')
            self.logger.info(f"Successfully connected to MongoDB database: {database_name}")
            return True
            
        except ConnectionFailure as e:
            self.logger.error(f"Failed to connect to MongoDB: {e}")
            return False
    
    def get_establishment_by_url(self, google_url: str) -> Optional[Dict]:
        """Check if establishment already exists by Google URL"""
        return self.db.establishments.find_one({"google_url": google_url})
    
    def create_establishment(self, display_name: str, google_url: str, website: str) -> str:
        """Create new establishment record"""
        establishment = {
            "display_name": display_name,
            "google_url": google_url,
            "website": website,
            "trustpilot_url": f"{website}?languages=all",
            "google_last_scraped": None,
            "trustpilot_last_scraped": None,
            "google_total_reviews": 0,
            "trustpilot_total_reviews": 0,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }
        
        result = self.db.establishments.insert_one(establishment)
        self.logger.info(f"Created establishment: {display_name}")
        return str(result.inserted_id)
    
    def update_establishment_scrape_info(self, establishment_id: str, platform: str, total_reviews: int):
        """Update last scraped timestamp and review count"""
        update_data = {
            f"{platform}_last_scraped": datetime.utcnow(),
            f"{platform}_total_reviews": total_reviews,
            "updated_at": datetime.utcnow()
        }
        
        self.db.establishments.update_one(
            {"_id": establishment_id},
            {"$set": update_data}
        )
    
    def save_google_reviews(self, establishment_id: str, reviews: List[Dict]):
        """Save Google reviews to database"""
        if not reviews:
            return 0
            
        for review in reviews:
            review["establishment_id"] = establishment_id
            review["platform"] = "google"
            review["scraped_at"] = datetime.utcnow()
        
        result = self.db.google.insert_many(reviews)
        self.logger.info(f"Saved {len(reviews)} Google reviews for establishment {establishment_id}")
        return len(result.inserted_ids)
    
    def save_trustpilot_reviews(self, establishment_id: str, reviews: List[Dict]):
        """Save Trustpilot reviews to database"""
        if not reviews:
            return 0
            
        for review in reviews:
            review["establishment_id"] = establishment_id
            review["platform"] = "trustpilot"
            review["scraped_at"] = datetime.utcnow()
        
        result = self.db.trustpilot.insert_many(reviews)
        self.logger.info(f"Saved {len(reviews)} Trustpilot reviews for establishment {establishment_id}")
        return len(result.inserted_ids)
    
    def get_establishments_to_scrape(self) -> List[Dict]:
        """Get all establishments that need scraping"""
        return list(self.db.establishments.find({}))
    
    def close_connection(self):
        """Close database connection"""
        if self.client:
            self.client.close()
            self.logger.info("Database connection closed")