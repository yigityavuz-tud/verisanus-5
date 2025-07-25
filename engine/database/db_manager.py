# database/db_manager.py
import os
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from datetime import datetime
from typing import Optional, Dict, List
import logging
from bson import ObjectId

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
    
    def _standardize_google_review(self, review: Dict) -> Dict:
        """Standardize Google review to unified format"""
        return {
            "_id": review["_id"],  # Use original MongoDB ObjectId as unified review ID
            "original_review_id": review.get('review_id'),
            "establishment_id": review.get('establishment_id'),
            "platform": "google",
            "author_name": review.get('name'),
            "author_id": review.get('reviewerId'),
            "author_url": review.get('reviewerUrl'),
            "author_photo_url": review.get('reviewerPhotoUrl'),
            "author_review_count": review.get('reviewerNumberOfReviews'),
            "is_local_guide": review.get('isLocalGuide', False),
            "rating": review.get('rating') or review.get('stars'),
            "title": None,  # Google reviews don't have titles
            "review_text": review.get('text', ''),
            "review_text_translated": review.get('textTranslated'),
            "review_date": review.get('publishedAtDate', ''),
            "published_at": review.get('publishAt'),
            "published_at_date": review.get('publishedAtDate'),
            "verified_purchase": None,  # Google doesn't provide this info
            "helpful_votes": review.get('likesCount', 0),
            "response_from_owner_date": review.get('responseFromOwnerDate'),
            "response_from_owner_text": review.get('responseFromOwnerText'),
            "review_image_urls": review.get('reviewImageUrls', []),
            "review_context": review.get('reviewContext', {}),
            "review_detailed_rating": review.get('reviewDetailedRating', {}),
            "visited_in": review.get('visitedIn'),
            "is_advertisement": review.get('isAdvertisement', False),
            "original_language": review.get('originalLanguage'),
            "translated_language": review.get('translatedLanguage'),
            "review_language": review.get('language'),
            "country_code": review.get('countryCode'),
            "place_id": review.get('placeId'),
            "location": review.get('location', {}),
            "address": review.get('address'),
            "neighborhood": review.get('neighborhood'),
            "street": review.get('street'),
            "city": review.get('city'),
            "postal_code": review.get('postalCode'),
            "state": review.get('state'),
            "category_name": review.get('categoryName'),
            "categories": review.get('categories', []),
            "business_title": review.get('title'),
            "total_score": review.get('totalScore'),
            "permanently_closed": review.get('permanentlyClosed', False),
            "temporarily_closed": review.get('temporarilyClosed', False),
            "reviews_count": review.get('reviewsCount'),
            "business_url": review.get('url'),
            "price": review.get('price'),
            "cid": review.get('cid'),
            "fid": review.get('fid'),
            "image_url": review.get('imageUrl'),
            "source_url": review.get('source_url', ''),
            "scraped_at": review.get('scraped_at'),
            "scraper_scraped_at": review.get('scrapedAt'),
            "search_string": review.get('searchString'),
            "review_origin": review.get('reviewOrigin'),
            "review_url": review.get('reviewUrl'),
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }
    
    def _standardize_trustpilot_review(self, review: Dict) -> Dict:
        """Standardize Trustpilot review to unified format"""
        return {
            "_id": review["_id"],  # Use original MongoDB ObjectId as unified review ID
            "original_review_id": review.get('review_id'),
            "establishment_id": review.get('establishment_id'),
            "platform": "trustpilot",
            "author_name": None,  # Removed from Trustpilot data
            "author_id": None,  # Not available in this structure
            "author_url": None,
            "author_photo_url": None,
            "author_review_count": review.get('numberOfReviews'),
            "is_local_guide": None,  # Trustpilot doesn't have this concept
            "rating": review.get('ratingValue', 0),
            "title": review.get('reviewHeadline', ''),
            "review_text": review.get('reviewBody', ''),
            "review_text_translated": None,  # Not available in Trustpilot
            "review_date": review.get('datePublished', ''),
            "published_at": None,  # Google-specific field
            "published_at_date": review.get('datePublished'),
            "verified_purchase": review.get('verified', False),
            "verification_level": review.get('verificationLevel'),
            "helpful_votes": review.get('likes', 0),
            "response_from_owner_date": None,  # Not in this data structure
            "response_from_owner_text": None,  # Not in this data structure
            "review_image_urls": [],  # Not in this data structure
            "review_context": {},  # Google-specific
            "review_detailed_rating": {},  # Google-specific
            "visited_in": None,  # Google-specific
            "is_advertisement": False,  # Trustpilot doesn't mark ads this way
            "original_language": None,  # Not explicitly available
            "translated_language": None,  # Not available
            "review_language": review.get('reviewLanguage'),
            "country_code": review.get('consumerCountryCode'),
            "place_id": None,  # Google-specific
            "location": {},  # Google-specific
            "address": None,  # Google-specific
            "neighborhood": None,  # Google-specific
            "street": None,  # Google-specific
            "city": None,  # Google-specific
            "postal_code": None,  # Google-specific
            "state": None,  # Google-specific
            "category_name": None,  # Google-specific
            "categories": [],  # Google-specific
            "business_title": None,  # Google-specific
            "total_score": None,  # Google-specific
            "permanently_closed": None,  # Google-specific
            "temporarily_closed": None,  # Google-specific
            "reviews_count": None,  # Google-specific
            "business_url": None,  # Google-specific
            "price": None,  # Google-specific
            "cid": None,  # Google-specific
            "fid": None,  # Google-specific
            "image_url": None,  # Google-specific
            "experience_date": review.get('experienceDate'),
            "source_url": review.get('source_url', ''),
            "scraped_at": review.get('scraped_at'),
            "scraper_scraped_at": None,  # Google-specific
            "review_url": review.get('reviewUrl'),
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }
    
    def get_existing_unified_review_ids(self) -> set:
        """Get all existing unified review IDs to avoid duplicates"""
        existing_ids = self.db.unified_reviews.distinct("_id")
        return set(existing_ids)
    
    def unify_reviews_incremental(self, establishment_ids: List[str] = None) -> Dict[str, int]:
        """
        Incrementally unify reviews from Google and Trustpilot collections.
        Only processes reviews that haven't been unified yet.
        
        Args:
            establishment_ids: Optional list of establishment IDs to process. 
                              If None, processes all establishments.
        
        Returns:
            Dictionary with counts of unified reviews by platform
        """
        self.logger.info("Starting incremental review unification...")
        
        # Get existing unified review IDs to avoid duplicates
        existing_unified_ids = self.get_existing_unified_review_ids()
        self.logger.info(f"Found {len(existing_unified_ids)} existing unified reviews")
        
        # Build query filter
        query_filter = {}
        if establishment_ids:
            query_filter["establishment_id"] = {"$in": establishment_ids}
        
        unified_count = {"google": 0, "trustpilot": 0}
        reviews_to_insert = []
        
        # Process Google reviews
        self.logger.info("Processing Google reviews...")
        google_reviews = self.db.google.find(query_filter)
        
        for review in google_reviews:
            try:
                # Skip if already unified (using MongoDB _id)
                if review["_id"] in existing_unified_ids:
                    continue
                
                unified_review = self._standardize_google_review(review)
                reviews_to_insert.append(unified_review)
                unified_count["google"] += 1
                
                # Batch insert every 1000 reviews to manage memory
                if len(reviews_to_insert) >= 1000:
                    try:
                        self.db.unified_reviews.insert_many(reviews_to_insert, ordered=False)
                        self.logger.info(f"Inserted batch of {len(reviews_to_insert)} unified reviews")
                    except Exception as e:
                        self.logger.error(f"Error inserting batch: {str(e)[:200]}...")
                    reviews_to_insert.clear()
                    
            except Exception as e:
                self.logger.warning(f"Error processing Google review {review.get('_id', 'unknown')}: {e}")
                continue
        
        # Process Trustpilot reviews
        self.logger.info("Processing Trustpilot reviews...")
        trustpilot_reviews = self.db.trustpilot.find(query_filter)
        
        for review in trustpilot_reviews:
            try:
                # Skip if already unified (using MongoDB _id)
                if review["_id"] in existing_unified_ids:
                    continue
                
                unified_review = self._standardize_trustpilot_review(review)
                reviews_to_insert.append(unified_review)
                unified_count["trustpilot"] += 1
                
                # Batch insert every 1000 reviews to manage memory
                if len(reviews_to_insert) >= 1000:
                    try:
                        self.db.unified_reviews.insert_many(reviews_to_insert, ordered=False)
                        self.logger.info(f"Inserted batch of {len(reviews_to_insert)} unified reviews")
                    except Exception as e:
                        self.logger.error(f"Error inserting batch: {str(e)[:200]}...")
                    reviews_to_insert.clear()
                    
            except Exception as e:
                self.logger.warning(f"Error processing Trustpilot review {review.get('_id', 'unknown')}: {e}")
                continue
        
        # Insert remaining reviews
        if reviews_to_insert:
            try:
                self.db.unified_reviews.insert_many(reviews_to_insert, ordered=False)
                self.logger.info(f"Inserted final batch of {len(reviews_to_insert)} unified reviews")
            except Exception as e:
                self.logger.error(f"Error inserting final batch: {str(e)[:200]}...")
        
        total_unified = unified_count["google"] + unified_count["trustpilot"]
        self.logger.info(f"Unification complete! Unified {total_unified} new reviews: "
                        f"Google={unified_count['google']}, Trustpilot={unified_count['trustpilot']}")
        
        return unified_count
    
    def create_unified_reviews_indexes(self):
        """Create indexes on unified_reviews collection for better performance"""
        try:
            # Create indexes (_id is automatically indexed by MongoDB)
            self.db.unified_reviews.create_index("establishment_id")
            self.db.unified_reviews.create_index("platform")
            self.db.unified_reviews.create_index("review_date")
            self.db.unified_reviews.create_index([("establishment_id", 1), ("platform", 1)])
            
            self.logger.info("Created indexes on unified_reviews collection")
        except Exception as e:
            self.logger.error(f"Error creating indexes: {e}")
    
    def get_unified_reviews_stats(self) -> Dict:
        """Get statistics about unified reviews"""
        try:
            pipeline = [
                {
                    "$group": {
                        "_id": "$platform",
                        "count": {"$sum": 1},
                        "avg_rating": {"$avg": "$rating"}
                    }
                }
            ]
            
            platform_stats = list(self.db.unified_reviews.aggregate(pipeline))
            
            total_reviews = self.db.unified_reviews.count_documents({})
            
            return {
                "total_reviews": total_reviews,
                "platform_breakdown": platform_stats,
                "last_updated": datetime.utcnow()
            }
        except Exception as e:
            self.logger.error(f"Error getting unified reviews stats: {e}")
            return {}
    
    def get_unified_reviews_by_establishment(self, establishment_id: str, 
                                           platform: str = None, 
                                           limit: int = None) -> List[Dict]:
        """
        Get unified reviews for a specific establishment
        
        Args:
            establishment_id: The establishment ID
            platform: Optional platform filter ('google' or 'trustpilot')
            limit: Optional limit on number of reviews returned
        """
        query = {"establishment_id": establishment_id}
        if platform:
            query["platform"] = platform
        
        cursor = self.db.unified_reviews.find(query).sort("review_date", -1)
        if limit:
            cursor = cursor.limit(limit)
        
        return list(cursor)
    
    def close_connection(self):
        """Close database connection"""
        if self.client:
            self.client.close()
            self.logger.info("Database connection closed")