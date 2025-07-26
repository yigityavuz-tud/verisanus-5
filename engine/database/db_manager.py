# database/db_manager.py
import os
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from datetime import datetime
from typing import Optional, Dict, List
import logging
from bson import ObjectId
import langdetect
import hashlib

class DatabaseManager:
    def __init__(self):
        self.client = None
        self.db = None
        self.logger = self._setup_logging()
        self.translation_cache = {}  # In-memory cache for duplicate texts
        
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
    
    # NEW LANGUAGE STANDARDIZATION METHODS
    
    def _detect_language(self, text: str) -> Optional[str]:
        """Detect language of text using langdetect"""
        if not text or len(text.strip()) < 5:
            return None
        
        try:
            detected = langdetect.detect(text)
            return detected if detected else None
        except:
            return None  # Failed detection = assume needs translation
    
    def _get_text_hash(self, text: str) -> str:
        """Generate hash for text caching"""
        return hashlib.md5(text.encode('utf-8')).hexdigest()
    
    def _translate_text(self, text: str, source_lang: str = None) -> str:
        """
        Translate text to English using Google Gemini API
        """
        if not text or not text.strip():
            return text
        
        # Check cache first
        text_hash = self._get_text_hash(text)
        if text_hash in self.translation_cache:
            return self.translation_cache[text_hash]
        
        # Initialize translation counter if not exists
        if not hasattr(self, 'translation_counter'):
            self.translation_counter = 0
            self.translation_total = 0
        
        # Increment counter for actual translation
        self.translation_counter += 1
        
        # Prompt user every 100 translations
        if self.translation_counter % 100 == 0:
            self.logger.info(f"\n{'='*50}")
            self.logger.info(f"TRANSLATION PROGRESS: {self.translation_counter} texts translated so far")
            if self.translation_total > 0:
                self.logger.info(f"Estimated remaining: {self.translation_total - self.translation_counter}")
            self.logger.info(f"{'='*50}")
        
        try:
            import google.generativeai as genai
            
            # Load API key
            try:
                with open('tokens/google_api_key.txt', 'r') as f:
                    api_key = f.read().strip()
                genai.configure(api_key=api_key)
            except FileNotFoundError:
                self.logger.error("Google API key file not found: tokens/google_api_key.txt")
                return text
            
            # Initialize model
            model = genai.GenerativeModel('gemini-2.5-flash')
            
            # Create prompt
            prompt = f"Translate the following text to English. Return only the translated text with no additional content:\n\n{text}"
            
            # Generate translation
            response = model.generate_content(prompt)
            translated = response.text.strip()
            
            # Cache the result
            self.translation_cache[text_hash] = translated
            return translated
            
        except Exception as e:
            self.logger.error(f"Translation failed for text: {str(e)}")
            return text  # Return original text on failure
    
    def _standardize_google_review_ls(self, review: Dict) -> Dict:
        """Standardize Google review for language standardization"""
        # Start with the unified review
        ls_review = review.copy()
        
        # Detect language of owner response
        response_text = review.get('response_from_owner_text')
        response_language = self._detect_language(response_text)
        ls_review['response_from_owner_language'] = response_language
        
        # Translate owner response if not English
        if response_text and response_language and response_language != 'en':
            translated_response = self._translate_text(response_text, response_language)
            ls_review['response_from_owner_text'] = translated_response
        
        # Update timestamps
        ls_review['updated_at'] = datetime.utcnow()
        
        return ls_review
    
    def _standardize_trustpilot_review_ls(self, review: Dict) -> Dict:
        """Standardize Trustpilot review for language standardization"""
        # Start with the unified review
        ls_review = review.copy()
        
        # Detect language of owner response
        response_text = review.get('response_from_owner_text')
        response_language = self._detect_language(response_text)
        ls_review['response_from_owner_language'] = response_language
        
        # Translate review content if not English
        review_language = review.get('review_language')
        if review_language and review_language != 'en':
            title = review.get('title', '')
            review_text = review.get('review_text', '')
            
            # Concatenate title and review text for translation
            combined_text = f"{title}\n{review_text}".strip()
            if combined_text:
                translated_content = self._translate_text(combined_text, review_language)
                
                # Split back into title and review text
                # Simple approach: if original title exists, assume first line is title
                if title and '\n' in translated_content:
                    lines = translated_content.split('\n', 1)
                    ls_review['title'] = lines[0]
                    ls_review['review_text'] = lines[1] if len(lines) > 1 else ''
                else:
                    ls_review['review_text'] = translated_content
        
        # Translate owner response if not English
        if response_text and response_language and response_language != 'en':
            translated_response = self._translate_text(response_text, response_language)
            ls_review['response_from_owner_text'] = translated_response
        
        # Update timestamps
        ls_review['updated_at'] = datetime.utcnow()
        
        return ls_review
    
    def get_existing_ls_unified_review_ids(self) -> set:
        """Get all existing language standardized review IDs to avoid duplicates"""
        try:
            existing_ids = self.db.ls_unified_reviews.distinct("_id")
            return set(existing_ids)
        except:
            # Collection doesn't exist yet
            return set()
    
    def create_ls_unified_reviews_indexes(self):
        """Create indexes on ls_unified_reviews collection for better performance"""
        try:
            # Create indexes (_id is automatically indexed by MongoDB)
            self.db.ls_unified_reviews.create_index("establishment_id")
            self.db.ls_unified_reviews.create_index("platform")
            self.db.ls_unified_reviews.create_index("review_date")
            self.db.ls_unified_reviews.create_index("response_from_owner_language")
            self.db.ls_unified_reviews.create_index([("establishment_id", 1), ("platform", 1)])
            
            self.logger.info("Created indexes on ls_unified_reviews collection")
        except Exception as e:
            self.logger.error(f"Error creating indexes: {e}")
    
    def _count_translations_needed(self, establishment_ids: List[str] = None) -> Dict[str, int]:
        """Count how many translations will be needed before starting standardization"""
        self.logger.info("Counting translations needed...")
        
        # Get existing standardized review IDs to avoid counting duplicates
        existing_ls_ids = self.get_existing_ls_unified_review_ids()
        
        # Build query filter
        query_filter = {}
        if establishment_ids:
            query_filter["establishment_id"] = {"$in": establishment_ids}
        
        translation_count = {"google_responses": 0, "trustpilot_content": 0, "trustpilot_responses": 0}
        
        # Process in batches to avoid cursor timeout
        total_reviews = self.db.unified_reviews.count_documents(query_filter)
        processed_count = 0
        batch_size = 1000
        
        while processed_count < total_reviews:
            # Get batch of reviews
            reviews_batch = list(
                self.db.unified_reviews
                .find(query_filter)
                .skip(processed_count)
                .limit(batch_size)
            )
            
            if not reviews_batch:
                break
            
            for review in reviews_batch:
                if review["_id"] in existing_ls_ids:
                    continue
                    
                platform = review.get('platform')
                
                if platform == 'google':
                    response_text = review.get('response_from_owner_text')
                    if response_text:
                        response_language = self._detect_language(response_text)
                        if response_language and response_language != 'en':
                            translation_count["google_responses"] += 1
                
                elif platform == 'trustpilot':
                    # Check review content
                    review_language = review.get('review_language')
                    if review_language and review_language != 'en':
                        title = review.get('title', '')
                        review_text = review.get('review_text', '')
                        combined_text = f"{title}\n{review_text}".strip()
                        if combined_text:
                            translation_count["trustpilot_content"] += 1
                    
                    # Check owner response
                    response_text = review.get('response_from_owner_text')
                    if response_text:
                        response_language = self._detect_language(response_text)
                        if response_language and response_language != 'en':
                            translation_count["trustpilot_responses"] += 1
            
            processed_count += len(reviews_batch)
            self.logger.info(f"Counted translations for {processed_count}/{total_reviews} reviews")
        
        total_translations = sum(translation_count.values())
        self.logger.info(f"Translations needed: {total_translations} total "
                        f"(Google responses: {translation_count['google_responses']}, "
                        f"Trustpilot content: {translation_count['trustpilot_content']}, "
                        f"Trustpilot responses: {translation_count['trustpilot_responses']})")
        
        return translation_count
    def standardize_reviews_incremental(self, establishment_ids: List[str] = None) -> Dict[str, int]:
        """
        Incrementally standardize reviews from unified_reviews collection.
        Only processes reviews that haven't been standardized yet.
        
        Args:
            establishment_ids: Optional list of establishment IDs to process. 
                              If None, processes all establishments.
        
        Returns:
            Dictionary with counts of standardized reviews by platform
        """
        self.logger.info("Starting incremental review language standardization...")
        
        # Count translations needed first
        translation_estimates = self._count_translations_needed(establishment_ids)
        total_translations_needed = sum(translation_estimates.values())
        
        if total_translations_needed > 0:
            self.logger.info(f"\n{'='*60}")
            self.logger.info(f"TRANSLATION ESTIMATE: {total_translations_needed} texts need translation")
            self.logger.info(f"Google owner responses: {translation_estimates['google_responses']}")
            self.logger.info(f"Trustpilot review content: {translation_estimates['trustpilot_content']}")
            self.logger.info(f"Trustpilot owner responses: {translation_estimates['trustpilot_responses']}")
            self.logger.info(f"{'='*60}")
        
        # Initialize translation counter
        self.translation_counter = 0
        self.translation_total = total_translations_needed
        
        # Get existing standardized review IDs to avoid duplicates
        existing_ls_ids = self.get_existing_ls_unified_review_ids()
        self.logger.info(f"Found {len(existing_ls_ids)} existing language standardized reviews")
        
        # Build query filter
        query_filter = {}
        if establishment_ids:
            query_filter["establishment_id"] = {"$in": establishment_ids}
        
        standardized_count = {"google": 0, "trustpilot": 0}
        reviews_to_insert = []
        translation_count = {"google_responses": 0, "trustpilot_content": 0, "trustpilot_responses": 0}
        
    def standardize_reviews_incremental(self, establishment_ids: List[str] = None) -> Dict[str, int]:
        """
        Incrementally standardize reviews from unified_reviews collection.
        Only processes reviews that haven't been standardized yet.
        
        Args:
            establishment_ids: Optional list of establishment IDs to process. 
                              If None, processes all establishments.
        
        Returns:
            Dictionary with counts of standardized reviews by platform
        """
        self.logger.info("Starting incremental review language standardization...")
        
        # Count translations needed first
        translation_estimates = self._count_translations_needed(establishment_ids)
        total_translations_needed = sum(translation_estimates.values())
        
        if total_translations_needed > 0:
            self.logger.info(f"\n{'='*60}")
            self.logger.info(f"TRANSLATION ESTIMATE: {total_translations_needed} texts need translation")
            self.logger.info(f"Google owner responses: {translation_estimates['google_responses']}")
            self.logger.info(f"Trustpilot review content: {translation_estimates['trustpilot_content']}")
            self.logger.info(f"Trustpilot owner responses: {translation_estimates['trustpilot_responses']}")
            self.logger.info(f"{'='*60}")
        
        # Initialize translation counter
        self.translation_counter = 0
        self.translation_total = total_translations_needed
        
        # Get existing standardized review IDs to avoid duplicates
        existing_ls_ids = self.get_existing_ls_unified_review_ids()
        self.logger.info(f"Found {len(existing_ls_ids)} existing language standardized reviews")
        
        # Build query filter
        query_filter = {}
        if establishment_ids:
            query_filter["establishment_id"] = {"$in": establishment_ids}
        
        standardized_count = {"google": 0, "trustpilot": 0}
        reviews_to_insert = []
        translation_count = {"google_responses": 0, "trustpilot_content": 0, "trustpilot_responses": 0}
        
        # Process reviews in batches to avoid cursor timeout
        self.logger.info("Processing unified reviews for language standardization...")
        
        # Get total count for progress tracking
        total_reviews_to_process = self.db.unified_reviews.count_documents(query_filter)
        processed_count = 0
        batch_size = 500  # Smaller batch size to avoid cursor timeout
        
        try:
            while processed_count < total_reviews_to_process:
                # Get a batch of reviews with skip and limit
                unified_reviews_batch = list(
                    self.db.unified_reviews
                    .find(query_filter)
                    .skip(processed_count)
                    .limit(batch_size)
                )
                
                if not unified_reviews_batch:
                    break
                
                self.logger.info(f"Processing batch {processed_count//batch_size + 1}: "
                               f"reviews {processed_count + 1}-{processed_count + len(unified_reviews_batch)} "
                               f"of {total_reviews_to_process}")
                
                for review in unified_reviews_batch:
                    try:
                        # Skip if already standardized (using MongoDB _id)
                        if review["_id"] in existing_ls_ids:
                            continue
                        
                        platform = review.get('platform')
                        
                        if platform == 'google':
                            ls_review = self._standardize_google_review_ls(review)
                            standardized_count["google"] += 1
                            
                            # Count translations
                            if (review.get('response_from_owner_text') and 
                                ls_review.get('response_from_owner_language') and 
                                ls_review.get('response_from_owner_language') != 'en'):
                                translation_count["google_responses"] += 1
                                
                        elif platform == 'trustpilot':
                            ls_review = self._standardize_trustpilot_review_ls(review)
                            standardized_count["trustpilot"] += 1
                            
                            # Count translations
                            if (review.get('review_language') and 
                                review.get('review_language') != 'en'):
                                translation_count["trustpilot_content"] += 1
                            
                            if (review.get('response_from_owner_text') and 
                                ls_review.get('response_from_owner_language') and 
                                ls_review.get('response_from_owner_language') != 'en'):
                                translation_count["trustpilot_responses"] += 1
                        
                        else:
                            self.logger.warning(f"Unknown platform: {platform}")
                            continue
                        
                        reviews_to_insert.append(ls_review)
                        
                        # Batch insert every 1000 reviews to manage memory
                        if len(reviews_to_insert) >= 1000:
                            try:
                                self.db.ls_unified_reviews.insert_many(reviews_to_insert, ordered=False)
                                self.logger.info(f"Inserted batch of {len(reviews_to_insert)} standardized reviews")
                            except Exception as e:
                                self.logger.error(f"Error inserting batch: {str(e)[:200]}...")
                            reviews_to_insert.clear()
                            
                    except Exception as e:
                        self.logger.warning(f"Error processing review {review.get('_id', 'unknown')}: {e}")
                        continue
                
                processed_count += len(unified_reviews_batch)
                
                # Insert any remaining reviews from this batch
                if reviews_to_insert:
                    try:
                        self.db.ls_unified_reviews.insert_many(reviews_to_insert, ordered=False)
                        self.logger.info(f"Inserted batch of {len(reviews_to_insert)} standardized reviews")
                    except Exception as e:
                        self.logger.error(f"Error inserting batch: {str(e)[:200]}...")
                    reviews_to_insert.clear()
        
        except KeyboardInterrupt:
            self.logger.info("\nTranslation process interrupted by user")
            # Save any pending reviews before exiting
            if reviews_to_insert:
                try:
                    self.db.ls_unified_reviews.insert_many(reviews_to_insert, ordered=False)
                    self.logger.info(f"Saved {len(reviews_to_insert)} reviews before exit")
                except:
                    pass
            raise
        
        # Insert any final remaining reviews
        if reviews_to_insert:
            try:
                self.db.ls_unified_reviews.insert_many(reviews_to_insert, ordered=False)
                self.logger.info(f"Inserted final batch of {len(reviews_to_insert)} standardized reviews")
            except Exception as e:
                self.logger.error(f"Error inserting final batch: {str(e)[:200]}...")
        
        total_standardized = standardized_count["google"] + standardized_count["trustpilot"]
        total_translations = sum(translation_count.values())
        
        self.logger.info(f"\n{'='*60}")
        self.logger.info(f"Language standardization complete!")
        self.logger.info(f"Standardized {total_standardized} new reviews: "
                        f"Google={standardized_count['google']}, Trustpilot={standardized_count['trustpilot']}")
        self.logger.info(f"Total translations performed: {getattr(self, 'translation_counter', 0)}")
        self.logger.info(f"{'='*60}")
        
        return {
            "standardized": standardized_count,
            "translations_needed": translation_count
        }
        
        total_standardized = standardized_count["google"] + standardized_count["trustpilot"]
        total_translations = sum(translation_count.values())
        
        self.logger.info(f"\n{'='*60}")
        self.logger.info(f"Language standardization complete!")
        self.logger.info(f"Standardized {total_standardized} new reviews: "
                        f"Google={standardized_count['google']}, Trustpilot={standardized_count['trustpilot']}")
        self.logger.info(f"Total translations performed: {getattr(self, 'translation_counter', 0)}")
        self.logger.info(f"{'='*60}")
        
        return {
            "standardized": standardized_count,
            "translations_needed": translation_count
        }
    
    def get_ls_unified_reviews_stats(self) -> Dict:
        """Get statistics about language standardized reviews"""
        try:
            pipeline = [
                {
                    "$group": {
                        "_id": "$platform",
                        "count": {"$sum": 1},
                        "avg_rating": {"$avg": "$rating"},
                        "has_owner_response": {
                            "$sum": {
                                "$cond": [{"$ne": ["$response_from_owner_text", None]}, 1, 0]
                            }
                        }
                    }
                }
            ]
            
            platform_stats = list(self.db.ls_unified_reviews.aggregate(pipeline))
            
            # Additional stats for response languages
            response_lang_pipeline = [
                {
                    "$match": {"response_from_owner_language": {"$ne": None}}
                },
                {
                    "$group": {
                        "_id": "$response_from_owner_language",
                        "count": {"$sum": 1}
                    }
                }
            ]
            
            response_lang_stats = list(self.db.ls_unified_reviews.aggregate(response_lang_pipeline))
            
            total_reviews = self.db.ls_unified_reviews.count_documents({})
            
            return {
                "total_reviews": total_reviews,
                "platform_breakdown": platform_stats,
                "response_language_breakdown": response_lang_stats,
                "last_updated": datetime.utcnow()
            }
        except Exception as e:
            self.logger.error(f"Error getting language standardized reviews stats: {e}")
            return {}
    
    def get_ls_unified_reviews_by_establishment(self, establishment_id: str, 
                                              platform: str = None, 
                                              limit: int = None) -> List[Dict]:
        """
        Get language standardized reviews for a specific establishment
        
        Args:
            establishment_id: The establishment ID
            platform: Optional platform filter ('google' or 'trustpilot')
            limit: Optional limit on number of reviews returned
        """
        query = {"establishment_id": establishment_id}
        if platform:
            query["platform"] = platform
        
        try:
            cursor = self.db.ls_unified_reviews.find(query).sort("review_date", -1)
            if limit:
                cursor = cursor.limit(limit)
            
            return list(cursor)
        except Exception as e:
            self.logger.error(f"Error fetching LS reviews: {e}")
            return []
    
    def close_connection(self):
        """Close database connection"""
        if self.client:
            self.client.close()
            self.logger.info("Database connection closed")