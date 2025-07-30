
import os
import sys
import yaml
import json
import logging
from pathlib import Path
from typing import List, Dict, Optional, Set
from datetime import datetime
from dateutil.parser import parse as parse_date
from pymongo import MongoClient
from bson import ObjectId
import google.generativeai as genai

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from database.db_manager import DatabaseManager

class ReviewEnrichmentProcessor:
    def __init__(self, config_path: str = "enrichment_config.yaml"):
        self.config_path = config_path
        self.config = None
        self.db_manager = DatabaseManager()
        self.genai_model = None
        self.logger = self._setup_logging()
        
        # Token limit management (70% of 1M tokens)
        self.MAX_TOKENS = int(1_000_000 * 0.70)
        
        # Processing thresholds
        self.MIN_REVIEW_LENGTH = 10
        
        # Load configuration
        self._load_config()
        self._setup_genai()
    
    def _setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('enrichment.log'),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger(__name__)
    
    def _load_config(self):
        """Load enrichment configuration"""
        try:
            with open(self.config_path, 'r') as f:
                self.config = yaml.safe_load(f)
            self.logger.info("Enrichment configuration loaded successfully")
        except FileNotFoundError:
            self.logger.error(f"Config file '{self.config_path}' not found. Creating default config.")
            self._create_default_config()
            with open(self.config_path, 'r') as f:
                self.config = yaml.safe_load(f)
    
    def _create_default_config(self):
        """Create default configuration file"""
        default_config = {
            'sentiment_attributes': {
                'staff_satisfaction': {
                    'description': 'Sentiment about any kind of staff. Owner, nurses, doctors, receptionist etc.',
                    'enabled': True
                },
                'scheduling': {
                    'description': 'Sentiment about wait times, proper scheduling, good timing, sticking with appointments.',
                    'enabled': True
                },
                'treatment_satisfaction': {
                    'description': 'Sentiment about clinical competence, effectiveness of the treatment, competency of the expert and the knowledgeability, careful examination.',
                    'enabled': True
                },
                'onsite_communication': {
                    'description': 'Sentiment about clear, transparent explanation of procedures and risks, clear answering of questions, no language barrier (via translators etc.).',
                    'enabled': True
                },
                'facility': {
                    'description': 'Sentiment about cleanliness, modern equipment, sufficient amenities.',
                    'enabled': True
                },
                'post_op': {
                    'description': 'Sentiment about follow up communication and attention post operation.',
                    'enabled': True
                },
                'affordability': {
                    'description': 'Sentiment about price level, affordability. Cheap is positive, expensive is negative.',
                    'enabled': True
                },
                'recommendation': {
                    'description': 'Does the review indicate recommendation or would the patient visit here again?',
                    'enabled': True
                },
                'accommodation_transportation': {
                    'description': 'Sentiment about the accommodation and transportation services.',
                    'enabled': True
                }
            },
            'complaint_attribute': {
                'is_complaint': {
                    'description': 'Does the review include any complaint?',
                    'enabled': True
                }
            },
            'response_attributes': {
                'has_constructive_response': {
                    'description': 'Does the response explain the situation or offer a solution?',
                    'requires': ['has_response', 'is_complaint'],
                    'enabled': True
                },
                'has_no_threat': {
                    'description': 'Does the response threaten the reviewer with a legal action?',
                    'requires': ['has_response', 'is_complaint'],
                    'enabled': True
                }
            },
            'processing': {
                'batch_size': 30,
                'sentiment_batch_size': 3,
                'max_retries': 3
            }
        }
        
        with open(self.config_path, 'w') as f:
            yaml.dump(default_config, f, default_flow_style=False, indent=2)
        
        self.logger.info(f"Created default config file: {self.config_path}")
    
    def _setup_genai(self):
        """Setup Google Generative AI"""
        try:
            with open('tokens/google_api_key.txt', 'r') as f:
                api_key = f.read().strip()
            genai.configure(api_key=api_key)
            self.genai_model = genai.GenerativeModel('gemini-2.5-flash')
            self.logger.info("Google Generative AI configured successfully")
        except FileNotFoundError:
            self.logger.error("Google API key file not found: tokens/google_api_key.txt")
            raise
    
    def _load_tokens(self):
        """Load database tokens"""
        try:
            with open('tokens/mongodb_connection.txt', 'r') as f:
                mongodb_connection = f.read().strip()
            return mongodb_connection
        except FileNotFoundError:
            self.logger.error("MongoDB connection file not found")
            return None
    
    def initialize(self):
        """Initialize database connection"""
        mongodb_connection = self._load_tokens()
        if not mongodb_connection:
            return False
        
        if not self.db_manager.connect(mongodb_connection):
            return False
        
        # Create enriched_reviews collection indexes
        self._create_enriched_reviews_indexes()
        return True
    
    def _create_enriched_reviews_indexes(self):
        """Create indexes for enriched_reviews collection"""
        try:
            self.db_manager.db.enriched_reviews.create_index("establishment_id")
            self.db_manager.db.enriched_reviews.create_index("platform")
            self.db_manager.db.enriched_reviews.create_index("published_at_date")
            self.db_manager.db.enriched_reviews.create_index("processed_at")
            self.db_manager.db.enriched_reviews.create_index([("establishment_id", 1), ("platform", 1)])
            self.logger.info("Created indexes on enriched_reviews collection")
        except Exception as e:
            self.logger.error(f"Error creating indexes: {e}")
    
    def _parse_date_filter(self, date_str: str) -> datetime:
        """Parse date string and convert to datetime"""
        if not date_str:
            return None
        try:
            return parse_date(date_str)
        except Exception as e:
            self.logger.error(f"Error parsing date '{date_str}': {e}")
            return None
    
    def _get_reviews_to_process(self, establishment_ids: List[str] = None, 
                               published_after: str = None, 
                               incremental: bool = True) -> List[Dict]:
        """Get reviews that need processing based on parameters"""
        query = {}
        
        # Filter by establishment IDs (from config or parameter)
        config_establishments = self.config.get('target_establishments', [])
        target_establishments = establishment_ids or config_establishments
        
        if target_establishments:
            query["establishment_id"] = {"$in": target_establishments}
            self.logger.info(f"Filtering by {len(target_establishments)} target establishments")
        
        # Filter by date
        if published_after:
            parsed_date = self._parse_date_filter(published_after)
            if parsed_date:
                query["published_at_date"] = {"$gte": parsed_date.isoformat()}
        
        # Get reviews from ls_unified_reviews
        reviews = list(self.db_manager.db.ls_unified_reviews.find(query))
        
        if incremental:
            # Filter out already processed reviews
            processed_ids = set(self.db_manager.db.enriched_reviews.distinct("_id"))
            reviews = [r for r in reviews if r["_id"] not in processed_ids]
        
        # Filter by minimum length
        reviews = [r for r in reviews if self._get_review_content_length(r) >= self.MIN_REVIEW_LENGTH]
        
        self.logger.info(f"Found {len(reviews)} reviews to process")
        return reviews
    
    def _get_review_content_length(self, review: Dict) -> int:
        """Calculate character count of review content"""
        content = ""
        if review.get('title'):
            content += review['title']
        if review.get('review_text'):
            content += " " + review['review_text']
        return len(content.strip())
    
    def _calculate_basic_fields(self, review: Dict) -> Dict:
        """Calculate has_response and review_length fields"""
        has_response = 1 if review.get('response_from_owner_text') else 0
        review_length = self._get_review_content_length(review)
        
        return {
            'has_response': has_response,
            'review_length': review_length
        }
    
    def _build_sentiment_prompt(self, reviews: List[Dict], attributes: Dict) -> str:
        """Build prompt for sentiment analysis"""
        
        # Concise attributes list
        attrs_list = []
        for attr_name, attr_config in attributes.items():
            attrs_list.append(f"{attr_name}: {attr_config['description']}")
        
        prompt = f"""Analyze sentiment for: {', '.join(attrs_list)}

Scale: 0=not mentioned, 1=negative, 2=neutral/mixed, 3=positive

Return JSON: {{"review_id": {{{', '.join(f'"{attr}": 0' for attr in attributes.keys())}}}}}

Reviews:
"""
        
        for review in reviews:
            review_id = str(review['_id'])
            title = review.get('title', '') or ''
            text = review.get('review_text', '') or ''
            content = f"{title} {text}".strip()
            prompt += f"{review_id}: {content}\n"
        
        return prompt
    
    def _build_complaint_prompt(self, reviews: List[Dict]) -> str:
        """Build prompt for complaint classification"""
        
        prompt = """Classify reviews as complaint (1) or not (0).

Return JSON: {"review_id": 0}

Reviews:
"""
        
        for review in reviews:
            review_id = str(review['_id'])
            title = review.get('title', '') or ''
            text = review.get('review_text', '') or ''
            content = f"{title} {text}".strip()
            prompt += f"{review_id}: {content}\n"
        
        return prompt
    
    def _build_response_prompt(self, reviews: List[Dict], attributes: Dict) -> str:
        """Build prompt for response analysis"""
        
        # Concise attributes list
        attrs_list = []
        for attr_name, attr_config in attributes.items():
            attrs_list.append(f"{attr_name}: {attr_config['description']}")
        
        prompt = f"""Analyze owner responses for: {', '.join(attrs_list)}

Return JSON: {{"review_id": {{{', '.join(f'"{attr}": 0' for attr in attributes.keys())}}}}}

Review + Response pairs:
"""
        
        for review in reviews:
            review_id = str(review['_id'])
            title = review.get('title', '') or ''
            text = review.get('review_text', '') or ''
            response = review.get('response_from_owner_text', '') or ''
            
            review_content = f"{title} {text}".strip()
            prompt += f"{review_id}:\nReview: {review_content}\nResponse: {response}\n\n"
        
        return prompt
    
    def _estimate_token_count(self, text: str) -> int:
        """Rough estimation of token count (1 token ≈ 4 characters)"""
        return len(text) // 4
    
    def _call_gemini_batch(self, prompt: str) -> Dict:
        """Call Gemini API with error handling"""
        try:
            # Check token limit
            estimated_tokens = self._estimate_token_count(prompt)
            if estimated_tokens > self.MAX_TOKENS:
                self.logger.warning(f"Prompt too long ({estimated_tokens} tokens), skipping batch")
                return {}
            
            response = self.genai_model.generate_content(prompt)
            
            if not response or not response.text:
                self.logger.error("Empty response from Gemini")
                return {}
            
            # Clean and parse JSON response
            try:
                response_text = response.text.strip()
                
                # Remove markdown code blocks if present
                if response_text.startswith('```json'):
                    response_text = response_text[7:]  # Remove ```json
                elif response_text.startswith('```'):
                    response_text = response_text[3:]   # Remove ```
                
                if response_text.endswith('```'):
                    response_text = response_text[:-3]  # Remove trailing ```
                
                response_text = response_text.strip()
                
                # Parse JSON
                result = json.loads(response_text)
                return result
                
            except json.JSONDecodeError as e:
                self.logger.error(f"Failed to parse Gemini response as JSON: {e}")
                self.logger.error(f"Raw response: {response.text[:500]}...")
                
                # Try to extract JSON from the response using regex as fallback
                import re
                json_match = re.search(r'\{.*\}', response.text, re.DOTALL)
                if json_match:
                    try:
                        result = json.loads(json_match.group())
                        self.logger.info("Successfully extracted JSON using regex fallback")
                        return result
                    except json.JSONDecodeError:
                        pass
                
                return {}
                
        except Exception as e:
            self.logger.error(f"Gemini API call failed: {e}")
            return {}
    
    def _validate_sentiment_response(self, response_data: Dict, expected_attributes: Set[str]) -> Dict:
        """Validate sentiment analysis response format"""
        validated_data = {}
        
        if not isinstance(response_data, dict):
            self.logger.warning("Response data is not a dictionary")
            return validated_data
        
        for review_id, attributes in response_data.items():
            if not isinstance(attributes, dict):
                self.logger.warning(f"Invalid format for review {review_id}: expected dict, got {type(attributes)}")
                continue
            
            validated_attributes = {}
            for attr_name, value in attributes.items():
                if attr_name in expected_attributes and isinstance(value, int) and 0 <= value <= 3:
                    validated_attributes[attr_name] = value
                else:
                    self.logger.warning(f"Invalid attribute {attr_name}={value} for review {review_id}")
            
            if validated_attributes:
                validated_data[review_id] = validated_attributes
        
        return validated_data
    
    def _validate_binary_response(self, response_data: Dict, expected_attributes: Set[str]) -> Dict:
        """Validate binary classification response format"""
        validated_data = {}
        
        if not isinstance(response_data, dict):
            self.logger.warning("Response data is not a dictionary")
            return validated_data
        
        for review_id, attributes in response_data.items():
            if isinstance(attributes, int):
                # Handle complaint attribute (single integer)
                if 'is_complaint' in expected_attributes and 0 <= attributes <= 1:
                    validated_data[review_id] = {'is_complaint': attributes}
            elif isinstance(attributes, dict):
                # Handle response attributes (dictionary)
                validated_attributes = {}
                for attr_name, value in attributes.items():
                    if attr_name in expected_attributes and isinstance(value, int) and 0 <= value <= 1:
                        validated_attributes[attr_name] = value
                    else:
                        self.logger.warning(f"Invalid attribute {attr_name}={value} for review {review_id}")
                
                if validated_attributes:
                    validated_data[review_id] = validated_attributes
            else:
                self.logger.warning(f"Invalid format for review {review_id}: {type(attributes)}")
        
        return validated_data
    
    def _process_sentiment_attributes(self, reviews: List[Dict], attributes: Dict) -> Dict:
        """Process sentiment attributes (0-3 scale)"""
        batch_size = self.config['processing']['batch_size']
        enrichment_data = {}
        
        self.logger.info(f"Processing {len(attributes)} sentiment attributes for {len(reviews)} reviews")
        
        # Process reviews in batches
        for i in range(0, len(reviews), batch_size):
            batch = reviews[i:i + batch_size]
            self.logger.info(f"Processing sentiment batch {i//batch_size + 1}: reviews {i+1}-{min(i+batch_size, len(reviews))}")
            
            prompt = self._build_sentiment_prompt(batch, attributes)
            response_data = self._call_gemini_batch(prompt)
            
            if response_data:
                validated_data = self._validate_sentiment_response(response_data, set(attributes.keys()))
                
                for review_id, attr_data in validated_data.items():
                    if review_id not in enrichment_data:
                        enrichment_data[review_id] = {}
                    enrichment_data[review_id].update(attr_data)
            
        return enrichment_data
    
    def _process_complaint_attribute(self, reviews: List[Dict]) -> Dict:
        """Process complaint classification"""
        batch_size = self.config['processing']['batch_size']
        enrichment_data = {}
        
        self.logger.info(f"Processing complaint classification for {len(reviews)} reviews")
        
        # Process reviews in batches
        for i in range(0, len(reviews), batch_size):
            batch = reviews[i:i + batch_size]
            self.logger.info(f"Processing complaint batch {i//batch_size + 1}: reviews {i+1}-{min(i+batch_size, len(reviews))}")
            
            prompt = self._build_complaint_prompt(batch)
            response_data = self._call_gemini_batch(prompt)
            
            if response_data:
                validated_data = self._validate_binary_response(response_data, {'is_complaint'})
                
                for review_id, attr_data in validated_data.items():
                    if review_id not in enrichment_data:
                        enrichment_data[review_id] = {}
                    enrichment_data[review_id].update(attr_data)
            
        return enrichment_data
    
    def _process_response_attributes(self, reviews: List[Dict], attributes: Dict) -> Dict:
        """Process response-specific attributes"""
        # Filter reviews that have responses and complaints
        eligible_reviews = []
        for review in reviews:
            has_response = review.get('response_from_owner_text')
            # We need is_complaint to be already processed, so we check enriched_reviews
            existing_enrichment = self.db_manager.db.enriched_reviews.find_one({"_id": review["_id"]})
            is_complaint = existing_enrichment.get('is_complaint', 0) if existing_enrichment else 0
            
            if has_response and is_complaint == 1:
                eligible_reviews.append(review)
        
        if not eligible_reviews:
            self.logger.info("No reviews eligible for response attribute analysis")
            return {}
        
        batch_size = self.config['processing']['batch_size']
        enrichment_data = {}
        
        self.logger.info(f"Processing {len(attributes)} response attributes for {len(eligible_reviews)} eligible reviews")
        
        # Process reviews in batches
        for i in range(0, len(eligible_reviews), batch_size):
            batch = eligible_reviews[i:i + batch_size]
            self.logger.info(f"Processing response batch {i//batch_size + 1}: reviews {i+1}-{min(i+batch_size, len(eligible_reviews))}")
            
            prompt = self._build_response_prompt(batch, attributes)
            response_data = self._call_gemini_batch(prompt)
            
            if response_data:
                validated_data = self._validate_binary_response(response_data, set(attributes.keys()))
                
                for review_id, attr_data in validated_data.items():
                    if review_id not in enrichment_data:
                        enrichment_data[review_id] = {}
                    enrichment_data[review_id].update(attr_data)
            
        return enrichment_data
    
    def _upsert_enriched_reviews(self, enrichment_data: Dict, reviews: List[Dict]):
        """Upsert enrichment data to enriched_reviews collection"""
        if not enrichment_data:
            self.logger.info("No enrichment data to upsert")
            return
        
        from pymongo import UpdateOne
        
        operations = []
        processed_at = datetime.utcnow()
        
        for review in reviews:
            review_id = str(review['_id'])
            
            # Start with basic fields
            update_data = self._calculate_basic_fields(review)
            
            # Add LLM-generated fields if available
            if review_id in enrichment_data:
                update_data.update(enrichment_data[review_id])
            
            # Add metadata
            update_data.update({
                'establishment_id': review['establishment_id'],
                'platform': review['platform'],
                'published_at_date': review.get('published_at_date'),
                'processed_at': processed_at,
                'updated_at': processed_at
            })
            
            # Create proper UpdateOne operation
            operation = UpdateOne(
                {'_id': ObjectId(review_id)},
                {'$set': update_data},
                upsert=True
            )
            operations.append(operation)
        
        if operations:
            try:
                result = self.db_manager.db.enriched_reviews.bulk_write(operations)
                self.logger.info(f"Upserted {result.upserted_count} new and modified {result.modified_count} existing enriched reviews")
            except Exception as e:
                self.logger.error(f"Error upserting enriched reviews: {e}")
                # Log the first few operations for debugging
                self.logger.error(f"Sample operation: {operations[0]}")
                raise
    
    def process_reviews(self, establishment_ids: List[str] = None, 
                       published_after: str = None, 
                       incremental: bool = True,
                       attribute_groups: List[str] = None):
        """
        Main processing method
        
        Args:
            establishment_ids: List of establishment IDs to process
            published_after: ISO date string to filter reviews
            incremental: Only process unprocessed reviews
            attribute_groups: List of attribute groups to process 
                            ('sentiment', 'complaint', 'response', 'all')
        """
        if not self.initialize():
            self.logger.error("Failed to initialize processor")
            return False
        
        try:
            # Get reviews to process
            reviews = self._get_reviews_to_process(establishment_ids, published_after, incremental)
            
            if not reviews:
                self.logger.info("No reviews to process")
                return True
            
            # Determine which attribute groups to process
            if not attribute_groups:
                attribute_groups = ['all']
            
            if 'all' in attribute_groups:
                attribute_groups = ['sentiment', 'complaint', 'response']
            
            enrichment_data = {}
            
            # Process sentiment attributes
            if 'sentiment' in attribute_groups:
                enabled_sentiment_attrs = {
                    name: config for name, config in self.config['sentiment_attributes'].items()
                    if config.get('enabled', True)
                }
                
                if enabled_sentiment_attrs:
                    # Process in smaller batches by attribute group
                    sentiment_batch_size = self.config['processing'].get('sentiment_batch_size', 3)
                    attr_names = list(enabled_sentiment_attrs.keys())
                    
                    for i in range(0, len(attr_names), sentiment_batch_size):
                        attr_batch = {name: enabled_sentiment_attrs[name] 
                                    for name in attr_names[i:i + sentiment_batch_size]}
                        
                        self.logger.info(f"Processing sentiment attributes: {list(attr_batch.keys())}")
                        batch_data = self._process_sentiment_attributes(reviews, attr_batch)
                        
                        # Merge results - ensure we're working with dictionaries
                        if isinstance(batch_data, dict):
                            for review_id, attrs in batch_data.items():
                                if review_id not in enrichment_data:
                                    enrichment_data[review_id] = {}
                                if isinstance(attrs, dict):
                                    enrichment_data[review_id].update(attrs)
                                else:
                                    self.logger.warning(f"Invalid attrs format for review {review_id}: {type(attrs)}")
                        else:
                            self.logger.warning(f"Invalid batch_data format: {type(batch_data)}")
            
            # Process complaint attribute
            if 'complaint' in attribute_groups:
                if self.config['complaint_attribute']['is_complaint'].get('enabled', True):
                    self.logger.info("Processing complaint classification")
                    complaint_data = self._process_complaint_attribute(reviews)
                    
                    # Merge results - ensure we're working with dictionaries
                    if isinstance(complaint_data, dict):
                        for review_id, attrs in complaint_data.items():
                            if review_id not in enrichment_data:
                                enrichment_data[review_id] = {}
                            if isinstance(attrs, dict):
                                enrichment_data[review_id].update(attrs)
                            else:
                                self.logger.warning(f"Invalid attrs format for review {review_id}: {type(attrs)}")
                    else:
                        self.logger.warning(f"Invalid complaint_data format: {type(complaint_data)}")
            
            # Upsert current data before processing response attributes
            self._upsert_enriched_reviews(enrichment_data, reviews)
            
            # Process response attributes (requires complaint data to be available)
            if 'response' in attribute_groups:
                enabled_response_attrs = {
                    name: config for name, config in self.config['response_attributes'].items()
                    if config.get('enabled', True)
                }
                
                if enabled_response_attrs:
                    self.logger.info("Processing response attributes")
                    response_data = self._process_response_attributes(reviews, enabled_response_attrs)
                    
                    # Merge results - ensure we're working with dictionaries
                    if isinstance(response_data, dict):
                        for review_id, attrs in response_data.items():
                            if review_id not in enrichment_data:
                                enrichment_data[review_id] = {}
                            if isinstance(attrs, dict):
                                enrichment_data[review_id].update(attrs)
                            else:
                                self.logger.warning(f"Invalid attrs format for review {review_id}: {type(attrs)}")
                    else:
                        self.logger.warning(f"Invalid response_data format: {type(response_data)}")
            
            # Final upsert
            self._upsert_enriched_reviews(enrichment_data, reviews)
            
            self.logger.info("Review enrichment processing completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error during review processing: {e}")
            return False
        
        finally:
            self.db_manager.close_connection()
    
    def get_processing_stats(self) -> Dict:
        """Get statistics about processed reviews"""
        try:
            total_ls_reviews = self.db_manager.db.ls_unified_reviews.count_documents({})
            total_enriched = self.db_manager.db.enriched_reviews.count_documents({})
            
            # Platform breakdown
            platform_pipeline = [
                {
                    "$group": {
                        "_id": "$platform",
                        "count": {"$sum": 1}
                    }
                }
            ]
            
            platform_stats = list(self.db_manager.db.enriched_reviews.aggregate(platform_pipeline))
            
            # Find unprocessed reviews
            enriched_ids = set(self.db_manager.db.enriched_reviews.distinct("_id"))
            ls_ids = set(self.db_manager.db.ls_unified_reviews.distinct("_id"))
            unprocessed_count = len(ls_ids - enriched_ids)
            
            return {
                "total_ls_reviews": total_ls_reviews,
                "total_enriched": total_enriched,
                "unprocessed_count": unprocessed_count,
                "platform_breakdown": platform_stats,
                "processing_coverage": f"{total_enriched}/{total_ls_reviews} ({(total_enriched/total_ls_reviews*100):.1f}%)" if total_ls_reviews > 0 else "0/0 (0%)"
            }
            
        except Exception as e:
            self.logger.error(f"Error getting processing stats: {e}")
            return {}

def main():
    """Main entry point for testing"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Review Enrichment Processor')
    parser.add_argument('--config', default='enrichment_config.yaml', help='Configuration file path')
    parser.add_argument('--establishments', help='Comma-separated establishment IDs')
    parser.add_argument('--published-after', help='ISO date string (e.g., 2024-01-01T00:00:00.000Z)')
    parser.add_argument('--no-incremental', action='store_true', help='Process all reviews (not just new ones)')
    parser.add_argument('--attributes', help='Comma-separated attribute groups (sentiment,complaint,response,all)')
    parser.add_argument('--stats', action='store_true', help='Show processing statistics')
    
    args = parser.parse_args()
    
    processor = ReviewEnrichmentProcessor(args.config)
    
    if args.stats:
        stats = processor.get_processing_stats()
        print("\n" + "="*60)
        print("REVIEW ENRICHMENT STATISTICS")
        print("="*60)
        for key, value in stats.items():
            print(f"{key}: {value}")
        print("="*60)
        return
    
    # Parse arguments
    establishment_ids = None
    if args.establishments:
        establishment_ids = [id.strip() for id in args.establishments.split(',')]
    
    attribute_groups = None
    if args.attributes:
        attribute_groups = [attr.strip() for attr in args.attributes.split(',')]
    
    incremental = not args.no_incremental
    
    # Process reviews
    success = processor.process_reviews(
        establishment_ids=establishment_ids,
        published_after=args.published_after,
        incremental=incremental,
        attribute_groups=attribute_groups
    )
    
    if success:
        print("\n✅ Review enrichment completed successfully!")
    else:
        print("\n❌ Review enrichment failed!")

if __name__ == "__main__":
    main()