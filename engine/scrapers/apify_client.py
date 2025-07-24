import logging
from typing import Dict, List, Optional
from urllib.parse import urlparse
from apify_client import ApifyClient as ApifyClientSDK

class ApifyClient:
    def __init__(self, api_token: str):
        self.client = ApifyClientSDK(api_token)
        self.logger = logging.getLogger(__name__)
        
        # Actor IDs
        self.GOOGLE_ACTOR_ID = "Xb8osYTtOjlsgI6k9"
        self.TRUSTPILOT_ACTOR_ID = "fLXimoyuhE1UQgDbM"
    
    def scrape_google_reviews(self, google_url: str) -> List[Dict]:
        """Scrape Google Maps reviews using Apify client"""
        self.logger.info(f"Starting Google scrape for: {google_url}")
        
        run_input = {
            "startUrls": [{"url": google_url}],
            "language": "en",
            "maxReviews": 99999,
            "personalData": False
        }
        
        try:
            # Call the actor
            run = self.client.actor(self.GOOGLE_ACTOR_ID).call(run_input=run_input)
            
            # Get results
            results = []
            for item in self.client.dataset(run["defaultDatasetId"]).iterate_items():
                results.append(item)
            
            self.logger.info(f"Retrieved {len(results)} Google reviews")
            return self._process_google_reviews(results, google_url)
            
        except Exception as e:
            self.logger.error(f"Error scraping Google reviews: {e}")
            return []
    
    def scrape_trustpilot_reviews(self, website: str) -> List[Dict]:
        """Scrape Trustpilot reviews using Apify client"""
        # Extract domain from website URL
        domain = urlparse(website).netloc
        if domain.startswith('www.'):
            domain = domain[4:]
        
        trustpilot_domain = f"{domain}?languages=all"
        self.logger.info(f"Starting Trustpilot scrape for: {trustpilot_domain}")
        
        run_input = {
            "companyDomain": trustpilot_domain,
            "count": 100,
            "replies": False,
            "startPage": 1,
            "verified": False
        }
        
        try:
            # Call the actor
            run = self.client.actor(self.TRUSTPILOT_ACTOR_ID).call(run_input=run_input)
            
            # Get results
            results = []
            for item in self.client.dataset(run["defaultDatasetId"]).iterate_items():
                results.append(item)
            
            self.logger.info(f"Retrieved {len(results)} Trustpilot reviews")
            return self._process_trustpilot_reviews(results, website)
            
        except Exception as e:
            self.logger.error(f"Error scraping Trustpilot reviews: {e}")
            return []
    
    def _process_google_reviews(self, raw_reviews: List[Dict], source_url: str) -> List[Dict]:
        """Process Google reviews - flatten raw data and add minimal metadata"""
        processed = []
        
        for review in raw_reviews:
            try:
                # Start with the raw review data (flattened)
                processed_review = review.copy()
                
                # Add our metadata fields
                processed_review.update({
                    "review_id": review.get("reviewId"),  # Use the actual reviewId from raw data
                    "rating": review.get("stars"),  # Use stars field from raw data
                    "source_url": source_url,
                    # Don't add establishment_id, platform, scraped_at here - DB manager will add them
                })
                
                processed.append(processed_review)
                
            except Exception as e:
                self.logger.warning(f"Error processing Google review: {e}")
                continue
        
        return processed
    
    def _process_trustpilot_reviews(self, raw_reviews: List[Dict], source_url: str) -> List[Dict]:
        """Process Trustpilot reviews - flatten raw data and add minimal metadata"""
        processed = []
        
        for review in raw_reviews:
            try:
                # Start with the raw review data (flattened)
                processed_review = review.copy()
                
                # Remove authorName if it exists (as per requirements)
                processed_review.pop('authorName', None)
                
                # Add our metadata fields
                processed_review.update({
                    "review_id": review.get("reviewUrl", ""),  # Use reviewUrl as review_id
                    "verified": review.get("verificationLevel") == "verified",  # Convert to boolean
                    "source_url": source_url,
                    # Don't add establishment_id, platform, scraped_at here - DB manager will add them
                })
                
                processed.append(processed_review)
                
            except Exception as e:
                self.logger.warning(f"Error processing Trustpilot review: {e}")
                continue
        
        return processed
