# scrapers/apify_client.py
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
            return self._standardize_google_reviews(results, google_url)
            
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
            return self._standardize_trustpilot_reviews(results, website)
            
        except Exception as e:
            self.logger.error(f"Error scraping Trustpilot reviews: {e}")
            return []
    
    def _standardize_google_reviews(self, raw_reviews: List[Dict], source_url: str) -> List[Dict]:
        """Standardize Google reviews format"""
        standardized = []
        
        for review in raw_reviews:
            try:
                standardized_review = {
                    "review_id": review.get("reviewId", ""),
                    "author": review.get("name", ""),
                    "rating": review.get("stars", 0),
                    "text": review.get("text", ""),
                    "date": review.get("publishedAtDate", ""),
                    "likes": review.get("likesCount", 0),
                    "source_url": source_url,
                    "raw_data": review
                }
                standardized.append(standardized_review)
                
            except Exception as e:
                self.logger.warning(f"Error standardizing Google review: {e}")
                continue
        
        return standardized
    
    def _standardize_trustpilot_reviews(self, raw_reviews: List[Dict], source_url: str) -> List[Dict]:
        """Standardize Trustpilot reviews format"""
        standardized = []
        
        for review in raw_reviews:
            try:
                standardized_review = {
                    "review_id": review.get("id", ""),
                    "rating": review.get("rating", 0),
                    "title": review.get("title", ""),
                    "text": review.get("text", ""),
                    "date": review.get("date", ""),
                    "verified": review.get("verified", False),
                    "source_url": source_url,
                    "raw_data": review
                }
                standardized.append(standardized_review)
                
            except Exception as e:
                self.logger.warning(f"Error standardizing Trustpilot review: {e}")
                continue
        
        return standardized