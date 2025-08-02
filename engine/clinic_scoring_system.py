# clinic_scoring_system.py
import os
import sys
import logging
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
from collections import defaultdict, Counter
import statistics

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from database.db_manager import DatabaseManager

class ClinicScoringSystem:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.logger = self._setup_logging()
        
        # Scoring configuration
        self.PRIOR_WEIGHT = 100  # Bayesian prior weight
        self.SERVICE_QUALITY_WEIGHTS = {
            'treatment_satisfaction': 0.3,
            'post_op': 0.2,
            'staff_satisfaction': 0.3,
            'facility': 0.2
        }
        self.COMMUNICATION_WEIGHTS = {
            'onsite_communication': 0.4,
            'scheduling': 0.2,
            'online_communication': 0.3
        }
    
    def _setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('clinic_scoring.log'),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger(__name__)
    
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
        
        self.logger.info("Clinic scoring system initialized successfully")
        return True
    
    def _calculate_prior_average(self):
        """Calculate sample average rating across all reviews"""
        pipeline = [
            {
                "$match": {
                    "rating": {"$exists": True, "$ne": None, "$gt": 0}
                }
            },
            {
                "$group": {
                    "_id": None,
                    "avg_rating": {"$avg": "$rating"}
                }
            }
        ]
        
        result = list(self.db_manager.db.ls_unified_reviews.aggregate(pipeline))
        if result and result[0].get('avg_rating'):
            prior_avg = result[0]['avg_rating']
            self.logger.info(f"Calculated prior average rating: {prior_avg:.3f}")
            return prior_avg
        else:
            # Fallback to a reasonable default
            default_avg = 4.0
            self.logger.warning(f"Could not calculate prior average, using default: {default_avg}")
            return default_avg
    
    def _calculate_adjusted_rating(self, ratings: List[float], prior_avg: float):
        """Calculate Bayesian adjusted rating"""
        if not ratings:
            return None
        
        review_count = len(ratings)
        clinic_avg = statistics.mean(ratings)
        
        adjusted_rating = (
            (self.PRIOR_WEIGHT * prior_avg + review_count * clinic_avg) / 
            (self.PRIOR_WEIGHT + review_count)
        )
        
        return round(adjusted_rating, 3)
    
    def _calculate_online_communication_score(self, enriched_data: Dict):
        """Calculate online communication score using rule-based logic"""
        is_complaint = enriched_data.get('is_complaint', 0)
        has_response = enriched_data.get('has_response', 0)
        has_constructive_response = enriched_data.get('has_constructive_response', 0)
        
        # Rule-based scoring
        if is_complaint != 1:
            return 0  # Don't care about non-complaints
        elif is_complaint == 1 and has_response == 0:
            return 1  # Complaint without response
        elif is_complaint == 1 and has_response == 1 and has_constructive_response == 1:
            return 3  # Complaint with constructive response
        elif is_complaint == 1 and has_response == 1:
            return 2  # Complaint with response but not constructive
        else:
            return 0  # Default case
    
    def _calculate_nps_score(self, score_counts: Counter):
        """Calculate NPS-style score from score distribution"""
        # Only count scores 1, 2, 3 (exclude 0 = not mentioned)
        positive_count = score_counts.get(3, 0)  # Score 3 = positive
        neutral_count = score_counts.get(2, 0)   # Score 2 = neutral/mixed
        negative_count = score_counts.get(1, 0)  # Score 1 = negative
        
        total_count = positive_count + neutral_count + negative_count
        
        if total_count == 0:
            return None  # No data to calculate score
        
        nps_score = ((positive_count - negative_count) / total_count) * 100
        return round(nps_score, 2)
    
    def _calculate_composite_score(self, individual_scores: Dict, weights: Dict):
        """Calculate weighted composite score"""
        weighted_sum = 0
        total_weight = 0
        
        for attribute, weight in weights.items():
            score = individual_scores.get(attribute)
            if score is not None:  # Only include attributes with valid scores
                weighted_sum += score * weight
                total_weight += weight
        
        if total_weight == 0:
            return None  # No valid scores to calculate composite
        
        # Normalize by actual total weight used
        composite_score = weighted_sum / total_weight
        return round(composite_score, 2)
    
    def _get_establishment_data(self, establishment_id: str):
        """Get all review data for a specific establishment"""
        # Get ratings from ls_unified_reviews
        ratings_query = {
            "establishment_id": establishment_id,
            "rating": {"$exists": True, "$ne": None, "$gt": 0}
        }
        
        rating_reviews = list(self.db_manager.db.ls_unified_reviews.find(
            ratings_query, 
            {"_id": 1, "rating": 1}
        ))
        
        # Get enriched attributes from enriched_reviews
        enriched_query = {"establishment_id": establishment_id}
        enriched_reviews = list(self.db_manager.db.enriched_reviews.find(enriched_query))
        
        return rating_reviews, enriched_reviews
    
    def calculate_establishment_scores(self, establishment_id: str, prior_avg: float):
        """Calculate all scores for a single establishment"""
        # Get data
        rating_reviews, enriched_reviews = self._get_establishment_data(establishment_id)
        
        # Extract ratings
        ratings = [review['rating'] for review in rating_reviews]
        
        # Calculate adjusted rating
        adjusted_rating = self._calculate_adjusted_rating(ratings, prior_avg)
        
        # Initialize score counters for each attribute
        attribute_scores = defaultdict(Counter)
        online_comm_scores = Counter()
        
        # Process enriched reviews
        for enriched in enriched_reviews:
            # Calculate online communication score
            online_comm_score = self._calculate_online_communication_score(enriched)
            online_comm_scores[online_comm_score] += 1
            
            # Collect other attribute scores
            for attribute in ['staff_satisfaction', 'scheduling', 'treatment_satisfaction', 
                            'onsite_communication', 'facility', 'post_op', 'affordability', 
                            'recommendation']:
                score = enriched.get(attribute)
                if score is not None:
                    attribute_scores[attribute][score] += 1
        
        # Add online communication to attribute scores
        attribute_scores['online_communication'] = online_comm_scores
        
        # Calculate NPS scores for each attribute
        nps_scores = {}
        for attribute, score_counts in attribute_scores.items():
            nps_score = self._calculate_nps_score(score_counts)
            if nps_score is not None:
                nps_scores[attribute] = nps_score
        
        # Calculate composite scores
        service_quality_score = self._calculate_composite_score(
            nps_scores, self.SERVICE_QUALITY_WEIGHTS
        )
        
        communication_score = self._calculate_composite_score(
            nps_scores, self.COMMUNICATION_WEIGHTS
        )
        
        # Prepare results
        results = {
            "adjusted_rating": adjusted_rating,
            "total_reviews_analyzed": len(set([r['_id'] for r in rating_reviews] + 
                                           [r['_id'] for r in enriched_reviews])),
            "scores_updated_at": datetime.utcnow()
        }
        
        # Add individual NPS scores
        for attribute in ['affordability', 'recommendation']:
            if attribute in nps_scores:
                results[f"{attribute}_score"] = nps_scores[attribute]
        
        # Add composite scores
        if service_quality_score is not None:
            results["service_quality_score"] = service_quality_score
        
        if communication_score is not None:
            results["communication_score"] = communication_score
        
        # Add online communication score separately for transparency
        if 'online_communication' in nps_scores:
            results["online_communication_score"] = nps_scores['online_communication']
        
        return results
    
    def process_all_establishments(self, establishment_ids: List[str] = None):
        """Process scores for all establishments"""
        self.logger.info("Starting clinic scoring process...")
        
        # Calculate prior average
        prior_avg = self._calculate_prior_average()
        
        # Get establishments to process
        if establishment_ids:
            establishments = [{"_id": eid} for eid in establishment_ids]
            self.logger.info(f"Processing {len(establishment_ids)} specified establishments")
        else:
            establishments = list(self.db_manager.db.establishments.find({}, {"_id": 1}))
            self.logger.info(f"Processing all {len(establishments)} establishments")
        
        processed_count = 0
        updated_count = 0
        
        for establishment in establishments:
            establishment_id = str(establishment["_id"])
            
            try:
                # Calculate scores
                scores = self.calculate_establishment_scores(establishment_id, prior_avg)
                
                # Update establishment document
                if scores:
                    result = self.db_manager.db.establishments.update_one(
                        {"_id": establishment["_id"]},
                        {"$set": scores}
                    )
                    
                    if result.modified_count > 0:
                        updated_count += 1
                
                processed_count += 1
                
                if processed_count % 10 == 0:
                    self.logger.info(f"Processed {processed_count}/{len(establishments)} establishments")
                
            except Exception as e:
                self.logger.error(f"Error processing establishment {establishment_id}: {e}")
                continue
        
        self.logger.info(f"Scoring complete! Processed: {processed_count}, Updated: {updated_count}")
        return {"processed": processed_count, "updated": updated_count}
    
    def get_scoring_stats(self):
        """Get statistics about the scoring process"""
        try:
            # Count establishments with scores
            establishments_with_scores = self.db_manager.db.establishments.count_documents({
                "adjusted_rating": {"$exists": True}
            })
            
            total_establishments = self.db_manager.db.establishments.count_documents({})
            
            # Get score distribution
            pipeline = [
                {
                    "$match": {"adjusted_rating": {"$exists": True}}
                },
                {
                    "$group": {
                        "_id": None,
                        "avg_adjusted_rating": {"$avg": "$adjusted_rating"},
                        "avg_service_quality": {"$avg": "$service_quality_score"},
                        "avg_communication": {"$avg": "$communication_score"},
                        "avg_affordability": {"$avg": "$affordability_score"},
                        "avg_recommendation": {"$avg": "$recommendation_score"}
                    }
                }
            ]
            
            stats_result = list(self.db_manager.db.establishments.aggregate(pipeline))
            averages = stats_result[0] if stats_result else {}
            
            return {
                "total_establishments": total_establishments,
                "establishments_with_scores": establishments_with_scores,
                "coverage_percentage": round((establishments_with_scores / total_establishments * 100), 2) if total_establishments > 0 else 0,
                "average_scores": {k: round(v, 2) if v else None for k, v in averages.items() if k != "_id"}
            }
            
        except Exception as e:
            self.logger.error(f"Error getting scoring stats: {e}")
            return {}
    
    def cleanup(self):
        """Clean up resources"""
        if self.db_manager:
            self.db_manager.close_connection()

def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Clinic Scoring System')
    parser.add_argument('--establishments', help='Comma-separated establishment IDs to process')
    parser.add_argument('--stats', action='store_true', help='Show scoring statistics')
    
    args = parser.parse_args()
    
    scorer = ClinicScoringSystem()
    
    try:
        if not scorer.initialize():
            print("Failed to initialize scoring system")
            return
        
        if args.stats:
            stats = scorer.get_scoring_stats()
            print("\n" + "="*60)
            print("CLINIC SCORING STATISTICS")
            print("="*60)
            for key, value in stats.items():
                if key == "average_scores":
                    print(f"{key}:")
                    for score_name, score_value in value.items():
                        print(f"  {score_name}: {score_value}")
                else:
                    print(f"{key}: {value}")
            print("="*60)
            return
        
        # Parse establishment IDs if provided
        establishment_ids = None
        if args.establishments:
            establishment_ids = [id.strip() for id in args.establishments.split(',')]
        
        # Process establishments
        results = scorer.process_all_establishments(establishment_ids)
        
        print(f"\n✅ Scoring completed successfully!")
        print(f"Processed: {results['processed']} establishments")
        print(f"Updated: {results['updated']} establishments")
        
    except KeyboardInterrupt:
        print("\n\n⚠️ Scoring interrupted by user")
    except Exception as e:
        print(f"\n❌ Scoring failed: {e}")
    finally:
        scorer.cleanup()

if __name__ == "__main__":
    main()