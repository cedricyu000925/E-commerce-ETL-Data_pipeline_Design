"""
Extract reviews data from CSV
Includes review score analysis and sentiment validation
"""

import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.utils.logger import setup_logger
import yaml

logger = setup_logger('extract_reviews')


class ReviewsExtractor:
    """Extract and validate reviews data"""
    
    def __init__(self, config_path='config/file_paths.yaml'):
        """Initialize with file paths from config"""
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        self.file_path = config['data_paths']['reviews']
        logger.info(f"ReviewsExtractor initialized with path: {self.file_path}")
    
    def extract(self):
        """
        Extract reviews data from CSV
        
        Returns:
            DataFrame: Reviews data with parsed dates
        """
        try:
            logger.info("Starting reviews extraction...")
            
            # Read CSV
            df = pd.read_csv(self.file_path)
            logger.info(f"✓ Loaded {len(df):,} rows from {self.file_path}")
            
            # Parse date columns
            date_columns = [
                'review_creation_date',
                'review_answer_timestamp'
            ]
            
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    logger.info(f"✓ Parsed date column: {col}")
            
            # Basic info logging
            if 'order_id' in df.columns:
                logger.info(f"  - Orders with reviews: {df['order_id'].nunique():,}")
            
            if 'review_score' in df.columns:
                logger.info(f"  - Average review score: {df['review_score'].mean():.2f}/5.0")
                logger.info(f"  - Review score distribution:")
                score_dist = df['review_score'].value_counts().sort_index()
                for score, count in score_dist.items():
                    percentage = (count / len(df)) * 100
                    logger.info(f"    → {score} stars: {count:,} ({percentage:.1f}%)")
            
            # Validate data quality
            self._validate_data(df)
            
            logger.info(f"✓ Reviews extraction complete: {len(df):,} rows")
            return df
            
        except Exception as e:
            logger.error(f"✗ Reviews extraction failed: {e}")
            raise
    
    def _validate_data(self, df):
        """Validate reviews data quality"""
        
        # Check for missing review_id (should be unique)
        if 'review_id' in df.columns:
            duplicate_reviews = df['review_id'].duplicated().sum()
            if duplicate_reviews > 0:
                logger.warning(f"⚠ Found {duplicate_reviews} duplicate review_ids")
            else:
                logger.info("✓ No duplicate review_ids")
            
            missing_review_id = df['review_id'].isnull().sum()
            if missing_review_id > 0:
                logger.error(f"✗ Missing review_id: {missing_review_id} nulls")
                raise ValueError("review_id cannot be null")
        
        # Check for missing order_id
        if 'order_id' in df.columns:
            missing_order_id = df['order_id'].isnull().sum()
            if missing_order_id > 0:
                logger.error(f"✗ Missing order_id: {missing_order_id} nulls")
                raise ValueError("order_id cannot be null in reviews")
            else:
                logger.info("✓ No missing order_ids")
        
        # Check review scores
        if 'review_score' in df.columns:
            missing_scores = df['review_score'].isnull().sum()
            if missing_scores > 0:
                logger.warning(f"⚠ Missing review_score: {missing_scores} nulls ({missing_scores/len(df)*100:.2f}%)")
            
            # Check for invalid scores (should be 1-5)
            invalid_scores = df[(df['review_score'] < 1) | (df['review_score'] > 5)]
            if len(invalid_scores) > 0:
                logger.error(f"✗ Invalid review scores (not 1-5): {len(invalid_scores)}")
            else:
                logger.info("✓ All review scores are valid (1-5)")
        
        # Check review comments
        if 'review_comment_title' in df.columns:
            reviews_with_title = df['review_comment_title'].notna().sum()
            logger.info(f"  - Reviews with title: {reviews_with_title:,} ({reviews_with_title/len(df)*100:.1f}%)")
        
        if 'review_comment_message' in df.columns:
            reviews_with_message = df['review_comment_message'].notna().sum()
            logger.info(f"  - Reviews with message: {reviews_with_message:,} ({reviews_with_message/len(df)*100:.1f}%)")
        
        # Check review answer timestamp (seller responses)
        if 'review_answer_timestamp' in df.columns:
            reviews_with_answer = df['review_answer_timestamp'].notna().sum()
            logger.info(f"  - Reviews with seller answer: {reviews_with_answer:,} ({reviews_with_answer/len(df)*100:.1f}%)")
        
        # Analyze sentiment (simple categorization)
        if 'review_score' in df.columns:
            positive_reviews = (df['review_score'] >= 4).sum()
            neutral_reviews = (df['review_score'] == 3).sum()
            negative_reviews = (df['review_score'] <= 2).sum()
            
            logger.info(f"  - Sentiment breakdown:")
            logger.info(f"    → Positive (4-5 stars): {positive_reviews:,} ({positive_reviews/len(df)*100:.1f}%)")
            logger.info(f"    → Neutral (3 stars): {neutral_reviews:,} ({neutral_reviews/len(df)*100:.1f}%)")
            logger.info(f"    → Negative (1-2 stars): {negative_reviews:,} ({negative_reviews/len(df)*100:.1f}%)")
        
        logger.info("✓ Review data quality validation passed")


# Test the extractor
if __name__ == "__main__":
    extractor = ReviewsExtractor()
    df_reviews = extractor.extract()
    
    print("\n" + "="*80)
    print("REVIEWS EXTRACTION TEST")
    print("="*80)
    print(f"\nTotal rows: {len(df_reviews):,}")
    print(f"Columns: {list(df_reviews.columns)}")
    
    print("\nFirst 5 rows:")
    print(df_reviews.head())
    
    print("\nData types:")
    print(df_reviews.dtypes)
    
    print("\nMissing values:")
    print(df_reviews.isnull().sum())
    
    if 'review_score' in df_reviews.columns:
        print("\nReview score distribution:")
        print(df_reviews['review_score'].value_counts().sort_index())
        print(f"\nAverage review score: {df_reviews['review_score'].mean():.2f}/5.0")
    
    # Sample reviews
    if 'review_comment_message' in df_reviews.columns:
        reviews_with_comments = df_reviews[df_reviews['review_comment_message'].notna()]
        if len(reviews_with_comments) > 0:
            print(f"\nSample review comment:")
            print(reviews_with_comments.iloc[0]['review_comment_message'][:200] + "...")
