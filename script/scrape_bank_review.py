from google_play_scraper import Sort, reviews
import pandas as pd
import time
import os

def scrape_bank_reviews(bank, num_reviews=400, delay=5):
    """
    Scrape reviews for a given bank from Google Play Store and return a DataFrame.
    
    Args:
        bank (dict): Dictionary containing 'name' and 'app_id' of the bank.
        num_reviews (int): Number of reviews to scrape (default: 5).
        delay (int): Delay between requests in seconds (default: 5).
    
    Returns:
        pd.DataFrame: DataFrame containing the reviews, or None if scraping fails.
    """
    bank_playstore_id = bank["app_id"]
    print(f"\nFetching reviews for {bank['name']} (ID: {bank_playstore_id})...")
    try:
        result, continuation_token = reviews(
            bank_playstore_id,
            lang="en",
            country="et",  # Ethiopia
            sort=Sort.NEWEST,
            count=num_reviews,
            filter_score_with=None
        )
        if not result:
            print(f"No reviews found for {bank['name']}.")
            return None

        reviews_data = [
            {
                "user_name": review.get('userName', ''),
                "rating": review.get('score', ''),
                "date": review.get('at').strftime('%Y-%m-%d %H:%M:%S') if review.get('at') else 'N/A',
                "review_content": review.get('content', ''),
                "review_id": review.get('reviewId', ''),
                "app_version": review.get('appVersion', ''),
                "replied_at": str(review.get('repliedAt', '—')),
                "reply_content": review.get('replyContent', '—'),
                "thumbs_up_count": review.get('thumbsUpCount', ''),
                "user_image_url": review.get('userImage', ''),
                "bank": bank["name"],
                "source": "Google Play"
            }
            for review in result
        ]

        df = pd.DataFrame(reviews_data)
        time.sleep(delay)  # Delay to avoid rate limiting
        return df

    except Exception as e:
        print(f"Error fetching reviews for {bank['name']}: {e}")
        return None

def scrape_all_banks(banks):
    for bank in banks:
        df = scrape_bank_reviews(bank)
        if df is not None:
            df_name = f"df_{bank['name']}"
            globals()[df_name] = df
            save_dir = os.path.join('..', 'data')
            os.makedirs(save_dir, exist_ok=True)
            file_path = os.path.join(save_dir, f"{bank['name']}_reviews.csv")
            df.to_csv(file_path, index=False)
            print(f"Created {df_name} with {len(df)} reviews and saved to {file_path}")