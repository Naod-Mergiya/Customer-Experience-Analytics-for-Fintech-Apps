



import pandas as pd
from datetime import datetime
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Load raw data
file_path_BOA = os.path.join(os.path.dirname(__file__), '..', 'data', 'BOA_reviews.csv')
file_path_CBE = os.path.join(os.path.dirname(__file__), '..', 'data', 'CBE_reviews.csv')
file_path_Dashen = os.path.join(os.path.dirname(__file__), '..', 'data', 'Dashen_reviews.csv')

df_BOA = pd.read_csv(file_path_BOA)
df_CBE = pd.read_csv(file_path_CBE)
df_Dashen = pd.read_csv(file_path_Dashen)

df = pd.concat([df_CBE, df_BOA, df_Dashen], ignore_index=True)



# 1. Remove duplicates
print(f"Initial rows: {len(df)}")
df = df.drop_duplicates(subset=["review", "date", "bank"], keep="first")
print(f"Rows after removing duplicates: {len(df)}")

def clean_and_process_data(df):
    # 2. Handle missing data
    # Drop rows where 'review' or 'rating' is missing
    df = df.dropna(subset=["review", "rating"])
    print(f"Rows after dropping missing reviews/ratings: {len(df)}")
    # Fill missing 'date' with a placeholder
    df["date"] = df["date"].fillna("1970-01-01")

    # 3. Normalize dates (ensure YYYY-MM-DD)
    def normalize_date(date_str):
        try:
            dt = pd.to_datetime(date_str)
            return dt.strftime("%Y-%m-%d")
        except Exception as e:
            print(f"Invalid date encountered: {date_str}, error: {e}")
            return "1970-01-01"  # Fallback for invalid dates

    df['date'] = df['date'].apply(normalize_date)
    df['date'] = pd.to_datetime(df['date'])

    # 4. Ensure rating is an integer (1-5)
    df["rating"] = df["rating"].astype(int)
    df = df[df["rating"].between(1, 5)]

    # 5. Save clean dataset
    clean_file = os.path.join(os.path.dirname(__file__), '..', 'data', 'clean_reviews.csv')
    df.to_csv(clean_file, index=False, encoding="utf-8")
    print(f"Saved clean dataset to {clean_file}")

    # 6. Verify KPIs
    print(f"Total reviews: {len(df)}")
    print(f"Missing data:\n{df.isna().sum()}")
    print(f"Columns: {list(df.columns)}")

    return df

