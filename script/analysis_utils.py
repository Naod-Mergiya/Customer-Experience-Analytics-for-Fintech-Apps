import pandas as pd
import spacy
from transformers import pipeline
from collections import defaultdict

# Load spaCy model
nlp = spacy.load("en_core_web_sm")

# Load DistilBERT sentiment analysis pipeline
sentiment_analyzer = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")

def preprocess_text(text):
    doc = nlp(text.lower())
    tokens = [token.lemma_ for token in doc if not token.is_stop and not token.is_punct and token.is_alpha]
    return tokens

def get_sentiment(review):
    try:
        result = sentiment_analyzer(review)[0]
        label = result["label"].lower()  # POSITIVE or NEGATIVE
        score = result["score"]
        return label, score
    except Exception as e:
        print(f"Error processing review: {e}")
        return "neutral", 0.0  # Fallback for long reviews

def extract_keywords(text):
    doc = nlp(text)
    keywords = [chunk.text.lower() for chunk in doc.noun_chunks if len(chunk.text.split()) <= 3]
    return keywords

def assign_themes(keywords, bank):
    themes = {
        "Account Access Issues": ["login", "authentication", "password", "access", "sign in"],
        "Transaction Performance": ["transfer", "payment", "slow", "failed", "transaction"],
        "User Interface & Experience": ["ui", "interface", "crash", "app design", "user experience"],
        "Customer Support": ["support", "service", "help", "response", "customer"],
        "Feature Requests": ["feature", "update", "fingerprint", "option", "request"]
    }
    assigned_themes = []
    for theme, theme_keywords in themes.items():
        if any(any(kw in keyword for kw in theme_keywords) for keyword in keywords):
            assigned_themes.append(theme)
    return assigned_themes if assigned_themes else ["Other"]

def process_reviews(df):
    print("Processing reviews...")
    df["tokens"] = df["review"].apply(preprocess_text)
    df["keywords"] = df["review"].apply(extract_keywords)
    df[["sentiment_label", "sentiment_score"]] = df["review"].apply(lambda x: pd.Series(get_sentiment(x)))
    df["themes"] = df.apply(lambda row: assign_themes(row["keywords"], row["bank"]), axis=1)
    return df

def aggregate_sentiment(df):
    sentiment_agg = df.groupby(["bank", "rating"]).agg({
        "sentiment_score": "mean",
        "review_id": "count"
    }).rename(columns={"review_id": "count"}).reset_index()
    print("Sentiment aggregation:\n", sentiment_agg)
    return sentiment_agg

def aggregate_themes(df):
    theme_counts = defaultdict(lambda: defaultdict(int))
    for _, row in df.iterrows():
        bank = row["bank"]
        for theme in row["themes"]:
            theme_counts[bank][theme] += 1
    return theme_counts

def save_results(df, sentiment_agg, output_file="analysis_results.csv", sentiment_file="sentiment_aggregation.csv"):
    df[["review_id", "review", "sentiment_label", "sentiment_score", "themes", "bank"]].to_csv(output_file, index=False, encoding="utf-8")
    print(f"Saved analysis results to {output_file}")
    sentiment_agg.to_csv(sentiment_file, index=False, encoding="utf-8")
    print(f"Saved sentiment aggregation to {sentiment_file}")