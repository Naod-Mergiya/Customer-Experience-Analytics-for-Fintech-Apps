
# Fintech Review Insights (Week 2 Challenge)

This project analyzes customer feedback from Google Play Store for fintech apps: B1, B2, and B3 banks. The goal is to scrape, preprocess, and analyze reviews to gain insights into customer satisfaction and app performance.
## Overview
The project involves scraping reviews from the Google Play Store, cleaning and normalizing the data, and preparing it for analysis. The final output will be a set of cleaned CSV files containing customer reviews, ratings, dates, and sources (Google play store).

## Requirements
- Python 3.10+
- Find and install required packages using `pip install -r requirements.txt`

## Project Structure
- `scripts/`: Scraper and preprocessing scripts
- `data/`: Cleaned CSV datasets
- `notebooks/`: Exploration notebooks (optional)
- `tests/`: Unit tests for scripts
- `README.md`: Project overview and instructions

## Task 1: Scraping & Preprocessing
- Scrape 1200+ reviews (400 per bank)
- Clean and normalize text, rating, date, source
- Save to CSV files in `data/`
- Use `scripts/scraper.py` for scraping
- Use `scripts/preprocess.py` for preprocessing
- Ensure data is structured with columns: `review`, `rating`, `date`, `bank`, `source`
- Use `data/` directory for cleaned data
- Write unit tests for scraper and preprocessing scripts
- Use `tests/` directory for tests


