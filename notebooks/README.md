# Notebook: ```scrape.ipynb```
## Data Cleaning and Aggregation of Bank App Reviews

This notebook demonstrates the end-to-end process of collecting, cleaning, and aggregating user reviews from Google Play Store for multiple Ethiopian banks' mobile apps. It leverages utility scripts for scraping, loading, and cleaning review data.

---

## Workflow Overview

1. **Setup and Imports**
   - Adds the project parent directory to `sys.path` for easy script imports.
   - Imports utility functions from the `scripts` directory.

2. **Scraping Reviews**
   - Uses the `GooglePlayReviewScraper` (via the `main` function in `scrape_reviews.py`) to fetch reviews for three banks:
     - Commercial Bank of Ethiopia (CBE)
     - Bank of Abyssinia (BOA)
     - Dashen Bank
   - Saves each bank's reviews as a separate CSV file in the `data` directory.

3. **Loading Data**
   - Loads the saved CSV files using the `load_reviews` function from `load_csv.py`.
   - Each bank's reviews are loaded into a pandas DataFrame for further processing.

4. **Cleaning Data**
   - Cleans each DataFrame using the `clean_reviews` function from `clean_reviews_data.py`:
     - Drops unnecessary columns.
     - Renames columns for consistency.
     - Adds new columns to indicate the bank and data source.
   - Ensures all DataFrames have a uniform structure.

5. **Combining and Inspecting Data**
   - Concatenates the cleaned DataFrames from all banks into a single DataFrame.
   - Checks for missing values and inspects the combined data.

6. **Saving the Final Dataset**
   - Exports the fully cleaned and combined DataFrame to `all_reviews.csv` in the `data` directory.

---

## Key Scripts Utilized

- **`analysis_util.py`**: Automates scraping of Google Play Store reviews and saves them to CSV.
- **`scrape_bank_review.py`**: Loads CSV files into pandas DataFrames with error handling.
- **`clean_reviews.py`**: Cleans and standardizes review DataFrames.

---

## Example Usage in Notebook

```python
# Scrape reviews for a bank
from scripts.scrape_reviews import main as scrape_reviews
scrape_reviews(app_id="com.combanketh.mobilebanking", bank_name="CBE", total_reviews=400, lang="en", file_name="cbe_reviews.csv")

# Load reviews
from scripts.load_csv import load_reviews
cbe_reviews = load_reviews("cbe_reviews.csv")

# Clean reviews
from scripts.clean_reviews_data import clean_reviews
cbe_reviews_cleaned = clean_reviews(df=cbe_reviews, drop_columns=[...], rename_columns={...}, new_columns={...})
```

---

## Notes

- The notebook is modular and can be extended to include more banks or additional cleaning steps.
- All intermediate and final datasets are saved in the `data` directory for reproducibility.
- Logging is enabled in all scripts for transparency and debugging.

---
