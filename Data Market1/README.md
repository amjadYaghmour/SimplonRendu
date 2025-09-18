# Data Cleaning Scripts - Olist Dataset

This project provides simple Python scripts to clean and standardize the Olist marketing and sales datasets for internal (non-technical) users.

## What the Scripts Do

The scripts apply minimal cleaning rules to two CSV datasets:

### Inputs
- `data/olist_marketing_qualified_leads_dataset.csv` - Marketing qualified leads data
- `data/olist_closed_deals_dataset.csv` - Closed deals/sales data

### Outputs
- `data/clean/leads_clean.csv` - Cleaned marketing qualified leads
- `data/clean/closed_deals_clean.csv` - Cleaned closed deals data

## How to Run

### Prerequisites
- Python 3.10 or higher
- Virtual environment (recommended)

### Setup
```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Run Cleaning Scripts

**Option 1: Clean all datasets with one command (Recommended)**
```bash
python scripts/clean_all.py
```

**Option 2: Clean datasets individually**
```bash
# Clean marketing qualified leads
python scripts/clean_leads.py

# Clean closed deals
python scripts/clean_closed_deals.py
```

## Dependencies

- **Python**: 3.10+
- **pandas**: >=1.5.0 (mandatory library)

## Cleaning Rules & Limitations

### Applied Cleaning Rules
1. **Load CSV with explicit encoding** and dtype inference off (avoid silent type coercions)
2. **Trim whitespace** on string columns
3. **Standardize dates** to ISO YYYY-MM-DD format
4. **Lowercase column names** (no renaming beyond case; keep original names otherwise)
5. **Deduplicate rows** (full-row duplicates)
6. **Handle missing values**:
   - For ID/key columns (`mql_id`): drop rows where key is missing
   - For non-key columns: leave as null (no imputation at this stage)
7. **Basic schema check**: print column list & non-null counts to console

### Limitations
- **No joins/merges**: Focus is on clean, source-level datasets only
- **No data imputation**: Missing values in non-key columns are preserved as null
- **No complex transformations**: Only basic standardization and cleaning
- **File-level cleaning only**: No cross-file validation or relationships

### Output Format
- Clean CSV files with UTF-8 encoding
- Standardized date format (YYYY-MM-DD)
- Lowercase column names
- No duplicate rows
- Missing key values removed

## Project Structure
```
/project
  /scripts
    clean_all.py          # One-click cleaning (recommended)
    clean_leads.py        # Individual leads cleaning
    clean_closed_deals.py # Individual deals cleaning
  /data
    olist_marketing_qualified_leads_dataset.csv
    olist_closed_deals_dataset.csv
    /clean
      leads_clean.csv
      closed_deals_clean.csv
  README.md
  requirements.txt
```

## Notes
- Scripts are deterministic: same inputs produce same outputs
- All operations are logged to console for transparency
- Error handling included for common issues (missing files, encoding problems)
- **Automatic setup**: `clean_all.py` automatically copies files from `Data/` to `data/` if needed
- Project is ready for Git versioning (not required for this step)
