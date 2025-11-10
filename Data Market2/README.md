# Data Market 2 - Star Schema Data Warehouse

**Student:** Angelo Yaghmour  
**Project:** Data Warehouse Implementation using Star Schema  
**Database:** PostgreSQL

---

## 📋 Project Overview

This project implements a **star schema data warehouse** for analyzing marketing leads and closed deals. The warehouse consists of:

- **1 Fact Table:** `fact_closed_deals` (central transaction table)
- **4 Dimension Tables:** `dim_lead`, `dim_seller`, `dim_sdr`, `dim_sr`, `dim_date`

## 📂 Project Structure

```
Data Market2/
├── data/
│   └── clean/
│       ├── leads_clean.csv              # Source: leads data
│       ├── closed_deals_clean.csv       # Source: deals data
│       ├── dim_lead.csv                 # Generated dimension
│       ├── dim_seller.csv               # Generated dimension
│       ├── dim_sdr.csv                  # Generated dimension
│       ├── dim_sr.csv                   # Generated dimension
│       └── dim_date.csv                 # Generated dimension
├── scripts/
│   ├── generate_dimensions.py           # Generate all dimension CSVs
│   ├── create_tables.sql                # SQL DDL for star schema
│   └── load_to_db.py                    # ETL script to load data
├── models/
│   ├── ERD.drawio                       # Entity-Relationship Diagram
│   └── data_dictionary.xlsx             # Data dictionary
├── E3_Schema_Technique_Angelo_Yaghmour.pdf
├── E4_Rapport_Technique_Angelo_Yaghmour.pdf
└── README.md
```

## 🚀 Quick Start

### Prerequisites

- PostgreSQL 12+
- Python 3.8+
- pip

### Installation Steps

**1. Install Python dependencies:**

```bash
pip install pandas psycopg2-binary
```

**2. Create PostgreSQL database:**

```bash
# Connect to PostgreSQL
psql -U postgres

# Create database
CREATE DATABASE datamarket2;
\q
```

**3. Set environment variables (optional):**

```bash
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=datamarket2
export DB_USER=postgres
export DB_PASSWORD=your_password
```

**4. Generate dimension CSV files:**

```bash
python scripts/generate_dimensions.py
```

This creates:
- `dim_lead.csv`
- `dim_seller.csv`
- `dim_sdr.csv`
- `dim_sr.csv`
- `dim_date.csv`

**5. Create database schema:**

```bash
psql -d datamarket2 -f scripts/create_tables.sql
```

**6. Load data into PostgreSQL:**

```bash
python scripts/load_to_db.py
```

### Verify Installation

```bash
psql -d datamarket2
```

```sql
-- Check row counts
SELECT 'dim_lead' as table_name, COUNT(*) FROM dim_lead
UNION ALL
SELECT 'dim_seller', COUNT(*) FROM dim_seller
UNION ALL
SELECT 'dim_sdr', COUNT(*) FROM dim_sdr
UNION ALL
SELECT 'dim_sr', COUNT(*) FROM dim_sr
UNION ALL
SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL
SELECT 'fact_closed_deals', COUNT(*) FROM fact_closed_deals;
```

## 📊 Star Schema Structure

```
           ┌───────────┐
           │ dim_date  │
           └─────┬─────┘
                 │
   ┌──────────┐ │ ┌───────────┐
   │ dim_lead ├─┼─┤ dim_seller│
   └─────┬────┘ │ └─────┬─────┘
         │      │       │
         │  ┌───▼───────▼───┐
         └──┤ fact_closed   │
            │    _deals     │
         ┌──┤               ├──┐
         │  └───────────────┘  │
         │                     │
    ┌────▼────┐          ┌────▼────┐
    │ dim_sdr │          │ dim_sr  │
    └─────────┘          └─────────┘
```

## 📈 Example Analytical Queries

### 1. Closed Deals by Business Segment

```sql
SELECT 
    s.business_segment,
    COUNT(*) as total_deals,
    ROUND(AVG(f.declared_monthly_revenue), 2) as avg_revenue
FROM fact_closed_deals f
JOIN dim_seller s ON f.seller_id = s.seller_id
GROUP BY s.business_segment
ORDER BY total_deals DESC;
```

### 2. Conversion Rate by Lead Origin

```sql
SELECT 
    l.origin,
    COUNT(DISTINCT l.mql_id) as total_leads,
    COUNT(DISTINCT f.mql_id) as closed_deals,
    ROUND(100.0 * COUNT(DISTINCT f.mql_id) / COUNT(DISTINCT l.mql_id), 2) as conversion_rate
FROM dim_lead l
LEFT JOIN fact_closed_deals f ON l.mql_id = f.mql_id
GROUP BY l.origin
ORDER BY conversion_rate DESC;
```

### 3. Sales Performance by Team

```sql
SELECT 
    sr.sr_team,
    sr.sr_experience,
    COUNT(*) as deals_closed,
    SUM(f.declared_monthly_revenue) as total_revenue
FROM fact_closed_deals f
JOIN dim_sr sr ON f.sr_id = sr.sr_id
GROUP BY sr.sr_team, sr.sr_experience
ORDER BY total_revenue DESC;
```

### 4. Monthly Trend Analysis

```sql
SELECT 
    d.year,
    d.month,
    d.month_name,
    COUNT(*) as deals_count,
    SUM(f.declared_monthly_revenue) as revenue
FROM fact_closed_deals f
JOIN dim_date d ON f.won_date_id = d.date_id
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;
```

## 🔧 Maintenance

### Refresh Data

To update the warehouse with new data:

```bash
# 1. Update source CSVs (leads_clean.csv, closed_deals_clean.csv)
# 2. Regenerate dimensions
python scripts/generate_dimensions.py

# 3. Truncate and reload (or use UPSERT logic)
psql -d datamarket2 -c "TRUNCATE fact_closed_deals CASCADE;"
python scripts/load_to_db.py
```

### Backup Database

```bash
pg_dump -d datamarket2 > backup_$(date +%Y%m%d).sql
```

## 📚 Documentation

- **E3_Schema_Technique_Angelo_Yaghmour.pdf:** Technical schema design, ERD, data dictionary
- **E4_Rapport_Technique_Angelo_Yaghmour.pdf:** ETL implementation and technical documentation

## 🏗️ Technical Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Database | PostgreSQL | Data storage |
| ETL Language | Python 3 | Data processing |
| Data Library | Pandas | CSV manipulation |
| DB Connector | psycopg2 | PostgreSQL connectivity |

## ✅ Key Features

- ⭐ **Star schema** design for optimal analytics
- 🔗 **Referential integrity** via foreign keys
- 📊 **Date dimension** for time-based analysis
- 🎲 **Synthetic attributes** for enriched dimensions
- 🔄 **Reproducible ETL** pipeline
- 📝 **Comprehensive documentation**

## 📞 Support

For questions about this project, contact **Angelo Yaghmour**.

---

**License:** Simplon Data Market Training Program

