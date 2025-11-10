-- ============================================
-- Data Market 2 - Star Schema Creation
-- ============================================
-- Creates a star schema with:
-- - 1 Fact Table: fact_closed_deals
-- - 4 Dimension Tables: dim_lead, dim_seller, dim_sdr, dim_sr, dim_date
-- ============================================

-- Drop tables if they exist
DROP TABLE IF EXISTS fact_closed_deals CASCADE;
DROP TABLE IF EXISTS dim_lead CASCADE;
DROP TABLE IF EXISTS dim_seller CASCADE;
DROP TABLE IF EXISTS dim_sdr CASCADE;
DROP TABLE IF EXISTS dim_sr CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;

-- ============================================
-- DIMENSION: dim_lead
-- ============================================
CREATE TABLE dim_lead (
    mql_id VARCHAR(255) PRIMARY KEY,
    first_contact_date DATE,
    landing_page_id VARCHAR(255),
    origin VARCHAR(100)
);

CREATE INDEX idx_dim_lead_origin ON dim_lead(origin);
CREATE INDEX idx_dim_lead_date ON dim_lead(first_contact_date);

COMMENT ON TABLE dim_lead IS 'Marketing Qualified Leads dimension';
COMMENT ON COLUMN dim_lead.mql_id IS 'Unique lead identifier';
COMMENT ON COLUMN dim_lead.origin IS 'Lead source: social, paid_search, organic_search, email';

-- ============================================
-- DIMENSION: dim_seller
-- ============================================
CREATE TABLE dim_seller (
    seller_id VARCHAR(255) PRIMARY KEY,
    seller_name VARCHAR(255),
    region VARCHAR(100),
    city VARCHAR(100),
    business_segment VARCHAR(100)
);

CREATE INDEX idx_dim_seller_region ON dim_seller(region);
CREATE INDEX idx_dim_seller_segment ON dim_seller(business_segment);

COMMENT ON TABLE dim_seller IS 'Seller dimension with business information';

-- ============================================
-- DIMENSION: dim_sdr
-- ============================================
CREATE TABLE dim_sdr (
    sdr_id VARCHAR(255) PRIMARY KEY,
    sdr_name VARCHAR(255),
    sdr_team VARCHAR(100),
    sdr_experience VARCHAR(50)
);

CREATE INDEX idx_dim_sdr_team ON dim_sdr(sdr_team);

COMMENT ON TABLE dim_sdr IS 'Sales Development Representative dimension';

-- ============================================
-- DIMENSION: dim_sr
-- ============================================
CREATE TABLE dim_sr (
    sr_id VARCHAR(255) PRIMARY KEY,
    sr_name VARCHAR(255),
    sr_team VARCHAR(100),
    sr_experience VARCHAR(50)
);

CREATE INDEX idx_dim_sr_team ON dim_sr(sr_team);

COMMENT ON TABLE dim_sr IS 'Sales Representative dimension';

-- ============================================
-- DIMENSION: dim_date
-- ============================================
CREATE TABLE dim_date (
    date_id VARCHAR(8) PRIMARY KEY,
    full_date DATE NOT NULL,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    month_name VARCHAR(20),
    day INTEGER,
    day_of_week VARCHAR(20),
    week_of_year INTEGER
);

CREATE INDEX idx_dim_date_full_date ON dim_date(full_date);
CREATE INDEX idx_dim_date_year_month ON dim_date(year, month);

COMMENT ON TABLE dim_date IS 'Date dimension for time-based analysis';

-- ============================================
-- FACT TABLE: fact_closed_deals
-- ============================================
CREATE TABLE fact_closed_deals (
    deal_id SERIAL PRIMARY KEY,
    mql_id VARCHAR(255) NOT NULL,
    seller_id VARCHAR(255) NOT NULL,
    sdr_id VARCHAR(255) NOT NULL,
    sr_id VARCHAR(255) NOT NULL,
    contact_date_id VARCHAR(8),
    won_date_id VARCHAR(8),
    
    -- Measures/Metrics
    lead_type VARCHAR(100),
    lead_behaviour_profile VARCHAR(100),
    business_segment VARCHAR(100),
    business_type VARCHAR(100),
    declared_product_catalog_size NUMERIC(15, 2),
    declared_monthly_revenue NUMERIC(15, 2),
    
    -- Foreign Keys
    CONSTRAINT fk_fact_mql FOREIGN KEY (mql_id) 
        REFERENCES dim_lead(mql_id),
    CONSTRAINT fk_fact_seller FOREIGN KEY (seller_id) 
        REFERENCES dim_seller(seller_id),
    CONSTRAINT fk_fact_sdr FOREIGN KEY (sdr_id) 
        REFERENCES dim_sdr(sdr_id),
    CONSTRAINT fk_fact_sr FOREIGN KEY (sr_id) 
        REFERENCES dim_sr(sr_id),
    CONSTRAINT fk_fact_contact_date FOREIGN KEY (contact_date_id) 
        REFERENCES dim_date(date_id),
    CONSTRAINT fk_fact_won_date FOREIGN KEY (won_date_id) 
        REFERENCES dim_date(date_id)
);

-- Indexes for fact table
CREATE INDEX idx_fact_mql ON fact_closed_deals(mql_id);
CREATE INDEX idx_fact_seller ON fact_closed_deals(seller_id);
CREATE INDEX idx_fact_sdr ON fact_closed_deals(sdr_id);
CREATE INDEX idx_fact_sr ON fact_closed_deals(sr_id);
CREATE INDEX idx_fact_contact_date ON fact_closed_deals(contact_date_id);
CREATE INDEX idx_fact_won_date ON fact_closed_deals(won_date_id);
CREATE INDEX idx_fact_business_segment ON fact_closed_deals(business_segment);

COMMENT ON TABLE fact_closed_deals IS 'Fact table containing all closed deal transactions';
COMMENT ON COLUMN fact_closed_deals.declared_monthly_revenue IS 'Revenue metric for analysis';

