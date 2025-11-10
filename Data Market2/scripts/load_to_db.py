#!/usr/bin/env python3
"""
Load Star Schema to PostgreSQL

Loads all dimension and fact tables into PostgreSQL database:
1. Load dimensions: dim_lead, dim_seller, dim_sdr, dim_sr, dim_date
2. Load fact table: fact_closed_deals

Usage: python scripts/load_to_db.py
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import sys
import os
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_db_connection():
    """Create database connection"""
    host = os.getenv('DB_HOST', 'localhost')
    port = os.getenv('DB_PORT', '5432')
    database = os.getenv('DB_NAME', 'datamarket2')
    user = os.getenv('DB_USER', 'postgres')
    password = os.getenv('DB_PASSWORD', 'postgres')
    
    try:
        conn = psycopg2.connect(
            host=host, port=port, database=database,
            user=user, password=password
        )
        logger.info(f"Connected to database: {database}")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Connection error: {e}")
        raise


def load_dim_lead(conn, csv_path):
    """Load dim_lead"""
    logger.info(f"Loading dim_lead from {csv_path}")
    df = pd.read_csv(csv_path)
    df = df.where(pd.notnull(df), None)
    
    cursor = conn.cursor()
    try:
        values = [(row['mql_id'], row['first_contact_date'], 
                   row['landing_page_id'], row['origin'])
                  for _, row in df.iterrows()]
        
        execute_values(cursor, """
            INSERT INTO dim_lead (mql_id, first_contact_date, landing_page_id, origin)
            VALUES %s
        """, values)
        conn.commit()
        logger.info(f"  ✓ Loaded {len(values)} leads")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading dim_lead: {e}")
        raise
    finally:
        cursor.close()


def load_dim_seller(conn, csv_path):
    """Load dim_seller"""
    logger.info(f"Loading dim_seller from {csv_path}")
    df = pd.read_csv(csv_path)
    df = df.where(pd.notnull(df), None)
    
    cursor = conn.cursor()
    try:
        values = [(row['seller_id'], row['seller_name'], row['region'],
                   row['city'], row['business_segment'])
                  for _, row in df.iterrows()]
        
        execute_values(cursor, """
            INSERT INTO dim_seller (seller_id, seller_name, region, city, business_segment)
            VALUES %s
        """, values)
        conn.commit()
        logger.info(f"  ✓ Loaded {len(values)} sellers")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading dim_seller: {e}")
        raise
    finally:
        cursor.close()


def load_dim_sdr(conn, csv_path):
    """Load dim_sdr"""
    logger.info(f"Loading dim_sdr from {csv_path}")
    df = pd.read_csv(csv_path)
    df = df.where(pd.notnull(df), None)
    
    cursor = conn.cursor()
    try:
        values = [(row['sdr_id'], row['sdr_name'], row['sdr_team'], row['sdr_experience'])
                  for _, row in df.iterrows()]
        
        execute_values(cursor, """
            INSERT INTO dim_sdr (sdr_id, sdr_name, sdr_team, sdr_experience)
            VALUES %s
        """, values)
        conn.commit()
        logger.info(f"  ✓ Loaded {len(values)} SDRs")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading dim_sdr: {e}")
        raise
    finally:
        cursor.close()


def load_dim_sr(conn, csv_path):
    """Load dim_sr"""
    logger.info(f"Loading dim_sr from {csv_path}")
    df = pd.read_csv(csv_path)
    df = df.where(pd.notnull(df), None)
    
    cursor = conn.cursor()
    try:
        values = [(row['sr_id'], row['sr_name'], row['sr_team'], row['sr_experience'])
                  for _, row in df.iterrows()]
        
        execute_values(cursor, """
            INSERT INTO dim_sr (sr_id, sr_name, sr_team, sr_experience)
            VALUES %s
        """, values)
        conn.commit()
        logger.info(f"  ✓ Loaded {len(values)} SRs")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading dim_sr: {e}")
        raise
    finally:
        cursor.close()


def load_dim_date(conn, csv_path):
    """Load dim_date"""
    logger.info(f"Loading dim_date from {csv_path}")
    df = pd.read_csv(csv_path)
    df = df.where(pd.notnull(df), None)
    
    cursor = conn.cursor()
    try:
        values = [(row['date_id'], row['full_date'], row['year'], row['quarter'],
                   row['month'], row['month_name'], row['day'], 
                   row['day_of_week'], row['week_of_year'])
                  for _, row in df.iterrows()]
        
        execute_values(cursor, """
            INSERT INTO dim_date (date_id, full_date, year, quarter, month, 
                                 month_name, day, day_of_week, week_of_year)
            VALUES %s
        """, values)
        conn.commit()
        logger.info(f"  ✓ Loaded {len(values)} dates")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading dim_date: {e}")
        raise
    finally:
        cursor.close()


def load_fact_closed_deals(conn, csv_path, leads_csv):
    """Load fact_closed_deals"""
    logger.info(f"Loading fact_closed_deals from {csv_path}")
    
    deals_df = pd.read_csv(csv_path)
    leads_df = pd.read_csv(leads_csv)
    
    # Join to get contact dates
    merged = deals_df.merge(leads_df[['mql_id', 'first_contact_date']], 
                           on='mql_id', how='left')
    
    # Convert dates to date_id format
    merged['contact_date_id'] = pd.to_datetime(merged['first_contact_date'], 
                                               errors='coerce').dt.strftime('%Y%m%d')
    merged['won_date_id'] = pd.to_datetime(merged['won_date'], 
                                           errors='coerce').dt.strftime('%Y%m%d')
    
    merged = merged.where(pd.notnull(merged), None)
    
    cursor = conn.cursor()
    try:
        values = [
            (row['mql_id'], row['seller_id'], row['sdr_id'], row['sr_id'],
             row['contact_date_id'], row['won_date_id'],
             row.get('lead_type'), row.get('lead_behaviour_profile'),
             row.get('business_segment'), row.get('business_type'),
             row.get('declared_product_catalog_size'),
             row.get('declared_monthly_revenue'))
            for _, row in merged.iterrows()
        ]
        
        execute_values(cursor, """
            INSERT INTO fact_closed_deals (
                mql_id, seller_id, sdr_id, sr_id,
                contact_date_id, won_date_id,
                lead_type, lead_behaviour_profile, business_segment, business_type,
                declared_product_catalog_size, declared_monthly_revenue
            ) VALUES %s
        """, values)
        conn.commit()
        logger.info(f"  ✓ Loaded {len(values)} closed deals")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading fact_closed_deals: {e}")
        raise
    finally:
        cursor.close()


def main():
    logger.info("=" * 60)
    logger.info("LOADING STAR SCHEMA TO POSTGRESQL")
    logger.info("=" * 60)
    
    project_root = Path(__file__).parent.parent
    clean_dir = project_root / "data" / "clean"
    
    try:
        conn = get_db_connection()
        
        # Load dimensions first (order matters due to foreign keys)
        logger.info("\nLoading dimension tables...")
        load_dim_lead(conn, clean_dir / "dim_lead.csv")
        load_dim_seller(conn, clean_dir / "dim_seller.csv")
        load_dim_sdr(conn, clean_dir / "dim_sdr.csv")
        load_dim_sr(conn, clean_dir / "dim_sr.csv")
        load_dim_date(conn, clean_dir / "dim_date.csv")
        
        # Load fact table
        logger.info("\nLoading fact table...")
        load_fact_closed_deals(conn, 
                              clean_dir / "closed_deals_clean.csv",
                              clean_dir / "dim_lead.csv")
        
        logger.info("\n" + "=" * 60)
        logger.info("STAR SCHEMA LOADED SUCCESSFULLY!")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Failed to load star schema: {e}")
        sys.exit(1)
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()

