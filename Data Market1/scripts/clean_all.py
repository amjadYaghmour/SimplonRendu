#!/usr/bin/env python3
"""
Clean All Datasets

This script runs both cleaning operations in sequence:
1. Clean marketing qualified leads dataset
2. Clean closed deals dataset

Usage: python scripts/clean_all.py
"""

import pandas as pd
import sys
import shutil
from pathlib import Path


def clean_leads_data(input_file, output_file):
    """
    Clean the marketing qualified leads dataset according to minimal cleaning rules.
    
    Args:
        input_file (str): Path to input CSV file
        output_file (str): Path to output cleaned CSV file
    """
    try:
        # Load CSV with explicit encoding and dtype inference off
        print(f"Loading leads data from: {input_file}")
        df = pd.read_csv(input_file, encoding='utf-8', dtype=str)
        
        print(f"Original shape: {df.shape}")
        
        # Lowercase column names
        df.columns = df.columns.str.lower()
        
        # Trim whitespace on string columns
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            df[col] = df[col].astype(str).str.strip()
        
        # Standardize dates to ISO YYYY-MM-DD format
        if 'first_contact_date' in df.columns:
            df['first_contact_date'] = pd.to_datetime(df['first_contact_date'], errors='coerce')
            df['first_contact_date'] = df['first_contact_date'].dt.strftime('%Y-%m-%d')
        
        # Handle missing values: drop rows where key columns are missing
        key_columns = ['mql_id']
        for col in key_columns:
            if col in df.columns:
                initial_count = len(df)
                df = df.dropna(subset=[col])
                dropped_count = initial_count - len(df)
                if dropped_count > 0:
                    print(f"Dropped {dropped_count} rows with missing {col}")
        
        # Deduplicate rows (full-row duplicates)
        initial_count = len(df)
        df = df.drop_duplicates()
        duplicate_count = initial_count - len(df)
        if duplicate_count > 0:
            print(f"Removed {duplicate_count} duplicate rows")
        
        # Basic schema check: print column list & non-null counts
        print("\nLeads - Column list and non-null counts:")
        print("=" * 50)
        for col in df.columns:
            non_null_count = df[col].notna().sum()
            print(f"{col}: {non_null_count}/{len(df)} ({non_null_count/len(df)*100:.1f}%)")
        
        print(f"\nLeads - Final shape: {df.shape}")
        
        # Create output directory if it doesn't exist
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save cleaned data
        df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"Cleaned leads data saved to: {output_file}")
        
    except Exception as e:
        print(f"Error processing leads data: {str(e)}", file=sys.stderr)
        raise


def clean_closed_deals_data(input_file, output_file):
    """
    Clean the closed deals dataset according to minimal cleaning rules.
    
    Args:
        input_file (str): Path to input CSV file
        output_file (str): Path to output cleaned CSV file
    """
    try:
        # Load CSV with explicit encoding and dtype inference off
        print(f"\nLoading closed deals data from: {input_file}")
        df = pd.read_csv(input_file, encoding='utf-8', dtype=str)
        
        print(f"Original shape: {df.shape}")
        
        # Lowercase column names
        df.columns = df.columns.str.lower()
        
        # Trim whitespace on string columns
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            df[col] = df[col].astype(str).str.strip()
        
        # Standardize dates to ISO YYYY-MM-DD format
        if 'won_date' in df.columns:
            df['won_date'] = pd.to_datetime(df['won_date'], errors='coerce')
            df['won_date'] = df['won_date'].dt.strftime('%Y-%m-%d')
        
        # Handle missing values: drop rows where key columns are missing
        key_columns = ['mql_id']
        for col in key_columns:
            if col in df.columns:
                initial_count = len(df)
                df = df.dropna(subset=[col])
                dropped_count = initial_count - len(df)
                if dropped_count > 0:
                    print(f"Dropped {dropped_count} rows with missing {col}")
        
        # Deduplicate rows (full-row duplicates)
        initial_count = len(df)
        df = df.drop_duplicates()
        duplicate_count = initial_count - len(df)
        if duplicate_count > 0:
            print(f"Removed {duplicate_count} duplicate rows")
        
        # Basic schema check: print column list & non-null counts
        print("\nClosed Deals - Column list and non-null counts:")
        print("=" * 50)
        for col in df.columns:
            non_null_count = df[col].notna().sum()
            print(f"{col}: {non_null_count}/{len(df)} ({non_null_count/len(df)*100:.1f}%)")
        
        print(f"\nClosed Deals - Final shape: {df.shape}")
        
        # Create output directory if it doesn't exist
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save cleaned data
        df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"Cleaned closed deals data saved to: {output_file}")
        
    except Exception as e:
        print(f"Error processing closed deals data: {str(e)}", file=sys.stderr)
        raise


def main():
    """
    Run both cleaning operations in sequence.
    """
    print("=" * 60)
    print("STARTING DATA CLEANING PROCESS")
    print("=" * 60)
    
    # Default input and output paths
    leads_input = "data/olist_marketing_qualified_leads_dataset.csv"
    leads_output = "data/clean/leads_clean.csv"
    deals_input = "data/olist_closed_deals_dataset.csv"
    deals_output = "data/clean/closed_deals_clean.csv"
    
    # Source files in original Data directory
    leads_source = "Data/olist_marketing_qualified_leads_dataset.csv"
    deals_source = "Data/olist_closed_deals_dataset.csv"
    
    # Create data directory if it doesn't exist
    Path("data").mkdir(exist_ok=True)
    
    # Copy files from source if they don't exist in data directory
    if not Path(leads_input).exists():
        if Path(leads_source).exists():
            print(f"Copying {leads_source} to {leads_input}")
            shutil.copy2(leads_source, leads_input)
        else:
            print(f"Error: Source file {leads_source} does not exist", file=sys.stderr)
            sys.exit(1)
    
    if not Path(deals_input).exists():
        if Path(deals_source).exists():
            print(f"Copying {deals_source} to {deals_input}")
            shutil.copy2(deals_source, deals_input)
        else:
            print(f"Error: Source file {deals_source} does not exist", file=sys.stderr)
            sys.exit(1)
    
    try:
        # Clean marketing qualified leads
        clean_leads_data(leads_input, leads_output)
        
        # Clean closed deals
        clean_closed_deals_data(deals_input, deals_output)
        
        print("\n" + "=" * 60)
        print("DATA CLEANING COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print(f"✅ Leads cleaned: {leads_output}")
        print(f"✅ Closed deals cleaned: {deals_output}")
        
    except Exception as e:
        print(f"\nError during cleaning process: {str(e)}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
