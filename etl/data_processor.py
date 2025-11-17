#!/usr/bin/env python3
"""
Data Processor for Retail Dataset
Converts Excel to CSV and performs initial data analysis
"""

import pandas as pd
import numpy as np
import json
from pathlib import Path
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RetailDataProcessor:
    def __init__(self, input_file: str, output_dir: str):
        self.input_file = Path(input_file)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def convert_excel_to_csv(self) -> str:
        """Convert Excel file to CSV with data profiling"""
        logger.info(f"Processing Excel file: {self.input_file}")
        
        try:
            # Read Excel file in chunks to handle large files
            df = pd.read_excel(self.input_file, engine='openpyxl')
            logger.info(f"Successfully loaded {len(df):,} rows")
            
            # Basic info about the dataset
            logger.info(f"Columns: {list(df.columns)}")
            logger.info(f"Shape: {df.shape}")
            
            # Save as CSV
            csv_file = self.output_dir / "online_retail.csv"
            df.to_csv(csv_file, index=False)
            logger.info(f"Saved CSV to: {csv_file}")
            
            # Generate data profile
            self.generate_data_profile(df)
            
            return str(csv_file)
            
        except Exception as e:
            logger.error(f"Error processing Excel file: {e}")
            raise
    
    def generate_data_profile(self, df: pd.DataFrame):
        """Generate comprehensive data profile"""
        logger.info("Generating data profile...")
        
        profile = {
            "file_info": {
                "source_file": str(self.input_file),
                "processed_at": datetime.now().isoformat(),
                "total_rows": len(df),
                "total_columns": len(df.columns)
            },
            "columns": {},
            "data_quality": {},
            "sample_data": df.head(5).astype(str).to_dict('records')
        }
        
        # Analyze each column
        for col in df.columns:
            col_data = df[col]
            profile["columns"][col] = {
                "data_type": str(col_data.dtype),
                "non_null_count": int(col_data.count()),
                "null_count": int(col_data.isnull().sum()),
                "null_percentage": round(col_data.isnull().sum() / len(df) * 100, 2),
                "unique_values": int(col_data.nunique()),
                "memory_usage": int(col_data.memory_usage(deep=True))
            }
            
            # Add statistics for numeric columns
            if pd.api.types.is_numeric_dtype(col_data):
                profile["columns"][col].update({
                    "min": float(col_data.min()) if not col_data.empty else None,
                    "max": float(col_data.max()) if not col_data.empty else None,
                    "mean": float(col_data.mean()) if not col_data.empty else None,
                    "std": float(col_data.std()) if not col_data.empty else None
                })
            
            # Add statistics for datetime columns
            elif pd.api.types.is_datetime64_any_dtype(col_data):
                profile["columns"][col].update({
                    "min": col_data.min().isoformat() if not col_data.empty else None,
                    "max": col_data.max().isoformat() if not col_data.empty else None
                })
            
            # Add sample values for categorical columns
            if pd.api.types.is_object_dtype(col_data):
                top_values = col_data.value_counts().head(5)
                profile["columns"][col]["top_values"] = top_values.to_dict()
        
        # Data quality checks
        profile["data_quality"] = {
            "total_duplicates": int(df.duplicated().sum()),
            "completely_null_rows": int(df.isnull().all(axis=1).sum()),
            "rows_with_any_null": int(df.isnull().any(axis=1).sum())
        }
        
        # Business logic checks for retail data
        if 'Quantity' in df.columns:
            profile["data_quality"]["negative_quantities"] = int((df['Quantity'] < 0).sum())
            profile["data_quality"]["zero_quantities"] = int((df['Quantity'] == 0).sum())
        
        if 'UnitPrice' in df.columns:
            profile["data_quality"]["negative_prices"] = int((df['UnitPrice'] < 0).sum())
            profile["data_quality"]["zero_prices"] = int((df['UnitPrice'] == 0).sum())
        
        # Save profile
        profile_file = self.output_dir / "data_profile.json"
        with open(profile_file, 'w') as f:
            json.dump(profile, f, indent=2)
        
        logger.info(f"Data profile saved to: {profile_file}")
        
        # Print summary
        self.print_summary(profile)
    
    def print_summary(self, profile: dict):
        """Print a nice summary of the data"""
        print("\n" + "="*60)
        print("📊 RETAIL DATASET ANALYSIS SUMMARY")
        print("="*60)
        
        file_info = profile["file_info"]
        print(f"📁 File: {Path(file_info['source_file']).name}")
        print(f"📅 Processed: {file_info['processed_at'][:19]}")
        print(f"📏 Dimensions: {file_info['total_rows']:,} rows × {file_info['total_columns']} columns")
        
        print(f"\n🔍 COLUMN ANALYSIS:")
        for col, info in profile["columns"].items():
            null_pct = info['null_percentage']
            unique_vals = info['unique_values']
            print(f"  • {col:12} | {info['data_type']:10} | {null_pct:5.1f}% null | {unique_vals:,} unique")
        
        print(f"\n⚠️  DATA QUALITY ISSUES:")
        quality = profile["data_quality"]
        print(f"  • Duplicate rows: {quality['total_duplicates']:,}")
        print(f"  • Rows with nulls: {quality['rows_with_any_null']:,}")
        
        if 'negative_quantities' in quality:
            print(f"  • Negative quantities: {quality['negative_quantities']:,}")
        if 'negative_prices' in quality:
            print(f"  • Negative prices: {quality['negative_prices']:,}")
        
        print("="*60)

def main():
    """Main execution function"""
    # File paths
    input_file = "/Users/tuandang/personal/research/AI/datalake-demo/data/raw/online-retail.xlsx"
    output_dir = "/Users/tuandang/personal/research/AI/datalake-demo/data/processed"
    
    # Process the data
    processor = RetailDataProcessor(input_file, output_dir)
    csv_file = processor.convert_excel_to_csv()
    
    print(f"\n✅ Processing complete!")
    print(f"📄 CSV file: {csv_file}")
    print(f"📊 Profile: {output_dir}/data_profile.json")

if __name__ == "__main__":
    main()
