#!/usr/bin/env python3
"""
Wakefit Supply Chain Data Upload Script with .env support
Uploads CSV data to PostgreSQL database
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os
import sys
from pathlib import Path
from datetime import datetime

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("Loaded environment variables from .env file")
except ImportError:
    print("python-dotenv not installed. Using system environment variables.")
    print("Install with: pip install python-dotenv")

# Database configuration from environment variables
POSTGRES_CONFIG = {
    'host': os.environ.get('DB_HOST', 'insights-db.postgres.database.azure.com'),
    'database': os.environ.get('DB_NAME', 'wakefit_supply_chain'),
    'user': os.environ.get('DB_USER', 'turintonadmin'),
    'password': os.environ.get('DB_PASSWORD'),
    'port': int(os.environ.get('DB_PORT', 5432))
}

# CSV folder path from environment variable
CSV_FOLDER = os.environ.get('CSV_FOLDER', r"C:/Turinton/universal_data_generatsions/wakefit_data_optimized")

# Tables in dependency order
TABLES = [
    'customers',
    'products', 
    'facilities',
    'suppliers',
    'orders',
    'purchase_orders',
    'production_batches',
    'order_line_items',
    'inventory_movements',
    'logistics_shipments',
    'demand_forecasts',
    'supply_chain_events'
]

class WakefitDataUploader:
    def __init__(self, csv_folder, db_config):
        self.csv_folder = Path(csv_folder)
        self.db_config = db_config
        self.conn = None
        
    def connect_db(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            print("Database connection successful!")
            return True
        except Exception as e:
            print(f"Database connection failed: {e}")
            return False
    
    def close_db(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
    
    def table_exists(self, table_name):
        """Check if table exists in database"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_name = %s
                )
            """, (table_name,))
            exists = cursor.fetchone()[0]
            cursor.close()
            return exists
        except Exception as e:
            print(f"Error checking table existence: {e}")
            return False
    
    def upload_table(self, table_name):
        """Upload single table to database"""
        csv_file = self.csv_folder / f"{table_name}.csv"
        
        if not csv_file.exists():
            print(f"  CSV file not found: {csv_file}")
            return False
        
        if not self.table_exists(table_name):
            print(f"  Table '{table_name}' does not exist in database")
            return False
        
        try:
            # Read CSV file
            df = pd.read_csv(csv_file)
            row_count = len(df)
            
            print(f"  CSV file found with {row_count:,} rows")
            
            # Clear existing data
            cursor = self.conn.cursor()
            cursor.execute(f"TRUNCATE TABLE {table_name} CASCADE")
            print(f"  Cleared existing data from {table_name}")
            
            # Insert data if not empty
            if not df.empty:
                # Convert DataFrame to list of tuples
                columns = df.columns.tolist()
                values = []
                
                for _, row in df.iterrows():
                    # Convert NaN to None for proper NULL handling
                    row_values = []
                    for val in row:
                        if pd.isna(val):
                            row_values.append(None)
                        else:
                            row_values.append(val)
                    values.append(tuple(row_values))
                
                # Create insert query with proper column names
                placeholders = ','.join(['%s'] * len(columns))
                insert_query = f"""
                INSERT INTO {table_name} ({','.join(columns)}) 
                VALUES ({placeholders})
                """
                
                # Insert in batches for better performance
                batch_size = int(os.environ.get('BATCH_SIZE', 1000))
                for i in range(0, len(values), batch_size):
                    batch = values[i:i + batch_size]
                    cursor.executemany(insert_query, batch)
                
                print(f"  Uploading data in batches of {batch_size}...")
            
            # Commit transaction
            self.conn.commit()
            cursor.close()
            
            # Verify upload
            cursor = self.conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            db_count = cursor.fetchone()[0]
            cursor.close()
            
            print(f"  Successfully uploaded {db_count:,} rows to {table_name}")
            return True
            
        except Exception as e:
            print(f"  Error uploading {table_name}: {e}")
            if self.conn:
                self.conn.rollback()
            return False
    
    def upload_all_tables(self):
        """Upload all tables in dependency order"""
        success_count = 0
        failed_tables = []
        total_rows = 0
        
        print(f"Starting upload of {len(TABLES)} tables...")
        print("=" * 60)
        
        for i, table in enumerate(TABLES, 1):
            print(f"[{i}/{len(TABLES)}] Processing table: {table}")
            
            if self.upload_table(table):
                success_count += 1
                # Get row count for summary
                try:
                    cursor = self.conn.cursor()
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    table_rows = cursor.fetchone()[0]
                    cursor.close()
                    total_rows += table_rows
                except:
                    pass
            else:
                failed_tables.append(table)
            
            print()
        
        # Summary
        print("=" * 60)
        print("Upload Summary:")
        print(f"  Successful: {success_count} tables")
        print(f"  Failed: {len(failed_tables)} tables")
        print(f"  Total rows uploaded: {total_rows:,}")
        
        if failed_tables:
            print(f"  Failed tables: {', '.join(failed_tables)}")
        
        return success_count, failed_tables
    
    def show_table_counts(self):
        """Display final row counts for all tables"""
        print("\nFinal table counts:")
        print("-" * 40)
        
        cursor = self.conn.cursor()
        for table in TABLES:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"  {table:<25} {count:>10,} rows")
            except Exception as e:
                print(f"  {table:<25} {'Error':>10}")
        cursor.close()

def main():
    print("Wakefit Supply Chain Data Upload Script")
    print("=" * 60)
    print(f"Database: {POSTGRES_CONFIG['database']}")
    print(f"Host: {POSTGRES_CONFIG['host']}")
    print(f"User: {POSTGRES_CONFIG['user']}")
    print(f"CSV Folder: {CSV_FOLDER}")
    print("=" * 60)
    
    # Check if password is set
    if not POSTGRES_CONFIG['password']:
        print("Error: Database password not set.")
        print("Please set DB_PASSWORD in your .env file or environment variable")
        return 1
    
    # Validate CSV folder
    if not os.path.exists(CSV_FOLDER):
        print(f"Error: CSV folder does not exist: {CSV_FOLDER}")
        return 1
    
    # Create uploader
    uploader = WakefitDataUploader(CSV_FOLDER, POSTGRES_CONFIG)
    
    # Connect to database
    if not uploader.connect_db():
        return 1
    
    try:
        # Upload all tables
        success_count, failed_tables = uploader.upload_all_tables()
        
        # Show final counts if any uploads succeeded
        if success_count > 0:
            uploader.show_table_counts()
        
        print(f"\nUpload completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        return 0 if len(failed_tables) == 0 else 1
        
    finally:
        uploader.close_db()

if __name__ == "__main__":
    sys.exit(main())