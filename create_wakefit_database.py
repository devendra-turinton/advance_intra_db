#!/usr/bin/env python3
"""
Create Wakefit Supply Chain Database and Tables - Updated Version
Matches the final corrected data generator structure
"""

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration - connect to default postgres database first
ADMIN_CONFIG = {
    'host': os.environ.get('DB_HOST', 'insights-db.postgres.database.azure.com'),
    'database': 'postgres',  # Connect to default postgres database
    'user': os.environ.get('DB_USER', 'turintonadmin'),
    'password': os.environ.get('DB_PASSWORD'),
    'port': int(os.environ.get('DB_PORT', 5432))
}

DATABASE_NAME = os.environ.get('DB_NAME', 'wakefit_supply_chain')

def create_database():
    """Create the wakefit_supply_chain database"""
    try:
        # Connect to default postgres database
        conn = psycopg2.connect(**ADMIN_CONFIG)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname='{DATABASE_NAME}'")
        exists = cursor.fetchone()
        
        if exists:
            print(f"Database '{DATABASE_NAME}' already exists")
        else:
            # Create database
            cursor.execute(f'CREATE DATABASE "{DATABASE_NAME}"')
            print(f"Database '{DATABASE_NAME}' created successfully")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"Error creating database: {e}")
        return False

def create_tables():
    """Create all required tables with updated schema"""
    
    # Connect to the new database
    db_config = ADMIN_CONFIG.copy()
    db_config['database'] = DATABASE_NAME
    
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Table creation statements - Updated to match data generator
        tables = {
            'customers': '''
                CREATE TABLE IF NOT EXISTS customers (
                    customer_id VARCHAR(30) PRIMARY KEY,
                    customer_type VARCHAR(20),
                    registration_date DATE,
                    primary_channel VARCHAR(20),
                    delivery_city VARCHAR(50),
                    delivery_state VARCHAR(30),
                    pincode VARCHAR(10),
                    customer_segment VARCHAR(30),
                    delivery_sensitivity_score INTEGER,
                    lifetime_orders INTEGER,
                    lifetime_value DECIMAL(12,2),
                    avg_order_frequency_days INTEGER,
                    preferred_delivery_window VARCHAR(20),
                    last_order_date DATE
                )
            ''',
            
            'products': '''
                CREATE TABLE IF NOT EXISTS products (
                    sku_code VARCHAR(30) PRIMARY KEY,
                    product_name VARCHAR(200),
                    category VARCHAR(30),
                    sub_category VARCHAR(50),
                    size_variant VARCHAR(20),
                    manufacturing_complexity VARCHAR(10),
                    standard_production_time_hours DECIMAL(6,2),
                    is_customizable BOOLEAN,
                    weight_kg DECIMAL(8,2),
                    dimensions_lxwxh_cm VARCHAR(30),
                    is_bulky_item BOOLEAN,
                    raw_materials_list TEXT,
                    minimum_inventory_days INTEGER,
                    maximum_inventory_days INTEGER,
                    supplier_lead_time_days INTEGER,
                    seasonal_demand_factor TEXT,
                    price_inr DECIMAL(10,2),
                    cost_inr DECIMAL(10,2),
                    launch_date DATE,
                    discontinuation_date DATE
                )
            ''',
            
            'facilities': '''
                CREATE TABLE IF NOT EXISTS facilities (
                    facility_id VARCHAR(20) PRIMARY KEY,
                    facility_name VARCHAR(100),
                    facility_type VARCHAR(20),
                    location_city VARCHAR(50),
                    location_state VARCHAR(30),
                    pincode VARCHAR(10),
                    capacity_units_per_day INTEGER,
                    product_capabilities TEXT,
                    serving_regions TEXT,
                    operational_status VARCHAR(20),
                    setup_date DATE
                )
            ''',
            
            'suppliers': '''
                CREATE TABLE IF NOT EXISTS suppliers (
                    supplier_id VARCHAR(20) PRIMARY KEY,
                    supplier_name VARCHAR(100),
                    supplier_country VARCHAR(50),
                    supplier_type VARCHAR(30),
                    materials_supplied TEXT,
                    standard_lead_time_days INTEGER,
                    minimum_order_quantity INTEGER,
                    quality_rating_5 DECIMAL(3,2),
                    reliability_rating_5 DECIMAL(3,2),
                    cost_competitiveness VARCHAR(10),
                    contract_start_date DATE,
                    contract_end_date DATE,
                    payment_terms_days INTEGER
                )
            ''',
            
            'orders': '''
                CREATE TABLE IF NOT EXISTS orders (
                    order_id VARCHAR(50) PRIMARY KEY,
                    customer_id VARCHAR(30),
                    order_date DATE,
                    order_time TIME,
                    channel VARCHAR(20),
                    store_id VARCHAR(20),
                    total_items INTEGER,
                    total_quantity INTEGER,
                    gross_order_value DECIMAL(12,2),
                    discount_amount DECIMAL(10,2),
                    net_order_value DECIMAL(12,2),
                    payment_method VARCHAR(20),
                    payment_status VARCHAR(20),
                    customer_delivery_expectation DATE,
                    promised_delivery_date DATE,
                    delivery_address_full TEXT,
                    delivery_pincode VARCHAR(10),
                    delivery_instructions TEXT,
                    order_priority VARCHAR(20),
                    is_trial_order BOOLEAN,
                    estimated_dispatch_date DATE,
                    actual_dispatch_date DATE,
                    estimated_delivery_date DATE,
                    actual_delivery_date DATE,
                    delivery_status VARCHAR(20),
                    delivery_attempts INTEGER,
                    otif_status VARCHAR(20),
                    delay_days INTEGER,
                    customer_satisfaction_rating INTEGER,
                    nps_score INTEGER,
                    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
                )
            ''',
            
            'purchase_orders': '''
                CREATE TABLE IF NOT EXISTS purchase_orders (
                    po_id VARCHAR(50) PRIMARY KEY,
                    supplier_id VARCHAR(20),
                    po_date DATE,
                    expected_delivery_date DATE,
                    actual_delivery_date DATE,
                    total_po_value DECIMAL(12,2),
                    po_status VARCHAR(20),
                    materials_ordered TEXT,
                    payment_terms INTEGER,
                    quality_rating DECIMAL(3,2),
                    FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id)
                )
            ''',
            
            'production_batches': '''
                CREATE TABLE IF NOT EXISTS production_batches (
                    batch_id VARCHAR(50) PRIMARY KEY,
                    sku_code VARCHAR(30),
                    facility_id VARCHAR(20),
                    production_date DATE,
                    production_start_time TIME,
                    production_end_time TIME,
                    planned_quantity INTEGER,
                    actual_quantity_produced INTEGER,
                    efficiency_percentage DECIMAL(5,2),
                    quality_passed INTEGER,
                    raw_materials_consumed TEXT,
                    production_cost_per_unit DECIMAL(8,2),
                    FOREIGN KEY (sku_code) REFERENCES products(sku_code),
                    FOREIGN KEY (facility_id) REFERENCES facilities(facility_id)
                )
            ''',
            
            'order_line_items': '''
                CREATE TABLE IF NOT EXISTS order_line_items (
                    line_item_id VARCHAR(50) PRIMARY KEY,
                    order_id VARCHAR(50),
                    sku_code VARCHAR(30),
                    quantity_ordered INTEGER,
                    quantity_confirmed INTEGER,
                    quantity_dispatched INTEGER,
                    quantity_delivered INTEGER,
                    unit_price DECIMAL(10,2),
                    line_total DECIMAL(12,2),
                    customization_details TEXT,
                    estimated_manufacturing_date DATE,
                    actual_manufacturing_date DATE,
                    manufacturing_facility_id VARCHAR(20),
                    quality_check_status VARCHAR(20),
                    quality_check_date DATE,
                    inventory_allocation_time TIMESTAMP,
                    line_item_status VARCHAR(20),
                    dispatch_facility_id VARCHAR(20),
                    FOREIGN KEY (order_id) REFERENCES orders(order_id),
                    FOREIGN KEY (sku_code) REFERENCES products(sku_code),
                    FOREIGN KEY (manufacturing_facility_id) REFERENCES facilities(facility_id),
                    FOREIGN KEY (dispatch_facility_id) REFERENCES facilities(facility_id)
                )
            ''',
            
            'inventory_movements': '''
                CREATE TABLE IF NOT EXISTS inventory_movements (
                    movement_id VARCHAR(50) PRIMARY KEY,
                    sku_code VARCHAR(30),
                    facility_id VARCHAR(20),
                    movement_date DATE,
                    movement_time TIME,
                    movement_type VARCHAR(20),
                    quantity_change INTEGER,
                    previous_stock INTEGER,
                    new_stock INTEGER,
                    reference_id VARCHAR(50),
                    batch_number VARCHAR(50),
                    expiry_date DATE,
                    cost_per_unit DECIMAL(10,2),
                    movement_reason VARCHAR(200),
                    FOREIGN KEY (sku_code) REFERENCES products(sku_code),
                    FOREIGN KEY (facility_id) REFERENCES facilities(facility_id)
                )
            ''',
            
            'logistics_shipments': '''
                CREATE TABLE IF NOT EXISTS logistics_shipments (
                    shipment_id VARCHAR(50) PRIMARY KEY,
                    order_id VARCHAR(50),
                    carrier_name VARCHAR(50),
                    tracking_number VARCHAR(50),
                    dispatch_facility_id VARCHAR(20),
                    dispatch_date DATE,
                    dispatch_time VARCHAR(10),
                    delivery_address_verified TEXT,
                    delivery_pincode VARCHAR(10),
                    estimated_delivery_date DATE,
                    attempted_delivery_dates TEXT,
                    successful_delivery_date DATE,
                    successful_delivery_time VARCHAR(10),
                    delivery_person_name VARCHAR(100),
                    delivery_otp VARCHAR(10),
                    customer_signature_received BOOLEAN,
                    delivery_photos TEXT,
                    total_weight_kg DECIMAL(8,2),
                    total_volume_cubic_cm DECIMAL(12,2),
                    transportation_cost DECIMAL(10,2),
                    distance_km INTEGER,
                    delivery_rating_by_customer INTEGER,
                    delivery_issues VARCHAR(200),
                    return_initiated BOOLEAN,
                    FOREIGN KEY (order_id) REFERENCES orders(order_id),
                    FOREIGN KEY (dispatch_facility_id) REFERENCES facilities(facility_id)
                )
            ''',
            
            'demand_forecasts': '''
                CREATE TABLE IF NOT EXISTS demand_forecasts (
                    forecast_id VARCHAR(50) PRIMARY KEY,
                    sku_code VARCHAR(30),
                    facility_id VARCHAR(20),
                    forecast_date DATE,
                    forecast_for_date DATE,
                    forecast_horizon_days INTEGER,
                    forecasting_method VARCHAR(50),
                    base_forecast INTEGER,
                    promotional_adjustment INTEGER,
                    seasonal_adjustment DECIMAL(4,2),
                    external_factors TEXT,
                    final_forecast INTEGER,
                    actual_demand INTEGER,
                    forecast_error INTEGER,
                    forecast_error_percentage DECIMAL(6,2),
                    forecast_accuracy_rating VARCHAR(20),
                    FOREIGN KEY (sku_code) REFERENCES products(sku_code),
                    FOREIGN KEY (facility_id) REFERENCES facilities(facility_id)
                )
            ''',
            
            'supply_chain_events': '''
                CREATE TABLE IF NOT EXISTS supply_chain_events (
                    event_id VARCHAR(50) PRIMARY KEY,
                    related_order_id VARCHAR(50),
                    related_sku_code VARCHAR(30),
                    facility_id VARCHAR(20),
                    event_type VARCHAR(30),
                    event_timestamp TIMESTAMP,
                    expected_completion_time TIMESTAMP,
                    actual_completion_time TIMESTAMP,
                    duration_minutes INTEGER,
                    delay_minutes INTEGER,
                    delay_category VARCHAR(30),
                    delay_root_cause VARCHAR(200),
                    responsible_team VARCHAR(30),
                    resolution_action VARCHAR(200),
                    impact_on_customer VARCHAR(20),
                    cost_of_delay DECIMAL(10,2),
                    FOREIGN KEY (related_order_id) REFERENCES orders(order_id),
                    FOREIGN KEY (related_sku_code) REFERENCES products(sku_code),
                    FOREIGN KEY (facility_id) REFERENCES facilities(facility_id)
                )
            '''
        }
        
        print("Creating tables...")
        for table_name, create_sql in tables.items():
            cursor.execute(create_sql)
            print(f"  Created table: {table_name}")
        
        # Create indexes for better performance
        print("\nCreating indexes...")
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(order_date);",
            "CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_id);",
            "CREATE INDEX IF NOT EXISTS idx_line_items_order ON order_line_items(order_id);",
            "CREATE INDEX IF NOT EXISTS idx_line_items_sku ON order_line_items(sku_code);",
            "CREATE INDEX IF NOT EXISTS idx_inventory_sku_facility ON inventory_movements(sku_code, facility_id);",
            "CREATE INDEX IF NOT EXISTS idx_events_order ON supply_chain_events(related_order_id);",
            "CREATE INDEX IF NOT EXISTS idx_events_timestamp ON supply_chain_events(event_timestamp);",
            "CREATE INDEX IF NOT EXISTS idx_shipments_order ON logistics_shipments(order_id);",
            "CREATE INDEX IF NOT EXISTS idx_forecasts_sku ON demand_forecasts(sku_code, forecast_for_date);",
            "CREATE INDEX IF NOT EXISTS idx_production_date ON production_batches(production_date);"
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
        
        print("  Created performance indexes")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"\nAll tables created successfully in database '{DATABASE_NAME}'")
        print("Schema matches the final corrected data generator")
        return True
        
    except Exception as e:
        print(f"Error creating tables: {e}")
        return False

def validate_schema():
    """Validate the created schema matches expected structure"""
    db_config = ADMIN_CONFIG.copy()
    db_config['database'] = DATABASE_NAME
    
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        print("\nValidating schema...")
        
        # Check if all expected tables exist
        expected_tables = [
            'customers', 'products', 'facilities', 'suppliers', 'orders', 
            'purchase_orders', 'production_batches', 'order_line_items',
            'inventory_movements', 'logistics_shipments', 'demand_forecasts', 
            'supply_chain_events'
        ]
        
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
        """)
        
        existing_tables = [row[0] for row in cursor.fetchall()]
        
        print(f"  Expected tables: {len(expected_tables)}")
        print(f"  Created tables: {len(existing_tables)}")
        
        missing_tables = set(expected_tables) - set(existing_tables)
        if missing_tables:
            print(f"  Missing tables: {missing_tables}")
            return False
        
        # Check foreign key constraints
        cursor.execute("""
            SELECT tc.constraint_name, tc.table_name, kcu.column_name, 
                   ccu.table_name AS foreign_table_name,
                   ccu.column_name AS foreign_column_name 
            FROM information_schema.table_constraints AS tc 
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
            WHERE constraint_type = 'FOREIGN KEY'
        """)
        
        foreign_keys = cursor.fetchall()
        print(f"  Foreign key constraints: {len(foreign_keys)}")
        
        cursor.close()
        conn.close()
        
        print("  Schema validation completed successfully")
        return True
        
    except Exception as e:
        print(f"Error validating schema: {e}")
        return False

def main():
    print("Wakefit Database Setup Script - Updated Version")
    print("=" * 60)
    print("Matches final corrected data generator structure")
    
    if not os.environ.get('DB_PASSWORD'):
        print("Error: DB_PASSWORD not set in environment or .env file")
        return 1
    
    # Step 1: Create database
    print("\nStep 1: Creating database...")
    if not create_database():
        return 1
    
    # Step 2: Create tables
    print("\nStep 2: Creating tables...")
    if not create_tables():
        return 1
    
    # Step 3: Validate schema
    print("\nStep 3: Validating schema...")
    if not validate_schema():
        print("Warning: Schema validation failed, but tables may still be usable")
    
    print("\nDatabase setup completed successfully!")
    print("\nKey Updates Made:")
    print("  - Increased VARCHAR sizes to prevent truncation")
    print("  - Added missing columns from data generator")
    print("  - Added facility foreign key constraints")  
    print("  - Enhanced purchase_orders and production_batches tables")
    print("  - Added performance indexes")
    print("  - Added schema validation")
    
    print(f"\nNext Steps:")
    print(f"  1. Run the final data generator script")
    print(f"  2. Import CSV files in the correct order:")
    print(f"     - products.csv")
    print(f"     - customers.csv")
    print(f"     - facilities.csv") 
    print(f"     - suppliers.csv")
    print(f"     - orders.csv")
    print(f"     - order_line_items.csv")
    print(f"     - All remaining CSV files")
    
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())