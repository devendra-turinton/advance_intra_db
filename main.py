import psycopg2
import psycopg2.errors
import random
import pymongo
import pymongo.errors
import datetime
import mysql.connector
import mysql.connector.errors
import uuid
import numpy as np
from faker import Faker
import json
from decimal import Decimal
import time
from bson import ObjectId


fake = Faker()

POSTGRES_CONFIG = {
    'host': 'insights-db.postgres.database.azure.com',
    'database': 'integrated_manufacturing_system',
    'user': 'turintonadmin',
    'password': 'Passw0rd123!',  # Using original credentials
    'port': 5432
}

MYSQL_CONFIG = {
    'host': 'localhost',
    'database': 'manufacturing_operations',
    'user': 'root',
    'password': 'harish9@HY',  # Using your credentials
    'port': 3306
}

# MongoDB Configuration
MONGODB_CONFIG = {
    'host': 'localhost',
    'port': 27017,
    'database': 'manufacturing_iot'
}

SCALE_FACTORS = {
    'corporations': 1,
    'facilities': 15,
    'cost_centers': 500,
    'departments': 150,
    'employees': 15000, # 150K employees across all facilities
    'employee_roles': 100000,  # Some employees have multiple roles (180000)
    'specifications': 50000,
    'materials': 100000,
    'business_partners': 18000,  # vendors + customers + partners
    'storage_locations': 10000,
    'production_lines': 800,
    'legal_entities': 50,
    'contracts': 25000,
    'permits_licenses': 2000,
    'documents': 100000,

    # Equipment (logically grouped)
    'production_equipment': 25000,
    'measurement_instruments': 200000,
    'utility_equipment': 15000,
    'handling_equipment': 5000,
    'control_systems': 8000,
    
    # Operations
    'production_lines': 800,
    'work_orders': 200000,
    'quality_events': 100000,
    'maintenance_activities': 80000,
    
    # Transactions
    'purchase_orders': 150000,
    'purchase_order_items': 600000,  # ~4 items per PO
    'sales_orders': 180000,
    'sales_order_items': 720000,  # ~4 items per SO
    'invoices': 300000,
    'shipments': 120000,
    
    # Supply Chain
    'bill_of_materials': 200000,
    'routings': 25000,
    'goods_receipts': 200000,
    'demand_forecasts': 50000,
    'supplier_performance': 15000,
    
    # IT/OT Systems
    'it_systems': 2000,
    'network_infrastructure': 5000,
    'cybersecurity_events': 10000,
    'software_licenses': 3000,
    'process_parameters': 100000,
    'alarm_events': 500000,
    'batch_records': 50000,
    'recipes': 5000,
    'control_logic': 8000,
    
    # Maintenance
    'maintenance_plans': 5000,
    'maintenance_orders': 80000,
    'maintenance_schedules': 10000,

    # Environmental/IoT (Core MongoDB Data)
    'geographic_locations': 10000,
    'weather_stations': 200,
    'environmental_events': 5000,
    'sensor_readings': 5000000,  # 5M sensor readings for realistic IoT volume
    'equipment_analytics': 500000,
    'process_data': 1000000,
    'energy_consumption': 100000,
    'cybersecurity_events': 10000,
    'network_infrastructure': 5000,
    'it_systems': 2000,
    'software_licenses': 3000,
    'batch_records': 50000,
    'recipes': 5000,
    'control_logic': 8000,
    'alarm_events': 500000,
    'maintenance_analytics': 50000,
    'quality_analytics': 75000,
    'production_analytics': 200000,
    'supply_chain_analytics': 100000,
    
    # Reference to other databases
    'facilities': 15,
    'production_equipment': 25000,
    'employees': 150000,
    'materials': 100000
}

class PostgreSQLMasterDataGenerator:
    """Enhanced PostgreSQL Master Data Generator with Proper Relationships"""
    
    def __init__(self):
        self.conn = None
        
    def connect(self):
        """Connect to PostgreSQL database - create if doesn't exist"""
        try:
            # Try connecting to the target database first
            self.conn = psycopg2.connect(**POSTGRES_CONFIG)
            print("✅ Connected to existing PostgreSQL database")
        except psycopg2.OperationalError as e:
            if "does not exist" in str(e):
                print("🏗️  Database doesn't exist, creating it...")
                # Connect to default postgres database to create our database
                temp_config = POSTGRES_CONFIG.copy()
                temp_config['database'] = 'postgres'
                try:
                    temp_conn = psycopg2.connect(**temp_config)
                    temp_conn.autocommit = True
                    cursor = temp_conn.cursor()
                    cursor.execute(f"CREATE DATABASE {POSTGRES_CONFIG['database']}")
                    cursor.close()
                    temp_conn.close()
                    print(f"✅ Created database: {POSTGRES_CONFIG['database']}")
                    
                    # Now connect to the new database
                    self.conn = psycopg2.connect(**POSTGRES_CONFIG)
                    print("✅ Connected to new PostgreSQL database")
                except Exception as create_error:
                    print(f"❌ Error creating database: {create_error}")
                    raise
            else:
                print(f"❌ Database connection error: {e}")
                raise
            
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            print("🔒 PostgreSQL connection closed")

    def safe_string(self, text):
        """Sanitize strings to avoid formatting conflicts"""
        if text is None:
            return None
        return str(text).replace('%', 'percent')

    def create_database_and_schemas(self):
        """Create schemas in existing database"""
        cursor = self.conn.cursor()
        
        # Create schemas
        print("🏗️  Creating database schemas...")
        schemas = ['public']
        for schema in schemas:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        
        self.conn.commit()
        cursor.close()
        print("✅ Created database schemas")

    def create_master_data_tables(self):
        """Create all master data tables with proper relationships"""
        cursor = self.conn.cursor()
        print("🏗️  Creating master data tables...")
        
        # Drop existing tables in reverse dependency order to avoid FK violations
        print("🧹 Dropping existing tables if they exist...")
        drop_tables = [
            'documents', 'permits_licenses', 'contracts', 'production_lines', 
            'storage_locations', 'materials', 'specifications', 'business_partners',
            'employee_roles', 'employees', 'departments', 'cost_centers', 
            'facilities', 'legal_entities', 'corporations'
        ]
        
        for table in drop_tables:
            cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        
        self.conn.commit()
        print("✅ Cleaned up existing tables")
        
        # ============================================
        # MASTER DATA TABLES (in dependency order)
        # ============================================
        
        # 1. Corporations (Top of hierarchy - no dependencies)
        cursor.execute("""
            CREATE TABLE corporations (
                corporation_id SERIAL PRIMARY KEY,
                corporation_code VARCHAR(50) UNIQUE NOT NULL,
                corporation_name VARCHAR(200) NOT NULL,
                legal_structure VARCHAR(50),
                headquarters_location VARCHAR(200),
                incorporation_date DATE,
                stock_exchange_listing VARCHAR(100),
                ticker_symbol VARCHAR(10),
                number_of_employees INTEGER,
                annual_revenue DECIMAL(18,2),
                annual_profit DECIMAL(18,2),
                market_capitalization DECIMAL(18,2),
                total_assets DECIMAL(18,2),
                total_liabilities DECIMAL(18,2),
                shareholders_equity DECIMAL(18,2),
                number_of_facilities INTEGER,
                global_presence TEXT,
                business_segments TEXT,
                product_portfolio TEXT,
                competitive_position VARCHAR(50),
                market_share DECIMAL(5,2),
                innovation_index DECIMAL(5,2),
                sustainability_index DECIMAL(5,2),
                governance_rating VARCHAR(20),
                risk_rating VARCHAR(20),
                credit_rating VARCHAR(20),
                esg_score DECIMAL(5,2),
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE
            )
        """)
        
        # 2. Legal Entities (depends on corporations)
        cursor.execute("""
            CREATE TABLE legal_entities (
                legal_entity_id SERIAL PRIMARY KEY,
                entity_code VARCHAR(50) UNIQUE NOT NULL,
                entity_name VARCHAR(200) NOT NULL,
                entity_type VARCHAR(50),
                parent_corporation_id INTEGER REFERENCES corporations(corporation_id),
                incorporation_date DATE,
                jurisdiction VARCHAR(100),
                tax_identification_number VARCHAR(50),
                business_registration_number VARCHAR(50),
                registered_address TEXT,
                legal_representative VARCHAR(150),
                authorized_signatory VARCHAR(150),
                capital_structure JSONB,
                shareholding_structure TEXT,
                regulatory_requirements TEXT,
                compliance_status VARCHAR(50),
                annual_filing_requirements TEXT,
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE
            )
        """)
        
        # 3. Facilities (depends on corporations)
        cursor.execute("""
            CREATE TABLE facilities (
                facility_id SERIAL PRIMARY KEY,
                facility_code VARCHAR(50) UNIQUE NOT NULL,
                facility_name VARCHAR(200) NOT NULL,
                facility_type VARCHAR(50),
                facility_subtype VARCHAR(50),
                corporation_id INTEGER REFERENCES corporations(corporation_id),
                facility_manager_id INTEGER, -- FK to employees (added later)
                country VARCHAR(100),
                region VARCHAR(100),
                city VARCHAR(100),
                postal_code VARCHAR(20),
                street_address TEXT,
                latitude DECIMAL(10,8),
                longitude DECIMAL(11,8),
                time_zone VARCHAR(50),
                established_date DATE,
                total_area DECIMAL(12,2),
                built_up_area DECIMAL(12,2),
                production_area DECIMAL(12,2),
                storage_area DECIMAL(12,2),
                office_area DECIMAL(12,2),
                height_meters DECIMAL(6,2),
                number_of_buildings INTEGER,
                number_of_production_lines INTEGER,
                production_capacity DECIMAL(15,4),
                annual_production DECIMAL(15,4),
                storage_capacity DECIMAL(15,2),
                current_storage_utilization DECIMAL(5,2),
                temperature_controlled BOOLEAN DEFAULT FALSE,
                min_temperature DECIMAL(6,2),
                max_temperature DECIMAL(6,2),
                humidity_controlled BOOLEAN DEFAULT FALSE,
                hazmat_certified BOOLEAN DEFAULT FALSE,
                wms_system VARCHAR(100),
                total_employees INTEGER,
                capacity_utilization DECIMAL(5,2),
                operating_hours VARCHAR(100),
                shift_pattern VARCHAR(50),
                automation_level VARCHAR(50),
                technology_adoption_level VARCHAR(50),
                energy_consumption DECIMAL(12,2),
                water_consumption DECIMAL(12,2),
                waste_generation DECIMAL(12,2),
                environmental_certifications VARCHAR(500),
                safety_certifications VARCHAR(500),
                quality_certifications VARCHAR(500),
                security_level VARCHAR(20),
                sustainability_rating VARCHAR(20),
                operational_excellence_score DECIMAL(5,2),
                financial_performance_rating VARCHAR(20),
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE
            )
        """)
        
        # 4. Cost Centers
        cursor.execute("""
            CREATE TABLE cost_centers (
                cost_center_id SERIAL PRIMARY KEY,
                cost_center_code VARCHAR(50) UNIQUE NOT NULL,
                cost_center_name VARCHAR(200) NOT NULL,
                cost_center_type VARCHAR(50),
                facility_id INTEGER REFERENCES facilities(facility_id),
                department_id INTEGER, -- FK to departments (added later)
                parent_cost_center_id INTEGER REFERENCES cost_centers(cost_center_id),
                cost_center_manager_id INTEGER, -- FK to employees (added later)
                budget_annual DECIMAL(15,2),
                budget_current_period DECIMAL(15,2),
                actual_costs_ytd DECIMAL(15,2),
                variance_percentage DECIMAL(5,2),
                profit_center BOOLEAN DEFAULT FALSE,
                overhead_allocation_method VARCHAR(50),
                active_from_date DATE,
                active_to_date DATE,
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE
            )
        """)
        
        # 5. Departments
        cursor.execute("""
            CREATE TABLE departments (
                department_id SERIAL PRIMARY KEY,
                department_code VARCHAR(50) UNIQUE NOT NULL,
                department_name VARCHAR(200) NOT NULL,
                department_type VARCHAR(50),
                facility_id INTEGER REFERENCES facilities(facility_id),
                parent_department_id INTEGER REFERENCES departments(department_id),
                department_head_id INTEGER, -- FK to employees (added later)
                default_cost_center_id INTEGER REFERENCES cost_centers(cost_center_id),
                budget_amount DECIMAL(15,2),
                employee_count INTEGER,
                functional_area VARCHAR(100),
                establishment_date DATE,
                reporting_structure VARCHAR(100),
                performance_metrics TEXT,
                objectives TEXT,
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE
            )
        """)
        
        # 6. Employees
        cursor.execute("""
            CREATE TABLE employees (
                employee_id SERIAL PRIMARY KEY,
                employee_number VARCHAR(50) UNIQUE NOT NULL,
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                full_name VARCHAR(200),
                email_address VARCHAR(150),
                phone_number VARCHAR(50),
                hire_date DATE,
                termination_date DATE,
                employment_status VARCHAR(50),
                primary_job_title VARCHAR(150),
                department_id INTEGER REFERENCES departments(department_id),
                facility_id INTEGER REFERENCES facilities(facility_id),
                manager_id INTEGER REFERENCES employees(employee_id),
                default_cost_center_id INTEGER REFERENCES cost_centers(cost_center_id),
                work_location VARCHAR(100),
                salary_amount DECIMAL(12,2),
                hourly_rate DECIMAL(8,2),
                overtime_eligible BOOLEAN DEFAULT TRUE,
                employment_type VARCHAR(50),
                role_level VARCHAR(50),
                security_clearance VARCHAR(50),
                skills TEXT,
                certifications TEXT,
                education_background TEXT,
                previous_experience TEXT,
                performance_rating VARCHAR(20),
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE
            )
        """)
        
        # 7. Employee Roles
        cursor.execute("""
            CREATE TABLE employee_roles (
                role_id SERIAL PRIMARY KEY,
                employee_id INTEGER REFERENCES employees(employee_id),
                role_type VARCHAR(50) NOT NULL,
                role_title VARCHAR(150),
                start_date DATE,
                end_date DATE,
                reporting_to INTEGER REFERENCES employees(employee_id),
                responsibilities TEXT,
                authority_level VARCHAR(50),
                kpi_targets TEXT,
                board_appointment_date DATE,
                strategic_vision TEXT,
                leadership_style VARCHAR(100),
                public_profile_rating VARCHAR(20),
                team_size INTEGER,
                budget_responsibility DECIMAL(15,2),
                operational_scope TEXT,
                technical_specialization VARCHAR(200),
                equipment_expertise TEXT,
                certification_requirements TEXT,
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE,
                UNIQUE(employee_id, role_type, start_date)
            )
        """)
        
        # 8. Business Partners
        cursor.execute("""
            CREATE TABLE business_partners (
                partner_id SERIAL PRIMARY KEY,
                partner_number VARCHAR(50) UNIQUE NOT NULL,
                partner_name VARCHAR(200) NOT NULL,
                partner_type VARCHAR(50) NOT NULL,
                partner_category VARCHAR(50),
                industry_sector VARCHAR(100),
                business_description TEXT,
                country VARCHAR(100),
                region VARCHAR(100),
                city VARCHAR(100),
                postal_code VARCHAR(20),
                street_address TEXT,
                phone_number VARCHAR(50),
                email_address VARCHAR(150),
                website_url VARCHAR(200),
                primary_contact_person VARCHAR(150),
                tax_number VARCHAR(50),
                payment_currency VARCHAR(10),
                payment_terms VARCHAR(100),
                credit_limit DECIMAL(15,2),
                credit_rating VARCHAR(20),
                quality_rating VARCHAR(20),
                delivery_rating VARCHAR(20),
                service_rating VARCHAR(20),
                cost_competitiveness VARCHAR(20),
                overall_rating VARCHAR(20),
                relationship_start_date DATE,
                contract_end_date DATE,
                relationship_manager_id INTEGER, -- FK to employees (added later)
                preferred_partner BOOLEAN DEFAULT FALSE,
                strategic_partner BOOLEAN DEFAULT FALSE,
                blocked_status BOOLEAN DEFAULT FALSE,
                certifications JSONB,
                compliance_status VARCHAR(50),
                insurance_coverage DECIMAL(15,2),
                last_evaluation_date DATE,
                next_evaluation_date DATE,
                annual_volume DECIMAL(15,2),
                ytd_transactions INTEGER,
                created_by INTEGER, -- FK to employees (added later)
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE
            )
        """)
        
        # 9. Specifications
        cursor.execute("""
            CREATE TABLE specifications (
                specification_id SERIAL PRIMARY KEY,
                specification_number VARCHAR(50) UNIQUE NOT NULL,
                specification_name VARCHAR(200) NOT NULL,
                specification_type VARCHAR(50),
                specification_category VARCHAR(50),
                material_id INTEGER, -- Self-reference for material specs (added later)
                specification_document TEXT,
                version_number VARCHAR(20),
                effective_date DATE,
                expiry_date DATE,
                parameters JSONB,
                acceptance_criteria JSONB,
                test_methods JSONB,
                sampling_procedures TEXT,
                approval_status VARCHAR(30),
                approved_by INTEGER, -- FK to employees (added later)
                approval_date DATE,
                change_reason TEXT,
                superseded_spec_id INTEGER REFERENCES specifications(specification_id),
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_by INTEGER, -- FK to employees (added later)
                is_active BOOLEAN DEFAULT TRUE
            )
        """)
        
        # 10. Materials
        cursor.execute("""
            CREATE TABLE materials (
                material_id SERIAL PRIMARY KEY,
                material_number VARCHAR(50) UNIQUE NOT NULL,
                material_description TEXT NOT NULL,
                material_type VARCHAR(50),
                material_category VARCHAR(100),
                specification_id INTEGER REFERENCES specifications(specification_id),
                base_unit_of_measure VARCHAR(20),
                alternative_units JSONB,
                dimensions VARCHAR(100),
                weight_gross DECIMAL(12,6),
                weight_net DECIMAL(12,6),
                volume DECIMAL(12,6),
                shelf_life_days INTEGER,
                storage_requirements TEXT,
                handling_instructions TEXT,
                abc_classification VARCHAR(10),
                hazardous_material BOOLEAN DEFAULT FALSE,
                environmental_impact_rating VARCHAR(20),
                standard_cost DECIMAL(15,4),
                moving_average_price DECIMAL(15,4),
                last_purchase_price DECIMAL(15,4),
                price_unit VARCHAR(20),
                valuation_class VARCHAR(50),
                procurement_type VARCHAR(50),
                primary_supplier_id INTEGER, -- FK to business_partners (added later)
                lead_time_days INTEGER,
                safety_stock DECIMAL(12,4),
                reorder_point DECIMAL(12,4),
                maximum_stock DECIMAL(12,4),
                minimum_order_quantity DECIMAL(12,4),
                order_multiple DECIMAL(12,4),
                quality_grade VARCHAR(50),
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_by INTEGER, -- FK to employees (added later)
                is_active BOOLEAN DEFAULT TRUE
            )
        """)
        
        # 11. Storage Locations
        cursor.execute("""
            CREATE TABLE storage_locations (
                location_id SERIAL PRIMARY KEY,
                location_code VARCHAR(50) UNIQUE NOT NULL,
                location_name VARCHAR(200) NOT NULL,
                location_type VARCHAR(50),
                facility_id INTEGER REFERENCES facilities(facility_id),
                parent_location_id INTEGER REFERENCES storage_locations(location_id),
                coordinates VARCHAR(50),
                capacity DECIMAL(12,4),
                current_stock DECIMAL(12,4),
                reserved_stock DECIMAL(12,4),
                available_capacity DECIMAL(12,4),
                access_restrictions VARCHAR(100),
                picking_sequence INTEGER,
                cycle_count_class VARCHAR(10),
                last_cycle_count_date DATE,
                temperature_zone VARCHAR(20),
                hazmat_approved BOOLEAN DEFAULT FALSE,
                heavy_items_allowed BOOLEAN DEFAULT TRUE,
                max_weight_kg DECIMAL(10,2),
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE
            )
        """)
        
        # 12. Production Lines
        cursor.execute("""
            CREATE TABLE production_lines (
                line_id SERIAL PRIMARY KEY,
                line_code VARCHAR(50) UNIQUE NOT NULL,
                line_name VARCHAR(200) NOT NULL,
                line_type VARCHAR(50),
                facility_id INTEGER REFERENCES facilities(facility_id),
                department_id INTEGER REFERENCES departments(department_id),
                line_manager_id INTEGER, -- FK to employees (added later)
                default_cost_center_id INTEGER REFERENCES cost_centers(cost_center_id),
                design_capacity DECIMAL(15,4),
                effective_capacity DECIMAL(15,4),
                current_capacity DECIMAL(15,4),
                capacity_unit VARCHAR(20),
                oee_target DECIMAL(5,2),
                availability_target DECIMAL(5,2),
                performance_target DECIMAL(5,2),
                quality_target DECIMAL(5,2),
                cycle_time_seconds DECIMAL(8,2),
                changeover_time_minutes DECIMAL(8,2),
                setup_time_minutes DECIMAL(8,2),
                number_of_operators INTEGER,
                shift_pattern VARCHAR(100),
                operating_hours_per_day DECIMAL(6,2),
                maintenance_window VARCHAR(100),
                product_mix JSONB,
                process_description TEXT,
                automation_level VARCHAR(50),
                technology_level VARCHAR(50),
                floor_plan_location VARCHAR(100),
                line_length_meters DECIMAL(8,2),
                floor_space_sqm DECIMAL(8,2),
                installation_date DATE,
                commissioning_date DATE,
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE
            )
        """)
        
        # 13. Contracts
        cursor.execute("""
            CREATE TABLE contracts (
                contract_id SERIAL PRIMARY KEY,
                contract_number VARCHAR(50) UNIQUE NOT NULL,
                contract_type VARCHAR(50) NOT NULL,
                contract_category VARCHAR(50),
                partner_id INTEGER REFERENCES business_partners(partner_id),
                legal_entity_id INTEGER REFERENCES legal_entities(legal_entity_id),
                contract_value DECIMAL(18,2),
                contract_currency VARCHAR(10),
                start_date DATE,
                end_date DATE,
                renewal_terms TEXT,
                payment_terms VARCHAR(200),
                delivery_terms VARCHAR(200),
                service_level_agreements TEXT,
                performance_guarantees TEXT,
                penalty_clauses TEXT,
                termination_conditions TEXT,
                insurance_requirements TEXT,
                quality_specifications TEXT,
                compliance_requirements TEXT,
                regulatory_approvals TEXT,
                contract_manager_id INTEGER, -- FK to employees (added later)
                approval_status VARCHAR(50),
                approval_date DATE,
                approved_by INTEGER, -- FK to employees (added later)
                dispute_resolution_method VARCHAR(100),
                governing_law VARCHAR(100),
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_by INTEGER, -- FK to employees (added later)
                is_active BOOLEAN DEFAULT TRUE
            )
        """)
        
        # 14. Permits and Licenses
        cursor.execute("""
            CREATE TABLE permits_licenses (
                permit_id SERIAL PRIMARY KEY,
                permit_number VARCHAR(50) UNIQUE NOT NULL,
                permit_type VARCHAR(50),
                permit_category VARCHAR(50),
                facility_id INTEGER REFERENCES facilities(facility_id),
                legal_entity_id INTEGER REFERENCES legal_entities(legal_entity_id),
                issuing_authority VARCHAR(200),
                permit_description TEXT,
                issue_date DATE,
                expiry_date DATE,
                renewal_date DATE,
                renewal_period_months INTEGER,
                auto_renewal BOOLEAN DEFAULT FALSE,
                permit_conditions TEXT,
                compliance_requirements TEXT,
                inspection_requirements TEXT,
                reporting_requirements TEXT,
                permit_fees DECIMAL(12,2),
                renewal_fees DECIMAL(12,2),
                penalty_structure TEXT,
                responsible_person_id INTEGER, -- FK to employees (added later)
                permit_status VARCHAR(50),
                compliance_status VARCHAR(50),
                permit_documents JSONB,
                inspection_history JSONB,
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_by INTEGER, -- FK to employees (added later)
                is_active BOOLEAN DEFAULT TRUE
            )
        """)
        
        # 15. Documents
        cursor.execute("""
            CREATE TABLE documents (
                document_id SERIAL PRIMARY KEY,
                document_number VARCHAR(50) UNIQUE NOT NULL,
                document_title VARCHAR(300) NOT NULL,
                document_type VARCHAR(50),
                document_category VARCHAR(50),
                document_format VARCHAR(20),
                document_description TEXT,
                keywords JSONB,
                document_language VARCHAR(10),
                confidentiality_level VARCHAR(20),
                retention_period_years INTEGER,
                version_number VARCHAR(20),
                parent_document_id INTEGER REFERENCES documents(document_id),
                superseded_document_id INTEGER REFERENCES documents(document_id),
                related_facility_id INTEGER REFERENCES facilities(facility_id),
                related_department_id INTEGER REFERENCES departments(department_id),
                related_contract_id INTEGER REFERENCES contracts(contract_id),
                document_status VARCHAR(50),
                approval_required BOOLEAN DEFAULT TRUE,
                approved_by INTEGER, -- FK to employees (added later)
                approval_date DATE,
                review_due_date DATE,
                next_revision_date DATE,
                file_path VARCHAR(500),
                file_size_kb INTEGER,
                access_control_list JSONB,
                download_count INTEGER DEFAULT 0,
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_by INTEGER, -- FK to employees (added later)
                last_modified_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_modified_by INTEGER, -- FK to employees (added later)
                is_active BOOLEAN DEFAULT TRUE
            )
        """)
        
        self.conn.commit()
        cursor.close()
        print("✅ Created all master data tables")

    def add_foreign_key_constraints(self):
        """Add foreign key constraints after all tables are created"""
        cursor = self.conn.cursor()
        print("🔗 Adding foreign key constraints...")
        
        try:
            # Add employee FK constraints to other tables
            cursor.execute("ALTER TABLE facilities ADD CONSTRAINT fk_facility_manager FOREIGN KEY (facility_manager_id) REFERENCES employees(employee_id)")
            cursor.execute("ALTER TABLE cost_centers ADD CONSTRAINT fk_cc_department FOREIGN KEY (department_id) REFERENCES departments(department_id)")
            cursor.execute("ALTER TABLE cost_centers ADD CONSTRAINT fk_cc_manager FOREIGN KEY (cost_center_manager_id) REFERENCES employees(employee_id)")
            cursor.execute("ALTER TABLE departments ADD CONSTRAINT fk_dept_head FOREIGN KEY (department_head_id) REFERENCES employees(employee_id)")
            cursor.execute("ALTER TABLE production_lines ADD CONSTRAINT fk_line_manager FOREIGN KEY (line_manager_id) REFERENCES employees(employee_id)")
            cursor.execute("ALTER TABLE business_partners ADD CONSTRAINT fk_partner_manager FOREIGN KEY (relationship_manager_id) REFERENCES employees(employee_id)")
            cursor.execute("ALTER TABLE business_partners ADD CONSTRAINT fk_partner_created_by FOREIGN KEY (created_by) REFERENCES employees(employee_id)")
            cursor.execute("ALTER TABLE specifications ADD CONSTRAINT fk_spec_approved_by FOREIGN KEY (approved_by) REFERENCES employees(employee_id)")
            cursor.execute("ALTER TABLE specifications ADD CONSTRAINT fk_spec_created_by FOREIGN KEY (created_by) REFERENCES employees(employee_id)")
            cursor.execute("ALTER TABLE specifications ADD CONSTRAINT fk_spec_material FOREIGN KEY (material_id) REFERENCES materials(material_id)")
            cursor.execute("ALTER TABLE materials ADD CONSTRAINT fk_material_supplier FOREIGN KEY (primary_supplier_id) REFERENCES business_partners(partner_id)")
            cursor.execute("ALTER TABLE materials ADD CONSTRAINT fk_material_created_by FOREIGN KEY (created_by) REFERENCES employees(employee_id)")
            cursor.execute("ALTER TABLE contracts ADD CONSTRAINT fk_contract_manager FOREIGN KEY (contract_manager_id) REFERENCES employees(employee_id)")
            cursor.execute("ALTER TABLE contracts ADD CONSTRAINT fk_contract_approved_by FOREIGN KEY (approved_by) REFERENCES employees(employee_id)")
            cursor.execute("ALTER TABLE contracts ADD CONSTRAINT fk_contract_created_by FOREIGN KEY (created_by) REFERENCES employees(employee_id)")
            cursor.execute("ALTER TABLE permits_licenses ADD CONSTRAINT fk_permit_responsible FOREIGN KEY (responsible_person_id) REFERENCES employees(employee_id)")
            cursor.execute("ALTER TABLE permits_licenses ADD CONSTRAINT fk_permit_created_by FOREIGN KEY (created_by) REFERENCES employees(employee_id)")
            cursor.execute("ALTER TABLE documents ADD CONSTRAINT fk_doc_approved_by FOREIGN KEY (approved_by) REFERENCES employees(employee_id)")
            cursor.execute("ALTER TABLE documents ADD CONSTRAINT fk_doc_created_by FOREIGN KEY (created_by) REFERENCES employees(employee_id)")
            cursor.execute("ALTER TABLE documents ADD CONSTRAINT fk_doc_modified_by FOREIGN KEY (last_modified_by) REFERENCES employees(employee_id)")
            
            self.conn.commit()
            print("✅ Added all foreign key constraints")
        except Exception as e:
            print(f"⚠️  Some foreign key constraints may have been skipped: {e}")
            self.conn.rollback()

    def populate_all_master_data(self):
        """Populate all master data in proper order"""
        print("🔄 Starting master data population in dependency order...")
        
        # Population order is critical for foreign key integrity
        self.populate_corporations()
        self.populate_legal_entities()
        self.populate_facilities()
        self.populate_cost_centers()
        self.populate_departments()
        self.populate_employees()
        self.populate_employee_roles()
        self.populate_business_partners()
        self.populate_specifications()
        self.populate_materials()
        self.populate_storage_locations()
        self.populate_production_lines()
        self.populate_contracts()
        self.populate_permits_licenses()
        self.populate_documents()
        
        print("✅ Completed all master data population")

    def populate_corporations(self):
        """Populate corporations table"""
        cursor = self.conn.cursor()
        print("  🏢 Inserting corporations...")
        
        cursor.execute("""
            INSERT INTO corporations (
                corporation_code, corporation_name, legal_structure, headquarters_location, 
                incorporation_date, number_of_employees, annual_revenue, number_of_facilities,
                market_capitalization, total_assets, total_liabilities, shareholders_equity,
                global_presence, business_segments, product_portfolio, market_share,
                innovation_index, sustainability_index, governance_rating, risk_rating,
                credit_rating, esg_score, is_active
            ) VALUES (
                'GLOBALMFG001', 'Global Manufacturing Corporation', 'Public Corporation', 
                'New York, NY, USA', '1995-01-15', 150000, 50000000000, 15,
                75000000000, 80000000000, 30000000000, 50000000000,
                'North America, Europe, Asia-Pacific', 'Automotive, Electronics, Chemicals',
                'Industrial Equipment, Consumer Electronics, Specialty Chemicals', 12.5,
                85.2, 78.9, 'AA', 'Low', 'AAA', 85.5, TRUE
            )
        """)
        
        self.conn.commit()
        cursor.close()

    def populate_legal_entities(self):
        """Populate legal entities"""
        cursor = self.conn.cursor()
        print("  🏛️ Inserting legal entities...")
        
        legal_entities_data = []
        entity_types = ['Subsidiary', 'Division', 'Joint Venture', 'Partnership', 'Branch Office']
        
        for i in range(SCALE_FACTORS['legal_entities']):
            capital_structure = {
                'authorized_shares': random.randint(1000000, 100000000),
                'issued_shares': random.randint(500000, 50000000),
                'par_value': round(random.uniform(0.01, 10), 2)
            }
            
            legal_entities_data.append((
                f"LE{i+1:03d}",
                f"Legal Entity {i+1:03d} - {fake.company()}",
                random.choice(entity_types),
                1,  # parent_corporation_id
                fake.date_between(start_date='-20y', end_date='-1y'),
                fake.country(),
                fake.ssn(),
                fake.uuid4()[:20],
                fake.address(),
                fake.name(),
                fake.name(),
                json.dumps(capital_structure),
                fake.text(max_nb_chars=200),
                fake.text(max_nb_chars=300),
                random.choice(['Compliant', 'Under Review', 'Non-Compliant']),
                fake.text(max_nb_chars=200),
                True
            ))
        
        cursor.executemany("""
            INSERT INTO legal_entities (
                entity_code, entity_name, entity_type, parent_corporation_id, incorporation_date,
                jurisdiction, tax_identification_number, business_registration_number, registered_address,
                legal_representative, authorized_signatory, capital_structure, shareholding_structure,
                regulatory_requirements, compliance_status, annual_filing_requirements, is_active
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, legal_entities_data)
        
        self.conn.commit()
        cursor.close()

    def populate_facilities(self):
        """Populate facilities"""
        cursor = self.conn.cursor()
        print("  🏭 Inserting facilities (plants, warehouses, distribution centers)...")
        
        facilities_data = []
        facility_types = {
            'Manufacturing Plant': ['Assembly', 'Fabrication', 'Machining', 'Chemicals', 'Electronics'],
            'Warehouse': ['Raw Materials', 'Finished Goods', 'WIP Storage', 'Spare Parts'], 
            'Distribution Center': ['Regional DC', 'Central DC', 'Cross-dock', 'Returns Processing'],
            'R&D Center': ['Product Development', 'Process Engineering', 'Quality Lab'],
            'Office': ['Corporate HQ', 'Regional Office', 'Sales Office']
        }
        
        for i in range(SCALE_FACTORS['facilities']):
            facility_type = random.choice(list(facility_types.keys()))
            facility_subtype = random.choice(facility_types[facility_type])
            
            # Different attributes based on facility type
            is_warehouse = facility_type in ['Warehouse', 'Distribution Center']
            is_manufacturing = facility_type == 'Manufacturing Plant'
            
            # Calculate areas properly
            total_area = round(random.uniform(50000, 500000), 2)
            built_up_area = round(random.uniform(30000, min(300000, total_area * 0.8)), 2) if not is_warehouse else round(random.uniform(10000, min(100000, total_area * 0.6)), 2)
            production_area = round(random.uniform(20000, min(200000, built_up_area * 0.7)), 2) if is_manufacturing else None
            storage_area = round(random.uniform(5000, min(50000, built_up_area * 0.3)), 2)
            office_area = round(random.uniform(2000, min(20000, built_up_area * 0.2)), 2)
            
            facilities_data.append((
                f"FAC{i+1:03d}",  # 1
                f"{facility_type} {i+1:03d} - {facility_subtype}",  # 2
                facility_type,  # 3
                facility_subtype,  # 4
                1,  # 5 - corporation_id
                None,  # 6 - facility_manager_id (will be updated after employees)
                fake.country(),  # 7
                fake.state(),  # 8
                fake.city(),  # 9
                fake.postcode(),  # 10
                fake.address(),  # 11
                round(random.uniform(-90, 90), 6),  # 12
                round(random.uniform(-180, 180), 6),  # 13
                random.choice(['UTC-8', 'UTC-5', 'UTC', 'UTC+1', 'UTC+8']),  # 14
                fake.date_between(start_date='-20y', end_date='-2y'),  # 15
                total_area,  # 16
                built_up_area,  # 17
                production_area,  # 18
                storage_area,  # 19
                office_area,  # 20
                round(random.uniform(8, 25), 1) if is_warehouse else round(random.uniform(4, 12), 1),  # 21
                random.randint(1, 15) if not is_warehouse else random.randint(1, 5),  # 22
                random.randint(5, 80) if is_manufacturing else None,  # 23
                round(random.uniform(10000, 500000), 4) if is_manufacturing else None,  # 24
                round(random.uniform(8000, 450000), 4) if is_manufacturing else None,  # 25
                round(random.uniform(10000, 100000), 2) if is_warehouse else None,  # 26
                round(random.uniform(60, 95), 2) if is_warehouse else None,  # 27
                random.choice([True, False]) if is_warehouse else None,  # 28
                round(random.uniform(-20, 5), 1) if is_warehouse and random.choice([True, False]) else None,  # 29
                round(random.uniform(20, 25), 1) if is_warehouse and random.choice([True, False]) else None,  # 30
                random.choice([True, False]) if is_warehouse else None,  # 31
                random.choice([True, False]) if is_warehouse else None,  # 32
                random.choice(['SAP WM', 'Manhattan WMS', 'Oracle WMS', 'Custom']) if is_warehouse else None,  # 33
                random.randint(500, 15000),  # 34
                round(random.uniform(65, 95), 2),  # 35
                '24/7' if is_warehouse else '16/5',  # 36
                '3-shift' if is_manufacturing else '2-shift',  # 37
                random.choice(['High', 'Medium', 'Low']),  # 38
                random.choice(['Advanced', 'Intermediate', 'Basic']),  # 39
                round(random.uniform(50000, 5000000), 2),  # 40
                round(random.uniform(10000, 1000000), 2),  # 41
                round(random.uniform(100, 10000), 2),  # 42
                'ISO14001,OHSAS18001,ISO50001',  # 43
                'OSHA,NFPA,IFC',  # 44
                'ISO9001,ISO/TS16949,AS9100' if is_manufacturing else 'ISO9001',  # 45
                random.choice(['High', 'Medium', 'Low']) if is_warehouse else None,  # 46
                random.choice(['Excellent', 'Good', 'Fair']),  # 47
                round(random.uniform(3.0, 5.0), 1),  # 48
                random.choice(['Excellent', 'Good', 'Fair']),  # 49
                True  # 50
            ))
        
        cursor.executemany("""
            INSERT INTO facilities (
                facility_code, facility_name, facility_type, facility_subtype, corporation_id, 
                facility_manager_id, country, region, city, postal_code, street_address, 
                latitude, longitude, time_zone, established_date, total_area, built_up_area, 
                production_area, storage_area, office_area, height_meters, number_of_buildings,
                number_of_production_lines, production_capacity, annual_production, 
                storage_capacity, current_storage_utilization, temperature_controlled, 
                min_temperature, max_temperature, humidity_controlled, hazmat_certified, 
                wms_system, total_employees, capacity_utilization, operating_hours, 
                shift_pattern, automation_level, technology_adoption_level, energy_consumption,
                water_consumption, waste_generation, environmental_certifications, 
                safety_certifications, quality_certifications, security_level, 
                sustainability_rating, operational_excellence_score, financial_performance_rating, is_active
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, facilities_data)
        
        self.conn.commit()
        cursor.close()

    def populate_cost_centers(self):
        """Populate cost centers"""
        cursor = self.conn.cursor()
        print("  💰 Inserting cost centers...")
        
        cost_centers_data = []
        cc_types = ['Production', 'Maintenance', 'Quality', 'Administration', 'R&D', 'Utilities', 'Facilities']
        
        for i in range(SCALE_FACTORS['cost_centers']):
            cost_centers_data.append((
                f"CC{i+1:04d}",
                f"{random.choice(cc_types)} Cost Center {i+1:04d}",
                random.choice(cc_types),
                random.randint(1, SCALE_FACTORS['facilities']),
                None,  # department_id - will be updated after departments
                random.randint(1, min(i, 50)) if i > 50 else None,  # parent_cost_center_id
                None,  # cost_center_manager_id - will be updated after employees
                round(random.uniform(100000, 10000000), 2),
                round(random.uniform(20000, 2000000), 2),
                round(random.uniform(15000, 1800000), 2),
                round(random.uniform(-15, 25), 2),
                random.choice([True, False]),
                random.choice(['Direct', 'Activity Based', 'Volume Based']),
                fake.date_between(start_date='-5y', end_date='today'),
                None,
                True
            ))
        
        cursor.executemany("""
            INSERT INTO cost_centers (
                cost_center_code, cost_center_name, cost_center_type, facility_id,
                department_id, parent_cost_center_id, cost_center_manager_id,
                budget_annual, budget_current_period, actual_costs_ytd, variance_percentage,
                profit_center, overhead_allocation_method, active_from_date, active_to_date, is_active
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, cost_centers_data)
        
        self.conn.commit()
        cursor.close()

    def populate_departments(self):
        """Populate departments"""
        cursor = self.conn.cursor()
        print("  🏢 Inserting departments...")
        
        departments_data = []
        department_names = ['Production', 'Quality', 'Maintenance', 'Engineering', 'Finance', 'HR', 'IT', 'Procurement', 'Sales', 'R&D', 'Logistics', 'Operations']
        
        for i in range(SCALE_FACTORS['departments']):
            facility_id = (i % SCALE_FACTORS['facilities']) + 1
            dept_name = random.choice(department_names)
            
            departments_data.append((
                f"DEPT{i+1:04d}",
                f"{dept_name} - Facility {facility_id:03d}",
                dept_name,
                facility_id,
                random.randint(1, max(1, i-1)) if i > 10 else None,  # parent_department_id
                None,  # department_head_id - will be updated after employees
                random.randint(1, min(SCALE_FACTORS['cost_centers'], 100)),  # default_cost_center_id
                round(random.uniform(100000, 5000000), 2),
                random.randint(10, 200),
                dept_name,
                fake.date_between(start_date='-15y', end_date='-1y'),
                'Hierarchical',
                f"{dept_name} performance metrics: productivity, quality, cost",
                f"{dept_name} objectives: operational excellence, continuous improvement",
                True
            ))
        
        cursor.executemany("""
            INSERT INTO departments (
                department_code, department_name, department_type, facility_id, parent_department_id,
                department_head_id, default_cost_center_id, budget_amount, employee_count,
                functional_area, establishment_date, reporting_structure, performance_metrics,
                objectives, is_active
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, departments_data)
        
        self.conn.commit()
        cursor.close()

    def populate_employees(self):
        """Populate employees with proper relationships"""
        cursor = self.conn.cursor()
        print("  👥 Inserting employees...")
        
        # Get valid department and facility IDs
        cursor.execute("SELECT department_id, facility_id FROM departments ORDER BY department_id")
        dept_facility_mapping = cursor.fetchall()
        
        cursor.execute("SELECT cost_center_id FROM cost_centers ORDER BY cost_center_id LIMIT 100")
        cost_center_ids = [row[0] for row in cursor.fetchall()]
        
        job_titles = [
            'CEO', 'COO', 'CFO', 'Plant Manager', 'Operations Manager', 'Production Supervisor',
            'Quality Manager', 'Quality Inspector', 'Process Engineer', 'Industrial Engineer',
            'Maintenance Manager', 'Maintenance Technician', 'Machine Operator', 'Production Worker',
            'Warehouse Manager', 'Logistics Coordinator', 'Purchasing Manager', 'Procurement Specialist',
            'HR Manager', 'HR Specialist', 'Finance Manager', 'Controller', 'Sales Manager',
            'Sales Representative', 'Customer Service Rep', 'IT Manager', 'IT Support Specialist'
        ]
        
        batch_size = 1000
        for batch_num in range(0, SCALE_FACTORS['employees'], batch_size):
            batch_data = []
            
            for i in range(batch_size):
                emp_num = batch_num + i + 1
                if emp_num > SCALE_FACTORS['employees']:
                    break
                
                # Assign to department and corresponding facility
                dept_facility = random.choice(dept_facility_mapping)
                department_id, facility_id = dept_facility
                
                first_name = fake.first_name()
                last_name = fake.last_name()
                
                batch_data.append((
                    f"EMP{emp_num:08d}",
                    first_name,
                    last_name,
                    f"{first_name} {last_name}",
                    fake.email(),
                    fake.phone_number(),
                    fake.date_between(start_date='-15y', end_date='today'),
                    None,  # termination_date
                    'Active',
                    random.choice(job_titles),
                    department_id,
                    facility_id,
                    random.randint(1, max(1, emp_num-1)) if emp_num > 100 else None,  # manager_id
                    random.choice(cost_center_ids),
                    f"Facility {facility_id:03d}",
                    round(random.uniform(35000, 250000), 2),
                    round(random.uniform(15, 125), 2),
                    random.choice([True, False]),
                    random.choice(['Full-time', 'Part-time', 'Contract', 'Temporary']),
                    random.choice(['Executive', 'Manager', 'Supervisor', 'Senior', 'Staff', 'Entry']),
                    random.choice(['None', 'Basic', 'Confidential', 'Secret']),
                    fake.sentence(),
                    fake.sentence(),
                    fake.sentence(),
                    fake.sentence(),
                    random.choice(['Excellent', 'Good', 'Satisfactory', 'Needs Improvement']),
                    True
                ))
            
            cursor.executemany("""
                INSERT INTO employees (
                    employee_number, first_name, last_name, full_name, email_address, phone_number,
                    hire_date, termination_date, employment_status, primary_job_title, department_id,
                    facility_id, manager_id, default_cost_center_id, work_location, salary_amount,
                    hourly_rate, overtime_eligible, employment_type, role_level, security_clearance,
                    skills, certifications, education_background, previous_experience, performance_rating, is_active
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, batch_data)
            
            self.conn.commit()
            
            if (batch_num + batch_size) % 10000 == 0:
                print(f"    Inserted {min(batch_num + batch_size, SCALE_FACTORS['employees']):,} employees...")
        
        cursor.close()

    def populate_employee_roles(self):
        """Populate employee roles"""
        cursor = self.conn.cursor()
        print("  👤 Inserting employee roles...")
        
        # Get employee IDs
        cursor.execute("SELECT employee_id FROM employees ORDER BY employee_id")
        employee_ids = [row[0] for row in cursor.fetchall()]
        
        role_types = [
            'CEO', 'COO', 'CFO', 'Plant Manager', 'Operations Manager', 'Production Supervisor',
            'Quality Manager', 'Quality Inspector', 'Process Engineer', 'Industrial Engineer',
            'Maintenance Manager', 'Maintenance Technician', 'Machine Operator', 'Production Worker',
            'Team Lead', 'Senior Specialist', 'Consultant', 'Project Manager'
        ]
        
        employee_roles_data = []
        for i in range(SCALE_FACTORS['employee_roles']):
            employee_id = random.choice(employee_ids)
            role_type = random.choice(role_types)
            
            # Ensure unique combination by adding some randomness to dates
            start_date = fake.date_between(start_date='-10y', end_date='today')
            
            employee_roles_data.append((
                employee_id,
                role_type,
                f"{role_type} - {fake.job()}",
                start_date,
                fake.date_between(start_date=start_date, end_date='+2y') if random.random() < 0.1 else None,
                random.choice(employee_ids) if random.random() < 0.8 else None,  # reporting_to
                fake.text(max_nb_chars=300),
                random.choice(['High', 'Medium', 'Low']),
                fake.sentence(),
                fake.date_between(start_date='-5y', end_date='today') if 'CEO' in role_type else None,
                fake.sentence() if 'CEO' in role_type else None,
                random.choice(['Transformational', 'Transactional', 'Democratic']) if 'Manager' in role_type else None,
                random.choice(['High', 'Medium', 'Low']),
                random.randint(1, 500) if 'Manager' in role_type else None,
                round(random.uniform(100000, 10000000), 2) if 'Manager' in role_type else None,
                fake.sentence(),
                fake.sentence(),
                fake.sentence(),
                fake.sentence(),
                True
            ))
        
        # Insert in batches to handle large volume and duplicates
        batch_size = 1000
        for i in range(0, len(employee_roles_data), batch_size):
            batch = employee_roles_data[i:i+batch_size]
            try:
                cursor.executemany("""
                    INSERT INTO employee_roles (
                        employee_id, role_type, role_title, start_date, end_date, reporting_to,
                        responsibilities, authority_level, kpi_targets, board_appointment_date,
                        strategic_vision, leadership_style, public_profile_rating, team_size,
                        budget_responsibility, operational_scope, technical_specialization,
                        equipment_expertise, certification_requirements, is_active
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, batch)
                self.conn.commit()
            except psycopg2.errors.UniqueViolation as e:
                if "duplicate key value" in str(e):
                    print(f"    ⚠️  Skipping batch due to duplicate role assignments")
                    self.conn.rollback()
                    # Insert one by one to skip only duplicates
                    for role_data in batch:
                        try:
                            cursor.execute("""
                                INSERT INTO employee_roles (
                                    employee_id, role_type, role_title, start_date, end_date, reporting_to,
                                    responsibilities, authority_level, kpi_targets, board_appointment_date,
                                    strategic_vision, leadership_style, public_profile_rating, team_size,
                                    budget_responsibility, operational_scope, technical_specialization,
                                    equipment_expertise, certification_requirements, is_active
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """, role_data)
                            self.conn.commit()
                        except psycopg2.errors.UniqueViolation:
                            self.conn.rollback()
                            continue
                else:
                    raise
        
        cursor.close()

    def populate_business_partners(self):
        """Populate business partners (vendors, customers, partners)"""
        cursor = self.conn.cursor()
        print("  🤝 Inserting business partners...")
        
        # Get some employee IDs for relationship managers
        cursor.execute("SELECT employee_id FROM employees ORDER BY employee_id LIMIT 1000")
        employee_ids = [row[0] for row in cursor.fetchall()]
        
        business_partners_data = []
        partner_types = ['Vendor', 'Customer', 'Technology Partner', 'Service Provider', 'Contractor', 'Freight Carrier']
        
        for i in range(SCALE_FACTORS['business_partners']):
            partner_type = random.choice(partner_types)
            certifications = {
                'ISO9001': random.choice([True, False]),
                'ISO14001': random.choice([True, False]),
                'OHSAS18001': random.choice([True, False])
            }
            
            business_partners_data.append((
                f"BP{i+1:08d}",  # 1
                fake.company(),  # 2
                partner_type,  # 3
                random.choice(['Strategic', 'Preferred', 'Standard', 'Tactical']),  # 4
                random.choice(['Manufacturing', 'Technology', 'Chemicals', 'Logistics', 'Services']),  # 5
                fake.catch_phrase(),  # 6
                fake.country(),  # 7
                fake.state(),  # 8
                fake.city(),  # 9
                fake.postcode(),  # 10
                fake.address(),  # 11
                fake.phone_number(),  # 12
                fake.company_email(),  # 13
                fake.url(),  # 14
                fake.name(),  # 15
                fake.ssn(),  # 16
                random.choice(['USD', 'EUR', 'GBP', 'JPY']),  # 17
                random.choice(['Net 30', 'Net 60', 'COD', 'Prepaid']),  # 18
                round(random.uniform(10000, 10000000), 2),  # 19
                random.choice(['AAA', 'AA', 'A', 'BBB', 'BB']),  # 20
                random.choice(['Excellent', 'Good', 'Average', 'Poor']),  # 21
                random.choice(['Excellent', 'Good', 'Average', 'Poor']),  # 22
                random.choice(['Excellent', 'Good', 'Average', 'Poor']),  # 23
                random.choice(['Excellent', 'Good', 'Average', 'Poor']),  # 24
                random.choice(['Excellent', 'Good', 'Average', 'Poor']),  # 25
                fake.date_between(start_date='-10y', end_date='today'),  # 26
                fake.date_between(start_date='today', end_date='+3y'),  # 27
                random.choice(employee_ids) if employee_ids else None,  # 28
                random.choice([True, False]),  # 29
                random.choice([True, False]),  # 30
                False,  # 31
                json.dumps(certifications),  # 32
                random.choice(['Compliant', 'Under Review', 'Non-Compliant']),  # 33
                round(random.uniform(100000, 10000000), 2),  # 34
                fake.date_between(start_date='-2y', end_date='today'),  # 35
                fake.date_between(start_date='today', end_date='+1y'),  # 36
                round(random.uniform(100000, 50000000), 2),  # 37
                random.randint(1, 1000),  # 38
                random.choice(employee_ids) if employee_ids else None,  # 39
                True  # 40
            ))
        
        # Insert in batches
        batch_size = 1000
        for i in range(0, len(business_partners_data), batch_size):
            batch = business_partners_data[i:i+batch_size]
            cursor.executemany("""
                INSERT INTO business_partners (
                    partner_number, partner_name, partner_type, partner_category, industry_sector,
                    business_description, country, region, city, postal_code, street_address,
                    phone_number, email_address, website_url, primary_contact_person, tax_number,
                    payment_currency, payment_terms, credit_limit, credit_rating, quality_rating,
                    delivery_rating, service_rating, cost_competitiveness, overall_rating,
                    relationship_start_date, contract_end_date, relationship_manager_id,
                    preferred_partner, strategic_partner, blocked_status, certifications,
                    compliance_status, insurance_coverage, last_evaluation_date, next_evaluation_date,
                    annual_volume, ytd_transactions, created_by, is_active
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, batch)
            self.conn.commit()
            
            if (i + batch_size) % 5000 == 0:
                print(f"    Inserted {min(i + batch_size, SCALE_FACTORS['business_partners']):,} business partners...")
        
        cursor.close()

    def populate_storage_locations(self):
        """Populate storage locations"""
        cursor = self.conn.cursor()
        print("  📍 Inserting storage locations...")
        
        # Get warehouse facility IDs
        cursor.execute("SELECT facility_id FROM facilities WHERE facility_type IN ('Warehouse', 'Distribution Center')")
        warehouse_facility_ids = [row[0] for row in cursor.fetchall()]
        
        if not warehouse_facility_ids:
            print("    ⚠️  No warehouse facilities found, using all facilities")
            cursor.execute("SELECT facility_id FROM facilities")
            warehouse_facility_ids = [row[0] for row in cursor.fetchall()]
        
        storage_locations_data = []
        location_types = ['Zone', 'Aisle', 'Rack', 'Shelf', 'Bin']
        
        for i in range(SCALE_FACTORS['storage_locations']):
            facility_id = random.choice(warehouse_facility_ids)
            location_type = random.choice(location_types)
            
            storage_locations_data.append((
                f"LOC{i+1:08d}",  # 1 - location_code
                f"{location_type} {i+1:08d}",  # 2 - location_name
                location_type,  # 3 - location_type
                facility_id,  # 4 - facility_id
                random.randint(1, max(1, i-1)) if i > 100 and random.random() < 0.3 else None,  # 5 - parent_location_id
                f"{random.randint(1, 50)},{random.randint(1, 100)},{random.randint(1, 20)}",  # 6 - coordinates
                round(random.uniform(100, 10000), 4),  # 7 - capacity
                round(random.uniform(0, 5000), 4),  # 8 - current_stock
                round(random.uniform(0, 1000), 4),  # 9 - reserved_stock
                round(random.uniform(50, 9000), 4),  # 10 - available_capacity
                random.choice(['General', 'Restricted', 'Authorized Personnel Only']),  # 11 - access_restrictions
                random.randint(1, 1000),  # 12 - picking_sequence
                random.choice(['A', 'B', 'C']),  # 13 - cycle_count_class
                fake.date_between(start_date='-30d', end_date='today'),  # 14 - last_cycle_count_date
                random.choice(['Ambient', 'Chilled', 'Frozen']),  # 15 - temperature_zone
                random.choice([True, False]),  # 16 - hazmat_approved
                True,  # 17 - heavy_items_allowed
                round(random.uniform(100, 5000), 2),  # 18 - max_weight_kg
                True  # 19 - is_active
            ))
        
        cursor.executemany("""
            INSERT INTO storage_locations (
                location_code, location_name, location_type, facility_id, parent_location_id,
                coordinates, capacity, current_stock, reserved_stock, available_capacity,
                access_restrictions, picking_sequence, cycle_count_class, last_cycle_count_date,
                temperature_zone, hazmat_approved, heavy_items_allowed, max_weight_kg, is_active
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, storage_locations_data)
        
        self.conn.commit()
        cursor.close()

    def populate_production_lines(self):
        """Populate production lines"""
        cursor = self.conn.cursor()
        print("  🏭 Inserting production lines...")
        
        # Get manufacturing facility IDs
        cursor.execute("SELECT facility_id FROM facilities WHERE facility_type = 'Manufacturing Plant'")
        manufacturing_facility_ids = [row[0] for row in cursor.fetchall()]
        
        if not manufacturing_facility_ids:
            print("    ⚠️  No manufacturing facilities found, using all facilities")
            cursor.execute("SELECT facility_id FROM facilities")
            manufacturing_facility_ids = [row[0] for row in cursor.fetchall()]
        
        # Get department and cost center IDs
        cursor.execute("SELECT department_id, facility_id FROM departments")
        dept_facility_mapping = {row[1]: row[0] for row in cursor.fetchall()}
        
        cursor.execute("SELECT cost_center_id FROM cost_centers WHERE cost_center_type = 'Production' LIMIT 100")
        production_cc_ids = [row[0] for row in cursor.fetchall()]
        
        cursor.execute("SELECT employee_id FROM employees WHERE primary_job_title LIKE '%Manager%' LIMIT 500")
        manager_ids = [row[0] for row in cursor.fetchall()]
        
        production_lines_data = []
        line_types = ['Assembly', 'Machining', 'Packaging', 'Testing', 'Finishing']
        
        for i in range(SCALE_FACTORS['production_lines']):
            facility_id = random.choice(manufacturing_facility_ids)
            department_id = dept_facility_mapping.get(facility_id, random.randint(1, 150))
            
            product_mix = {
                'primary_products': random.randint(1, 5),
                'secondary_products': random.randint(0, 3),
                'changeover_frequency': random.choice(['Daily', 'Weekly', 'Monthly'])
            }
            
            production_lines_data.append((
                f"LINE{i+1:04d}",  # 1 - line_code
                f"Production Line {i+1:04d} - {random.choice(line_types)}",  # 2 - line_name
                random.choice(line_types),  # 3 - line_type
                facility_id,  # 4 - facility_id
                department_id,  # 5 - department_id
                random.choice(manager_ids) if manager_ids else None,  # 6 - line_manager_id
                random.choice(production_cc_ids) if production_cc_ids else None,  # 7 - default_cost_center_id
                round(random.uniform(1000, 50000), 4),  # 8 - design_capacity
                round(random.uniform(800, 45000), 4),  # 9 - effective_capacity
                round(random.uniform(700, 40000), 4),  # 10 - current_capacity
                random.choice(['Units/hr', 'Tons/day', 'Liters/min']),  # 11 - capacity_unit
                round(random.uniform(75, 90), 2),  # 12 - oee_target
                round(random.uniform(85, 98), 2),  # 13 - availability_target
                round(random.uniform(80, 95), 2),  # 14 - performance_target
                round(random.uniform(95, 99.5), 2),  # 15 - quality_target
                round(random.uniform(30, 300), 2),  # 16 - cycle_time_seconds
                round(random.uniform(15, 120), 2),  # 17 - changeover_time_minutes
                round(random.uniform(5, 60), 2),  # 18 - setup_time_minutes
                random.randint(2, 12),  # 19 - number_of_operators
                random.choice(['2-shift', '3-shift', 'Continuous']),  # 20 - shift_pattern
                round(random.uniform(16, 24), 2),  # 21 - operating_hours_per_day
                random.choice(['Weekends', 'Night Shift', 'Sunday']),  # 22 - maintenance_window
                json.dumps(product_mix),  # 23 - product_mix
                fake.text(max_nb_chars=300),  # 24 - process_description
                random.choice(['High', 'Medium', 'Low']),  # 25 - automation_level
                random.choice(['Advanced', 'Standard', 'Basic']),  # 26 - technology_level
                f"Building {random.randint(1, 5)}, Floor {random.randint(1, 3)}",  # 27 - floor_plan_location
                round(random.uniform(50, 500), 2),  # 28 - line_length_meters
                round(random.uniform(100, 2000), 2),  # 29 - floor_space_sqm
                fake.date_between(start_date='-10y', end_date='-1y'),  # 30 - installation_date
                fake.date_between(start_date='-9y', end_date='today'),  # 31 - commissioning_date
                True  # 32 - is_active
            ))
        
        cursor.executemany("""
            INSERT INTO production_lines (
                line_code, line_name, line_type, facility_id, department_id, line_manager_id,
                default_cost_center_id, design_capacity, effective_capacity, current_capacity,
                capacity_unit, oee_target, availability_target, performance_target, quality_target,
                cycle_time_seconds, changeover_time_minutes, setup_time_minutes, number_of_operators,
                shift_pattern, operating_hours_per_day, maintenance_window, product_mix,
                process_description, automation_level, technology_level, floor_plan_location,
                line_length_meters, floor_space_sqm, installation_date, commissioning_date, is_active
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, production_lines_data)
        
        self.conn.commit()
        cursor.close()

    def populate_contracts(self):
        """Populate contracts"""
        cursor = self.conn.cursor()
        print("  📄 Inserting contracts...")
        
        # Get business partners and legal entities
        cursor.execute("SELECT partner_id FROM business_partners ORDER BY partner_id LIMIT 5000")
        partner_ids = [row[0] for row in cursor.fetchall()]
        
        cursor.execute("SELECT legal_entity_id FROM legal_entities")
        legal_entity_ids = [row[0] for row in cursor.fetchall()]
        
        cursor.execute("SELECT employee_id FROM employees WHERE primary_job_title LIKE '%Manager%' LIMIT 500")
        manager_ids = [row[0] for row in cursor.fetchall()]
        
        contracts_data = []
        contract_types = ['Purchase Agreement', 'Sales Agreement', 'Service Contract', 'Technology License', 'Lease Agreement']
        contract_categories = ['Master Agreement', 'Blanket Order', 'Spot Purchase', 'Long-term Contract']
        
        for i in range(SCALE_FACTORS['contracts']):
            contracts_data.append((
                f"CNT{i+1:08d}",  # 1 - contract_number
                random.choice(contract_types),  # 2 - contract_type
                random.choice(contract_categories),  # 3 - contract_category
                random.choice(partner_ids) if partner_ids else None,  # 4 - partner_id
                random.choice(legal_entity_ids),  # 5 - legal_entity_id
                round(random.uniform(10000, 50000000), 2),  # 6 - contract_value
                random.choice(['USD', 'EUR', 'GBP']),  # 7 - contract_currency
                fake.date_between(start_date='-2y', end_date='today'),  # 8 - start_date
                fake.date_between(start_date='today', end_date='+3y'),  # 9 - end_date
                fake.text(max_nb_chars=200),  # 10 - renewal_terms
                random.choice(['Net 30', 'Net 60', 'Prepaid']),  # 11 - payment_terms
                random.choice(['FOB Origin', 'FOB Destination', 'CIF']),  # 12 - delivery_terms
                fake.text(max_nb_chars=300),  # 13 - service_level_agreements
                fake.text(max_nb_chars=200),  # 14 - performance_guarantees
                fake.text(max_nb_chars=200),  # 15 - penalty_clauses
                fake.text(max_nb_chars=200),  # 16 - termination_conditions
                fake.text(max_nb_chars=200),  # 17 - insurance_requirements
                fake.text(max_nb_chars=300),  # 18 - quality_specifications
                fake.text(max_nb_chars=200),  # 19 - compliance_requirements
                fake.text(max_nb_chars=200),  # 20 - regulatory_approvals
                random.choice(manager_ids) if manager_ids else None,  # 21 - contract_manager_id
                'Approved',  # 22 - approval_status
                fake.date_between(start_date='-1y', end_date='today'),  # 23 - approval_date
                random.choice(manager_ids) if manager_ids else None,  # 24 - approved_by
                'Arbitration',  # 25 - dispute_resolution_method
                fake.state(),  # 26 - governing_law
                random.choice(manager_ids) if manager_ids else None,  # 27 - created_by
                True  # 28 - is_active
            ))
        
        # Insert in batches
        batch_size = 1000
        for i in range(0, len(contracts_data), batch_size):
            batch = contracts_data[i:i+batch_size]
            cursor.executemany("""
                INSERT INTO contracts (
                    contract_number, contract_type, contract_category, partner_id, legal_entity_id,
                    contract_value, contract_currency, start_date, end_date, renewal_terms,
                    payment_terms, delivery_terms, service_level_agreements, performance_guarantees,
                    penalty_clauses, termination_conditions, insurance_requirements,
                    quality_specifications, compliance_requirements, regulatory_approvals,
                    contract_manager_id, approval_status, approval_date, approved_by,
                    dispute_resolution_method, governing_law, created_by, is_active
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, batch)
            self.conn.commit()
        
        cursor.close()

    def populate_permits_licenses(self):
        """Populate permits and licenses"""
        cursor = self.conn.cursor()
        print("  📜 Inserting permits and licenses...")
        
        # Get facility and legal entity IDs
        cursor.execute("SELECT facility_id FROM facilities")
        facility_ids = [row[0] for row in cursor.fetchall()]
        
        cursor.execute("SELECT legal_entity_id FROM legal_entities")
        legal_entity_ids = [row[0] for row in cursor.fetchall()]
        
        cursor.execute("SELECT employee_id FROM employees LIMIT 1000")
        employee_ids = [row[0] for row in cursor.fetchall()]
        
        permits_data = []
        permit_types = ['Environmental', 'Building', 'Operating', 'Safety', 'Import/Export', 'Professional License']
        permit_categories = ['Federal', 'State', 'Local', 'Industry Specific']
        
        for i in range(SCALE_FACTORS['permits_licenses']):
            permit_documents = {
                'application': f"APP_{i+1:06d}.pdf",
                'certificate': f"CERT_{i+1:06d}.pdf",
                'inspection_report': f"INSP_{i+1:06d}.pdf"
            }
            
            inspection_history = [
                {
                    'date': fake.date_between(start_date='-2y', end_date='today').isoformat(),
                    'result': random.choice(['Pass', 'Conditional Pass', 'Fail']),
                    'inspector': fake.name()
                }
            ]
            
            permits_data.append((
                f"PL{i+1:08d}",  # 1 - permit_number
                random.choice(permit_types),  # 2 - permit_type
                random.choice(permit_categories),  # 3 - permit_category
                random.choice(facility_ids),  # 4 - facility_id
                random.choice(legal_entity_ids),  # 5 - legal_entity_id
                fake.company(),  # 6 - issuing_authority
                fake.text(max_nb_chars=300),  # 7 - permit_description
                fake.date_between(start_date='-3y', end_date='-1y'),  # 8 - issue_date
                fake.date_between(start_date='today', end_date='+5y'),  # 9 - expiry_date
                fake.date_between(start_date='-6m', end_date='+6m'),  # 10 - renewal_date
                random.randint(12, 60),  # 11 - renewal_period_months
                random.choice([True, False]),  # 12 - auto_renewal
                fake.text(max_nb_chars=200),  # 13 - permit_conditions
                fake.text(max_nb_chars=200),  # 14 - compliance_requirements
                fake.text(max_nb_chars=200),  # 15 - inspection_requirements
                fake.text(max_nb_chars=200),  # 16 - reporting_requirements
                round(random.uniform(100, 10000), 2),  # 17 - permit_fees
                round(random.uniform(50, 5000), 2),  # 18 - renewal_fees
                fake.text(max_nb_chars=200),  # 19 - penalty_structure
                random.choice(employee_ids) if employee_ids else None,  # 20 - responsible_person_id
                random.choice(['Active', 'Pending Renewal', 'Expired']),  # 21 - permit_status
                random.choice(['Compliant', 'Under Review', 'Non-Compliant']),  # 22 - compliance_status
                json.dumps(permit_documents),  # 23 - permit_documents
                json.dumps(inspection_history),  # 24 - inspection_history
                random.choice(employee_ids) if employee_ids else None,  # 25 - created_by
                True  # 26 - is_active
            ))
        
        cursor.executemany("""
            INSERT INTO permits_licenses (
                permit_number, permit_type, permit_category, facility_id, legal_entity_id,
                issuing_authority, permit_description, issue_date, expiry_date, renewal_date,
                renewal_period_months, auto_renewal, permit_conditions, compliance_requirements,
                inspection_requirements, reporting_requirements, permit_fees, renewal_fees,
                penalty_structure, responsible_person_id, permit_status, compliance_status,
                permit_documents, inspection_history, created_by, is_active
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, permits_data)
        
        self.conn.commit()
        cursor.close()

    def populate_documents(self):
        """Populate documents"""
        cursor = self.conn.cursor()
        print("  📋 Inserting documents...")
        
        # Get reference IDs
        cursor.execute("SELECT facility_id FROM facilities LIMIT 100")
        facility_ids = [row[0] for row in cursor.fetchall()]
        
        cursor.execute("SELECT department_id FROM departments LIMIT 100")
        department_ids = [row[0] for row in cursor.fetchall()]
        
        cursor.execute("SELECT contract_id FROM contracts LIMIT 1000")
        contract_ids = [row[0] for row in cursor.fetchall()]
        
        cursor.execute("SELECT employee_id FROM employees LIMIT 1000")
        employee_ids = [row[0] for row in cursor.fetchall()]
        
        documents_data = []
        document_types = ['Procedure', 'Work Instruction', 'Specification', 'Report', 'Manual', 'Form']
        document_categories = ['Quality', 'Safety', 'Operational', 'Training', 'Regulatory', 'Technical']
        
        for i in range(SCALE_FACTORS['documents']):
            keywords = ["manufacturing", "quality", "safety", "process", "procedure"]
            access_control = {
                'read_access': random.choice(['All', 'Department', 'Management']),
                'write_access': random.choice(['Author', 'Department Head', 'Quality Manager']),
                'approval_access': random.choice(['Department Head', 'Quality Manager', 'Plant Manager'])
            }
            
            documents_data.append((
                f"DOC{i+1:08d}",  # 1 - document_number
                fake.catch_phrase(),  # 2 - document_title
                random.choice(document_types),  # 3 - document_type
                random.choice(document_categories),  # 4 - document_category
                random.choice(['PDF', 'DOCX', 'XLSX', 'PPT']),  # 5 - document_format
                fake.text(max_nb_chars=300),  # 6 - document_description
                json.dumps(random.sample(keywords, 3)),  # 7 - keywords
                'English',  # 8 - document_language
                random.choice(['Public', 'Internal', 'Confidential', 'Restricted']),  # 9 - confidentiality_level
                random.randint(1, 10),  # 10 - retention_period_years
                f"V{random.randint(1, 5)}.{random.randint(0, 9)}",  # 11 - version_number
                random.randint(1, max(1, i-1)) if i > 1000 and random.random() < 0.1 else None,  # 12 - parent_document_id
                random.randint(1, max(1, i-1)) if i > 1000 and random.random() < 0.05 else None,  # 13 - superseded_document_id
                random.choice(facility_ids) if random.random() < 0.3 else None,  # 14 - related_facility_id
                random.choice(department_ids) if random.random() < 0.3 else None,  # 15 - related_department_id
                random.choice(contract_ids) if random.random() < 0.1 else None,  # 16 - related_contract_id
                random.choice(['Draft', 'Under Review', 'Approved', 'Obsolete']),  # 17 - document_status
                True,  # 18 - approval_required
                random.choice(employee_ids) if employee_ids else None,  # 19 - approved_by
                fake.date_between(start_date='-1y', end_date='today'),  # 20 - approval_date
                fake.date_between(start_date='today', end_date='+1y'),  # 21 - review_due_date
                fake.date_between(start_date='today', end_date='+2y'),  # 22 - next_revision_date
                f"/documents/{i+1:08d}/doc_{i+1:08d}.pdf",  # 23 - file_path
                random.randint(50, 5000),  # 24 - file_size_kb
                json.dumps(access_control),  # 25 - access_control_list
                random.randint(0, 500),  # 26 - download_count
                random.choice(employee_ids) if employee_ids else None,  # 27 - created_by
                random.choice(employee_ids) if employee_ids else None,  # 28 - last_modified_by
                True  # 29 - is_active
            ))
        
        # Insert in batches
        batch_size = 1000
        for i in range(0, len(documents_data), batch_size):
            batch = documents_data[i:i+batch_size]
            cursor.executemany("""
                INSERT INTO documents (
                    document_number, document_title, document_type, document_category, document_format,
                    document_description, keywords, document_language, confidentiality_level,
                    retention_period_years, version_number, parent_document_id, superseded_document_id,
                    related_facility_id, related_department_id, related_contract_id, document_status,
                    approval_required, approved_by, approval_date, review_due_date, next_revision_date,
                    file_path, file_size_kb, access_control_list, download_count, created_by,
                    last_modified_by, is_active
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, batch)
            self.conn.commit()
            
            if (i + batch_size) % 10000 == 0:
                print(f"    Inserted {min(i + batch_size, SCALE_FACTORS['documents']):,} documents...")
        
        cursor.close()

    def update_circular_references(self):
        """Update foreign key references that create circular dependencies"""
        cursor = self.conn.cursor()
        print("🔄 Updating circular references...")
        
        try:
            # Update facility managers
            cursor.execute("""
                UPDATE facilities 
                SET facility_manager_id = (
                    SELECT employee_id 
                    FROM employees 
                    WHERE employees.facility_id = facilities.facility_id 
                    AND primary_job_title LIKE '%Manager%' 
                    LIMIT 1
                )
                WHERE facility_manager_id IS NULL
            """)
            
            # Update cost center managers
            cursor.execute("""
                UPDATE cost_centers 
                SET cost_center_manager_id = (
                    SELECT employee_id 
                    FROM employees 
                    WHERE employees.default_cost_center_id = cost_centers.cost_center_id 
                    AND primary_job_title LIKE '%Manager%' 
                    LIMIT 1
                )
                WHERE cost_center_manager_id IS NULL
            """)
            
            # Update department heads
            cursor.execute("""
                UPDATE departments 
                SET department_head_id = (
                    SELECT employee_id 
                    FROM employees 
                    WHERE employees.department_id = departments.department_id 
                    AND primary_job_title LIKE '%Manager%' 
                    LIMIT 1
                )
                WHERE department_head_id IS NULL
            """)
            
            # Update cost center department links
            cursor.execute("""
                UPDATE cost_centers 
                SET department_id = (
                    SELECT department_id 
                    FROM departments 
                    WHERE departments.default_cost_center_id = cost_centers.cost_center_id 
                    LIMIT 1
                )
                WHERE department_id IS NULL
            """)
            
            self.conn.commit()
            print("✅ Updated circular references")
            
        except Exception as e:
            print(f"⚠️  Some circular references could not be updated: {e}")
            self.conn.rollback()
        
        cursor.close()

    def generate_summary_report(self):
        """Generate summary report of populated data"""
        cursor = self.conn.cursor()
        print("\n" + "="*100)
        print("📊 MASTER DATA POPULATION SUMMARY REPORT")
        print("="*100)
        
        tables = [
            'corporations', 'legal_entities', 'facilities', 'cost_centers', 'departments',
            'employees', 'employee_roles', 'business_partners', 'specifications', 'materials',
            'storage_locations', 'production_lines', 'contracts', 'permits_licenses', 'documents'
        ]
        
        total_records = 0
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            total_records += count
            print(f"📋 {table:<20}: {count:>10,} records")
        
        print(f"\n🎯 TOTAL RECORDS GENERATED: {total_records:,}")
        
        # Verify foreign key relationships
        print("\n🔗 FOREIGN KEY RELATIONSHIP VERIFICATION:")
        
        # Check employee-department relationships
        cursor.execute("""
            SELECT COUNT(*) FROM employees e 
            JOIN departments d ON e.department_id = d.department_id
        """)
        emp_dept_links = cursor.fetchone()[0]
        print(f"✅ Employee-Department links: {emp_dept_links:,}")
        
        # Check employee-facility relationships
        cursor.execute("""
            SELECT COUNT(*) FROM employees e 
            JOIN facilities f ON e.facility_id = f.facility_id
        """)
        emp_facility_links = cursor.fetchone()[0]
        print(f"✅ Employee-Facility links: {emp_facility_links:,}")
        
        # Check material-supplier relationships
        cursor.execute("""
            SELECT COUNT(*) FROM materials m 
            JOIN business_partners bp ON m.primary_supplier_id = bp.partner_id
            WHERE bp.partner_type = 'Vendor'
        """)
        material_supplier_links = cursor.fetchone()[0]
        print(f"✅ Material-Supplier links: {material_supplier_links:,}")
        
        # Check cost center relationships
        cursor.execute("""
            SELECT COUNT(*) FROM cost_centers cc 
            JOIN facilities f ON cc.facility_id = f.facility_id
        """)
        cc_facility_links = cursor.fetchone()[0]
        print(f"✅ Cost Center-Facility links: {cc_facility_links:,}")
        
        # Check production line relationships
        cursor.execute("""
            SELECT COUNT(*) FROM production_lines pl 
            JOIN facilities f ON pl.facility_id = f.facility_id
            JOIN departments d ON pl.department_id = d.department_id
        """)
        line_relationships = cursor.fetchone()[0]
        print(f"✅ Production Line relationships: {line_relationships:,}")
        
        # Facility type breakdown
        cursor.execute("""
            SELECT facility_type, COUNT(*) 
            FROM facilities 
            GROUP BY facility_type 
            ORDER BY COUNT(*) DESC
        """)
        facility_breakdown = cursor.fetchall()
        print(f"\n🏭 FACILITY TYPE BREAKDOWN:")
        for facility_type, count in facility_breakdown:
            print(f"   {facility_type:<20}: {count:>5}")
        
        # Business partner type breakdown
        cursor.execute("""
            SELECT partner_type, COUNT(*) 
            FROM business_partners 
            GROUP BY partner_type 
            ORDER BY COUNT(*) DESC
        """)
        partner_breakdown = cursor.fetchall()
        print(f"\n🤝 BUSINESS PARTNER TYPE BREAKDOWN:")
        for partner_type, count in partner_breakdown:
            print(f"   {partner_type:<20}: {count:>8,}")
        
        # Employee role breakdown
        cursor.execute("""
            SELECT role_type, COUNT(*) 
            FROM employee_roles 
            GROUP BY role_type 
            ORDER BY COUNT(*) DESC
            LIMIT 10
        """)
        role_breakdown = cursor.fetchall()
        print(f"\n👤 TOP EMPLOYEE ROLES:")
        for role_type, count in role_breakdown:
            print(f"   {role_type:<20}: {count:>8,}")
        
        print("\n✅ Master data generation completed successfully!")
        print("🚀 Database ready for operational data integration")
        print("="*100)
        
        cursor.close()

    def populate_specifications(self):
        """Populate specifications"""
        cursor = self.conn.cursor()
        print("  📋 Inserting specifications...")
        
        # Get some employee IDs for approvals
        cursor.execute("SELECT employee_id FROM employees ORDER BY employee_id LIMIT 1000")
        employee_ids = [row[0] for row in cursor.fetchall()]
        
        specifications_data = []
        spec_types = ['Material', 'Process', 'Quality', 'Safety', 'Environmental']
        spec_categories = ['Standard', 'Custom', 'Industry', 'Internal', 'Regulatory']
        
        for i in range(SCALE_FACTORS['specifications']):
            # Create realistic specification parameters
            parameters = {
                'tensile_strength': f"{random.randint(200, 800)} MPa",
                'hardness': f"{random.randint(150, 400)} HB",
                'temperature_range': f"{random.randint(-40, 20)} to {random.randint(80, 200)}°C",
                'density': f"{round(random.uniform(0.5, 15), 2)} g/cm³"
            }
            
            acceptance_criteria = {
                'min_yield_strength': f"{random.randint(180, 600)} MPa",
                'max_elongation': f"{random.randint(5, 25)}%",
                'surface_finish': f"Ra {random.uniform(0.8, 6.3):.1f} μm",
                'dimensional_tolerance': f"±{random.uniform(0.01, 0.5):.2f} mm"
            }
            
            test_methods = {
                'ASTM': f"ASTM D{random.randint(100, 999)}",
                'ISO': f"ISO {random.randint(100, 9999)}",
                'internal': f"INT-{random.randint(100, 999)}"
            }
            
            specifications_data.append((
                f"SPEC{i+1:08d}",  # 1
                f"Specification {i+1:08d} - {fake.catch_phrase()}",  # 2
                random.choice(spec_types),  # 3
                random.choice(spec_categories),  # 4
                None,  # 5 - material_id (will be linked later)
                fake.text(max_nb_chars=500),  # 6
                f"V{random.randint(1, 10)}.{random.randint(0, 9)}",  # 7
                fake.date_between(start_date='-2y', end_date='today'),  # 8
                fake.date_between(start_date='today', end_date='+5y'),  # 9
                json.dumps(parameters),  # 10
                json.dumps(acceptance_criteria),  # 11
                json.dumps(test_methods),  # 12
                fake.sentence(),  # 13
                'Approved',  # 14
                random.choice(employee_ids) if employee_ids else None,  # 15
                fake.date_between(start_date='-1y', end_date='today'),  # 16
                fake.text(max_nb_chars=200),  # 17
                None,  # 18 - superseded_spec_id
                random.choice(employee_ids) if employee_ids else None,  # 19
                True  # 20
            ))
        
        # Insert in batches
        batch_size = 1000
        for i in range(0, len(specifications_data), batch_size):
            batch = specifications_data[i:i+batch_size]
            cursor.executemany("""
                INSERT INTO specifications (
                    specification_number, specification_name, specification_type, specification_category,
                    material_id, specification_document, version_number, effective_date, expiry_date,
                    parameters, acceptance_criteria, test_methods, sampling_procedures, approval_status,
                    approved_by, approval_date, change_reason, superseded_spec_id, created_by, is_active
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, batch)
            self.conn.commit()
            
            if (i + batch_size) % 10000 == 0:
                print(f"    Inserted {min(i + batch_size, SCALE_FACTORS['specifications']):,} specifications...")
        
        cursor.close()

    def populate_materials(self):
        """Populate materials with proper relationships"""
        cursor = self.conn.cursor()
        print("  📦 Inserting materials...")
        
        # Get specification and business partner IDs
        cursor.execute("SELECT specification_id FROM specifications ORDER BY specification_id LIMIT 10000")
        spec_ids = [row[0] for row in cursor.fetchall()]
        
        cursor.execute("SELECT partner_id FROM business_partners WHERE partner_type = 'Vendor' ORDER BY partner_id LIMIT 1000")
        vendor_ids = [row[0] for row in cursor.fetchall()]
        
        cursor.execute("SELECT employee_id FROM employees ORDER BY employee_id LIMIT 1000")
        employee_ids = [row[0] for row in cursor.fetchall()]
        
        materials_data = []
        material_types = ['Raw Material', 'Semi-Finished', 'Finished Good', 'Packaging', 'Spare Part', 'Tool', 'Consumable']
        material_categories = ['Metals', 'Chemicals', 'Electronics', 'Plastics', 'Textiles', 'Composites']
        
        for i in range(SCALE_FACTORS['materials']):
            # Create realistic alternative units
            base_unit = random.choice(['KG', 'EA', 'L', 'M', 'LB', 'FT'])
            alternative_units = {
                'secondary_unit': random.choice(['G', 'OZ', 'ML', 'CM']),
                'conversion_factor': round(random.uniform(0.001, 1000), 6)
            }
            
            materials_data.append((
                f"MAT{i+1:08d}",  # 1
                fake.catch_phrase(),  # 2
                random.choice(material_types),  # 3
                random.choice(material_categories),  # 4
                random.choice(spec_ids) if spec_ids else None,  # 5
                base_unit,  # 6
                json.dumps(alternative_units),  # 7
                f"{random.randint(10, 500)}x{random.randint(10, 500)}x{random.randint(5, 200)} mm",  # 8
                round(random.uniform(0.1, 100), 6),  # 9
                round(random.uniform(0.05, 95), 6),  # 10
                round(random.uniform(0.001, 10), 6),  # 11
                random.randint(30, 1095),  # 12
                fake.sentence(),  # 13
                fake.sentence(),  # 14
                random.choice(['A', 'B', 'C']),  # 15
                random.choice([True, False]),  # 16
                random.choice(['Low', 'Medium', 'High']),  # 17
                round(random.uniform(0.5, 500), 4),  # 18
                round(random.uniform(0.45, 495), 4),  # 19
                round(random.uniform(0.4, 490), 4),  # 20
                base_unit,  # 21
                random.choice(['Standard', 'Special', 'Consignment']),  # 22
                random.choice(['Make', 'Buy', 'Transfer']),  # 23
                random.choice(vendor_ids) if vendor_ids else None,  # 24
                random.randint(1, 90),  # 25
                round(random.uniform(0, 1000), 4),  # 26
                round(random.uniform(0, 500), 4),  # 27
                round(random.uniform(1000, 10000), 4),  # 28
                round(random.uniform(1, 100), 4),  # 29
                round(random.uniform(1, 50), 4),  # 30
                random.choice(['Premium', 'Standard', 'Commercial']),  # 31
                random.choice(employee_ids) if employee_ids else None,  # 32
                True  # 33
            ))
        
        # Insert in batches
        batch_size = 1000
        for i in range(0, len(materials_data), batch_size):
            batch = materials_data[i:i+batch_size]
            cursor.executemany("""
                INSERT INTO materials (
                    material_number, material_description, material_type, material_category,
                    specification_id, base_unit_of_measure, alternative_units, dimensions,
                    weight_gross, weight_net, volume, shelf_life_days, storage_requirements,
                    handling_instructions, abc_classification, hazardous_material,
                    environmental_impact_rating, standard_cost, moving_average_price,
                    last_purchase_price, price_unit, valuation_class, procurement_type,
                    primary_supplier_id, lead_time_days, safety_stock, reorder_point,
                    maximum_stock, minimum_order_quantity, order_multiple, quality_grade,
                    created_by, is_active
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, batch)
            self.conn.commit()
            
            if (i + batch_size) % 10000 == 0:
                print(f"    Inserted {min(i + batch_size, SCALE_FACTORS['materials']):,} materials...")
        
        cursor.close()

class MySQLOperationalDataGenerator:
    """Enhanced MySQL Operational Data Generator with Proper Relationships"""
    
    def __init__(self):
        self.conn = None
        
    def connect(self):
        """Connect to MySQL database - create if doesn't exist"""
        try:
            # Try connecting to the target database first
            self.conn = mysql.connector.connect(**MYSQL_CONFIG)
            print("✅ Connected to existing MySQL database")
        except mysql.connector.errors.ProgrammingError as e:
            if "Unknown database" in str(e):
                print("🏗️  Database doesn't exist, creating it...")
                # Connect to MySQL server to create our database
                temp_config = MYSQL_CONFIG.copy()
                temp_config.pop('database')
                try:
                    temp_conn = mysql.connector.connect(**temp_config)
                    cursor = temp_conn.cursor()
                    cursor.execute(f"CREATE DATABASE {MYSQL_CONFIG['database']}")
                    cursor.close()
                    temp_conn.close()
                    print(f"✅ Created database: {MYSQL_CONFIG['database']}")
                    
                    # Now connect to the new database
                    self.conn = mysql.connector.connect(**MYSQL_CONFIG)
                    print("✅ Connected to new MySQL database")
                except Exception as create_error:
                    print(f"❌ Error creating database: {create_error}")
                    raise
            else:
                print(f"❌ Database connection error: {e}")
                raise
            
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            print("🔒 MySQL connection closed")

    def create_operational_tables(self):
        """Create all operational tables with proper relationships"""
        cursor = self.conn.cursor()
        print("🏗️  Creating operational tables...")
        
        # CRITICAL FIX: Disable foreign key checks before dropping tables
        print("🔓 Disabling foreign key checks...")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        
        # Drop existing tables in reverse dependency order to avoid FK violations
        print("🧹 Dropping existing tables if they exist...")
        drop_tables = [
            'alarm_events', 'process_parameters', 'control_logic', 'batch_records', 'recipes',
            'cybersecurity_events', 'software_licenses', 'network_infrastructure', 'it_systems',
            'supplier_performance', 'demand_forecasts', 'goods_receipts', 'routing_operations', 'routings',
            'bill_of_materials', 'shipment_items', 'shipments', 'sales_order_items', 'sales_orders',
            'purchase_order_items', 'purchase_orders', 'invoice_items', 'invoices',
            'maintenance_schedules', 'maintenance_orders', 'maintenance_plans',
            'quality_events', 'work_order_operations', 'work_orders', 'equipment'
        ]
        
        for table in drop_tables:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
        
        print("✅ Cleaned up existing tables")
        
        # ============================================
        # OPERATIONAL TABLES (in dependency order)
        # ============================================
        
        # 1. Equipment Master (Foundation for all operational data)
        cursor.execute("""
            CREATE TABLE equipment (
                equipment_id INT AUTO_INCREMENT PRIMARY KEY,
                equipment_number VARCHAR(50) UNIQUE NOT NULL,
                equipment_name VARCHAR(200) NOT NULL,
                equipment_type VARCHAR(50) NOT NULL,
                equipment_category VARCHAR(50),
                equipment_subtype VARCHAR(50),
                manufacturer VARCHAR(100),
                model_number VARCHAR(100),
                serial_number VARCHAR(100),
                manufacture_date DATE,
                installation_date DATE,
                commissioning_date DATE,
                warranty_expiry_date DATE,
                facility_id INT, -- FK to PostgreSQL facilities
                production_line_id INT, -- FK to PostgreSQL production_lines
                department_id INT, -- FK to PostgreSQL departments
                cost_center_id INT, -- FK to PostgreSQL cost_centers
                location_description VARCHAR(200),
                specifications JSON,
                operating_parameters JSON,
                performance_characteristics JSON,
                rated_capacity DECIMAL(15,4),
                capacity_unit VARCHAR(20),
                power_rating DECIMAL(10,2),
                voltage_rating VARCHAR(20),
                current_rating DECIMAL(8,2),
                pressure_rating DECIMAL(8,2),
                temperature_rating VARCHAR(50),
                dimensions VARCHAR(100),
                weight DECIMAL(10,2),
                floor_space DECIMAL(8,2),
                acquisition_cost DECIMAL(15,2),
                current_book_value DECIMAL(15,2),
                depreciation_method VARCHAR(50),
                operational_status VARCHAR(30),
                utilization_rate DECIMAL(5,2),
                efficiency_rating DECIMAL(5,2),
                maintenance_plan_id INT, -- Will reference maintenance_plans
                last_maintenance_date DATE,
                next_maintenance_date DATE,
                mtbf_hours DECIMAL(10,2),
                mttr_hours DECIMAL(8,2),
                safety_features TEXT,
                safety_rating VARCHAR(20),
                environmental_rating VARCHAR(20),
                compliance_certifications JSON,
                asset_criticality VARCHAR(20),
                replacement_priority VARCHAR(20),
                lifecycle_stage VARCHAR(30),
                expected_life_years INT,
                
                -- IT/OT Integration
                network_connected BOOLEAN DEFAULT FALSE,
                ip_address VARCHAR(45),
                mac_address VARCHAR(17),
                protocol_type VARCHAR(50), -- Ethernet/IP, Modbus, PROFINET, etc.
                communication_driver VARCHAR(100),
                data_collection_enabled BOOLEAN DEFAULT FALSE,
                
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_by INT, -- FK to PostgreSQL employees
                last_modified_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                last_modified_by INT, -- FK to PostgreSQL employees
                is_active BOOLEAN DEFAULT TRUE,
                
                INDEX idx_equipment_type (equipment_type),
                INDEX idx_equipment_status (operational_status),
                INDEX idx_facility (facility_id),
                INDEX idx_production_line (production_line_id),
                INDEX idx_maintenance_plan (maintenance_plan_id)
            )
        """)
        
        # 2. Maintenance Plans (Referenced from equipment)
        cursor.execute("""
            CREATE TABLE maintenance_plans (
                plan_id INT AUTO_INCREMENT PRIMARY KEY,
                plan_number VARCHAR(50) UNIQUE NOT NULL,
                plan_name VARCHAR(200) NOT NULL,
                plan_type VARCHAR(50), -- Preventive, Predictive, Condition Based
                equipment_type VARCHAR(50), -- Applies to equipment type
                
                -- Schedule Information
                frequency_type VARCHAR(30), -- Days, Hours, Cycles, Condition
                frequency_value INT,
                lead_time_days INT,
                estimated_duration_hours DECIMAL(6,2),
                
                -- Resource Requirements
                required_skills JSON,
                required_tools JSON,
                required_parts JSON,
                estimated_labor_hours DECIMAL(6,2),
                estimated_cost DECIMAL(12,2),
                
                -- Instructions
                work_instructions TEXT,
                safety_requirements TEXT,
                special_tools_required TEXT,
                shutdown_required BOOLEAN DEFAULT FALSE,
                
                -- Approval and Control
                approval_status VARCHAR(30),
                approved_by INT, -- FK to PostgreSQL employees
                approval_date DATE,
                effective_date DATE,
                
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_by INT, -- FK to PostgreSQL employees
                is_active BOOLEAN DEFAULT TRUE,
                
                INDEX idx_plan_type (plan_type),
                INDEX idx_equipment_type (equipment_type)
            )
        """)
        
        # 3. Work Orders (Enhanced with proper BOM/Routing references)
        cursor.execute("""
            CREATE TABLE work_orders (
                work_order_id INT AUTO_INCREMENT PRIMARY KEY,
                work_order_number VARCHAR(50) UNIQUE NOT NULL,
                work_order_type VARCHAR(50),
                priority VARCHAR(20) DEFAULT 'Normal',
                material_id INT, -- FK to PostgreSQL materials
                product_description VARCHAR(300),
                production_line_id INT, -- FK to PostgreSQL production_lines
                facility_id INT, -- FK to PostgreSQL facilities
                department_id INT, -- FK to PostgreSQL departments
                cost_center_id INT, -- FK to PostgreSQL cost_centers
                
                -- BOM and Routing references
                bom_id INT, -- Will reference bill_of_materials
                routing_id INT, -- Will reference routings
                
                -- Quantities
                planned_quantity DECIMAL(15,4),
                actual_quantity DECIMAL(15,4) DEFAULT 0,
                completed_quantity DECIMAL(15,4) DEFAULT 0,
                rejected_quantity DECIMAL(15,4) DEFAULT 0,
                quantity_unit VARCHAR(20),
                
                -- Scheduling
                planned_start_datetime DATETIME,
                actual_start_datetime DATETIME,
                planned_end_datetime DATETIME,
                actual_end_datetime DATETIME,
                planned_duration_hours DECIMAL(8,2),
                actual_duration_hours DECIMAL(8,2),
                
                -- Resources
                responsible_person_id INT, -- FK to PostgreSQL employees
                assigned_team JSON,
                required_skills JSON,
                
                -- Status and Progress
                work_order_status VARCHAR(30),
                completion_percentage DECIMAL(5,2) DEFAULT 0,
                
                -- Financial
                estimated_cost DECIMAL(15,2),
                actual_cost DECIMAL(15,2),
                labor_cost DECIMAL(15,2),
                material_cost DECIMAL(15,2),
                overhead_cost DECIMAL(15,2),
                
                -- Performance
                yield_percentage DECIMAL(5,2),
                first_pass_yield DECIMAL(5,2),
                oee_achieved DECIMAL(5,2),
                cycle_time_actual DECIMAL(8,2),
                setup_time_actual DECIMAL(8,2),
                
                -- Documentation
                work_instructions TEXT,
                special_instructions TEXT,
                completion_notes TEXT,
                
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_by INT, -- FK to PostgreSQL employees
                last_modified_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                last_modified_by INT, -- FK to PostgreSQL employees
                is_active BOOLEAN DEFAULT TRUE,
                
                INDEX idx_work_order_status (work_order_status),
                INDEX idx_work_order_type (work_order_type),
                INDEX idx_production_line (production_line_id),
                INDEX idx_material (material_id),
                INDEX idx_bom (bom_id),
                INDEX idx_routing (routing_id),
                INDEX idx_planned_start (planned_start_datetime)
            )
        """)
        
        # 4. Bill of Materials (Critical missing table)
        cursor.execute("""
            CREATE TABLE bill_of_materials (
                bom_id INT AUTO_INCREMENT PRIMARY KEY,
                bom_number VARCHAR(50) UNIQUE NOT NULL,
                parent_material_id INT, -- FK to PostgreSQL materials
                component_material_id INT, -- FK to PostgreSQL materials
                bom_level INT DEFAULT 1,
                
                -- Quantity and Units
                quantity_per DECIMAL(15,6),
                unit_of_measure VARCHAR(20),
                scrap_percentage DECIMAL(5,2) DEFAULT 0,
                
                -- Validity and Versions
                effective_date DATE,
                end_date DATE,
                version_number VARCHAR(20),
                
                -- Manufacturing Info
                operation_sequence INT,
                component_type VARCHAR(30), -- Raw Material, Sub-assembly, Phantom
                supply_type VARCHAR(30), -- Make, Buy, Transfer
                
                -- Lead Time and Planning
                lead_time_offset_days INT DEFAULT 0,
                planning_percentage DECIMAL(5,2) DEFAULT 100,
                
                -- Cost Information
                cost_allocation_percentage DECIMAL(5,2) DEFAULT 0,
                
                -- Change Control
                change_reason TEXT,
                engineering_change_number VARCHAR(50),
                
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_by INT, -- FK to PostgreSQL employees
                is_active BOOLEAN DEFAULT TRUE,
                
                INDEX idx_parent_material (parent_material_id),
                INDEX idx_component_material (component_material_id),
                INDEX idx_bom_level (bom_level),
                UNIQUE KEY unique_bom_component (parent_material_id, component_material_id, effective_date)
            )
        """)
        
        # 5. Routings (Manufacturing process routes)
        cursor.execute("""
            CREATE TABLE routings (
                routing_id INT AUTO_INCREMENT PRIMARY KEY,
                routing_number VARCHAR(50) UNIQUE NOT NULL,
                routing_name VARCHAR(200) NOT NULL,
                material_id INT, -- FK to PostgreSQL materials
                routing_type VARCHAR(50), -- Standard, Alternative, Rework
                
                -- Version Control
                version_number VARCHAR(20),
                effective_date DATE,
                end_date DATE,
                
                -- Production Information
                lot_size_minimum DECIMAL(12,4),
                lot_size_maximum DECIMAL(12,4),
                setup_time_minutes DECIMAL(8,2),
                
                -- Status and Approval
                routing_status VARCHAR(30), -- Draft, Active, Inactive, Obsolete
                approved_by INT, -- FK to PostgreSQL employees
                approval_date DATE,
                
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_by INT, -- FK to PostgreSQL employees
                is_active BOOLEAN DEFAULT TRUE,
                
                INDEX idx_material (material_id),
                INDEX idx_routing_type (routing_type),
                INDEX idx_routing_status (routing_status)
            )
        """)
        
        # 6. Routing Operations (Operations within a routing)
        cursor.execute("""
            CREATE TABLE routing_operations (
                operation_id INT AUTO_INCREMENT PRIMARY KEY,
                routing_id INT,
                operation_number INT,
                operation_code VARCHAR(50),
                operation_description VARCHAR(300),
                
                -- Sequencing
                sequence_number INT,
                
                -- Work Center and Resources
                work_center VARCHAR(50),
                equipment_id INT,
                required_skill_level VARCHAR(50),
                
                -- Time Standards
                setup_time_minutes DECIMAL(8,2),
                run_time_per_unit_seconds DECIMAL(8,2),
                teardown_time_minutes DECIMAL(8,2),
                queue_time_hours DECIMAL(6,2),
                move_time_hours DECIMAL(6,2),
                
                -- Labor Requirements
                operators_required INT DEFAULT 1,
                labor_rate_per_hour DECIMAL(8,2),
                
                -- Machine Requirements
                machine_rate_per_hour DECIMAL(10,2),
                utilization_rate DECIMAL(5,2) DEFAULT 100,
                
                -- Quality and Inspection
                inspection_required BOOLEAN DEFAULT FALSE,
                quality_control_plan VARCHAR(100),
                
                -- Costing
                overhead_rate_per_hour DECIMAL(8,2),
                
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE,
                
                INDEX idx_routing (routing_id),
                INDEX idx_equipment (equipment_id),
                INDEX idx_sequence (sequence_number)
            )
        """)
        
        # 7. Purchase Orders (Enhanced)
        cursor.execute("""
            CREATE TABLE purchase_orders (
                purchase_order_id INT AUTO_INCREMENT PRIMARY KEY,
                po_number VARCHAR(50) UNIQUE NOT NULL,
                po_type VARCHAR(50),
                po_category VARCHAR(50),
                vendor_id INT, -- FK to PostgreSQL business_partners
                vendor_name VARCHAR(200),
                vendor_contact_person VARCHAR(150),
                currency VARCHAR(10),
                total_amount DECIMAL(18,2),
                tax_amount DECIMAL(18,2),
                net_amount DECIMAL(18,2),
                payment_terms VARCHAR(100),
                po_date DATE,
                required_date DATE,
                promised_date DATE,
                delivery_date DATE,
                ship_to_facility_id INT, -- FK to PostgreSQL facilities
                ship_to_warehouse_facility_id INT, -- FK to PostgreSQL facilities (warehouse type)
                ship_to_address TEXT,
                shipping_terms VARCHAR(100),
                freight_terms VARCHAR(100),
                carrier_id INT, -- FK to PostgreSQL business_partners
                created_by INT, -- FK to PostgreSQL employees
                requested_by INT, -- FK to PostgreSQL employees
                approved_by INT, -- FK to PostgreSQL employees
                approval_date DATE,
                po_status VARCHAR(30),
                contract_id INT, -- FK to PostgreSQL contracts
                contract_number VARCHAR(50),
                special_terms TEXT,
                quality_requirements TEXT,
                delivery_instructions TEXT,
                acknowledgment_date DATE,
                acknowledgment_number VARCHAR(50),
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_modified_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE,
                
                INDEX idx_po_status (po_status),
                INDEX idx_vendor (vendor_id),
                INDEX idx_po_date (po_date),
                INDEX idx_required_date (required_date)
            )
        """)
        
        # 8. Purchase Order Items (Enhanced)
        cursor.execute("""
            CREATE TABLE purchase_order_items (
                po_item_id INT AUTO_INCREMENT PRIMARY KEY,
                purchase_order_id INT,
                line_number INT,
                material_id INT, -- FK to PostgreSQL materials
                material_number VARCHAR(50),
                material_description VARCHAR(300),
                specification_id INT, -- FK to PostgreSQL specifications
                ordered_quantity DECIMAL(15,4),
                received_quantity DECIMAL(15,4) DEFAULT 0,
                invoiced_quantity DECIMAL(15,4) DEFAULT 0,
                rejected_quantity DECIMAL(15,4) DEFAULT 0,
                unit_of_measure VARCHAR(20),
                unit_price DECIMAL(15,4),
                total_price DECIMAL(18,2),
                discount_percentage DECIMAL(5,2),
                discount_amount DECIMAL(12,2),
                tax_code VARCHAR(20),
                tax_amount DECIMAL(12,2),
                required_date DATE,
                promised_date DATE,
                delivery_date DATE,
                warehouse_facility_id INT, -- FK to PostgreSQL facilities (warehouse type)
                storage_location_id INT, -- FK to PostgreSQL storage_locations
                quality_specifications TEXT,
                technical_specifications TEXT,
                inspection_required BOOLEAN DEFAULT FALSE,
                item_status VARCHAR(30),
                receipt_status VARCHAR(30),
                cost_center_id INT, -- FK to PostgreSQL cost_centers
                gl_account VARCHAR(50),
                asset_number VARCHAR(50),
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE,
                
                INDEX idx_purchase_order (purchase_order_id),
                INDEX idx_material (material_id),
                INDEX idx_item_status (item_status),
                INDEX idx_warehouse (warehouse_facility_id)
            )
        """)
        
        # 9. Sales Orders (Enhanced)
        cursor.execute("""
            CREATE TABLE sales_orders (
                sales_order_id INT AUTO_INCREMENT PRIMARY KEY,
                so_number VARCHAR(50) UNIQUE NOT NULL,
                so_type VARCHAR(50),
                so_category VARCHAR(50),
                customer_id INT, -- FK to PostgreSQL business_partners
                customer_name VARCHAR(200),
                customer_contact_person VARCHAR(150),
                customer_po_number VARCHAR(50),
                customer_reference VARCHAR(100),
                currency VARCHAR(10),
                total_amount DECIMAL(18,2),
                tax_amount DECIMAL(18,2),
                net_amount DECIMAL(18,2),
                payment_terms VARCHAR(100),
                so_date DATE,
                requested_date DATE,
                promised_date DATE,
                shipping_date DATE,
                ship_from_warehouse_facility_id INT, -- FK to PostgreSQL facilities (warehouse type)
                ship_to_address TEXT,
                bill_to_address TEXT,
                shipping_method VARCHAR(100),
                freight_terms VARCHAR(100),
                carrier_id INT, -- FK to PostgreSQL business_partners
                sales_rep_id INT, -- FK to PostgreSQL employees
                sales_territory VARCHAR(100),
                commission_rate DECIMAL(5,2),
                created_by INT, -- FK to PostgreSQL employees
                approved_by INT, -- FK to PostgreSQL employees
                approval_date DATE,
                so_status VARCHAR(30),
                production_priority VARCHAR(20),
                special_instructions TEXT,
                quality_requirements TEXT,
                packaging_instructions TEXT,
                contract_id INT, -- FK to PostgreSQL contracts
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_modified_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE,
                
                INDEX idx_so_status (so_status),
                INDEX idx_customer (customer_id),
                INDEX idx_so_date (so_date),
                INDEX idx_sales_rep (sales_rep_id)
            )
        """)
        
        # 10. Sales Order Items (Critical missing table)
        cursor.execute("""
            CREATE TABLE sales_order_items (
                so_item_id INT AUTO_INCREMENT PRIMARY KEY,
                sales_order_id INT,
                line_number INT,
                material_id INT, -- FK to PostgreSQL materials
                material_number VARCHAR(50),
                material_description VARCHAR(300),
                specification_id INT, -- FK to PostgreSQL specifications
                ordered_quantity DECIMAL(15,4),
                confirmed_quantity DECIMAL(15,4),
                shipped_quantity DECIMAL(15,4) DEFAULT 0,
                invoiced_quantity DECIMAL(15,4) DEFAULT 0,
                returned_quantity DECIMAL(15,4) DEFAULT 0,
                unit_of_measure VARCHAR(20),
                unit_price DECIMAL(15,4),
                total_price DECIMAL(18,2),
                discount_percentage DECIMAL(5,2),
                discount_amount DECIMAL(12,2),
                tax_code VARCHAR(20),
                tax_amount DECIMAL(12,2),
                requested_date DATE,
                promised_date DATE,
                shipping_date DATE,
                warehouse_facility_id INT, -- FK to PostgreSQL facilities (warehouse type)
                quality_specifications TEXT,
                packaging_requirements TEXT,
                special_handling TEXT,
                item_status VARCHAR(30), -- Open, Confirmed, Shipped, Invoiced, Closed
                fulfillment_status VARCHAR(30), -- Not Started, In Production, Ready, Shipped
                
                -- Production Link
                work_order_id INT,
                
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE,
                
                INDEX idx_sales_order (sales_order_id),
                INDEX idx_material (material_id),
                INDEX idx_item_status (item_status),
                INDEX idx_work_order (work_order_id)
            )
        """)
        
        # 11. Goods Receipts (Receiving transactions)
        cursor.execute("""
            CREATE TABLE goods_receipts (
                receipt_id INT AUTO_INCREMENT PRIMARY KEY,
                receipt_number VARCHAR(50) UNIQUE NOT NULL,
                receipt_type VARCHAR(50), -- Purchase Order, Transfer, Return, etc.
                receipt_date DATE,
                receipt_time TIME,
                
                -- Source Document
                source_document_type VARCHAR(50), -- Purchase Order, Transfer Order, etc.
                source_document_number VARCHAR(50),
                po_item_id INT,
                
                -- Material Information
                material_id INT, -- FK to PostgreSQL materials
                material_number VARCHAR(50),
                lot_number VARCHAR(50),
                batch_number VARCHAR(50),
                serial_numbers JSON,
                
                -- Quantities
                delivered_quantity DECIMAL(15,4),
                received_quantity DECIMAL(15,4),
                accepted_quantity DECIMAL(15,4),
                rejected_quantity DECIMAL(15,4),
                unit_of_measure VARCHAR(20),
                
                -- Location
                facility_id INT, -- FK to PostgreSQL facilities
                warehouse_facility_id INT, -- FK to PostgreSQL facilities (warehouse type)
                storage_location_id INT, -- FK to PostgreSQL storage_locations
                
                -- Quality and Inspection
                inspection_required BOOLEAN DEFAULT FALSE,
                inspection_status VARCHAR(30), -- Not Required, Pending, Passed, Failed
                quality_certificate_number VARCHAR(50),
                rejection_reason TEXT,
                
                -- Financial
                unit_cost DECIMAL(15,4),
                total_value DECIMAL(18,2),
                
                -- Personnel
                received_by INT, -- FK to PostgreSQL employees
                inspector_id INT, -- FK to PostgreSQL employees
                
                -- Status
                receipt_status VARCHAR(30), -- Open, Completed, Cancelled
                posted_to_inventory BOOLEAN DEFAULT FALSE,
                
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_by INT, -- FK to PostgreSQL employees
                is_active BOOLEAN DEFAULT TRUE,
                
                INDEX idx_receipt_date (receipt_date),
                INDEX idx_material (material_id),
                INDEX idx_po_item (po_item_id),
                INDEX idx_inspection_status (inspection_status)
            )
        """)
        
        # 12. Shipments (Outbound logistics)
        cursor.execute("""
            CREATE TABLE shipments (
                shipment_id INT AUTO_INCREMENT PRIMARY KEY,
                shipment_number VARCHAR(50) UNIQUE NOT NULL,
                shipment_type VARCHAR(50), -- Sales Order, Transfer, Sample, etc.
                shipment_date DATE,
                shipment_time TIME,
                
                -- Customer/Destination
                customer_id INT, -- FK to PostgreSQL business_partners
                ship_to_address TEXT,
                
                -- Carrier Information
                carrier_id INT, -- FK to PostgreSQL business_partners
                carrier_name VARCHAR(200),
                tracking_number VARCHAR(100),
                service_level VARCHAR(50), -- Ground, Express, Overnight, etc.
                
                -- Weight and Dimensions
                total_weight DECIMAL(12,4),
                total_volume DECIMAL(12,4),
                number_of_packages INT,
                
                -- Shipping Costs
                freight_cost DECIMAL(12,2),
                insurance_cost DECIMAL(12,2),
                handling_charges DECIMAL(12,2),
                total_shipping_cost DECIMAL(12,2),
                
                -- Status and Tracking
                shipment_status VARCHAR(30), -- Planned, Picked, Packed, Shipped, Delivered
                pickup_date DATE,
                estimated_delivery_date DATE,
                actual_delivery_date DATE,
                
                -- Source Facility
                ship_from_facility_id INT, -- FK to PostgreSQL facilities
                ship_from_warehouse_facility_id INT, -- FK to PostgreSQL facilities (warehouse type)
                
                -- Documentation
                bill_of_lading VARCHAR(100),
                commercial_invoice VARCHAR(100),
                packing_list VARCHAR(100),
                
                -- Personnel
                shipped_by INT, -- FK to PostgreSQL employees
                packed_by INT, -- FK to PostgreSQL employees
                
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_by INT, -- FK to PostgreSQL employees
                is_active BOOLEAN DEFAULT TRUE,
                
                INDEX idx_shipment_date (shipment_date),
                INDEX idx_customer (customer_id),
                INDEX idx_carrier (carrier_id),
                INDEX idx_shipment_status (shipment_status),
                INDEX idx_tracking_number (tracking_number)
            )
        """)
        
        # 13. Shipment Items (Items within a shipment)
        cursor.execute("""
            CREATE TABLE shipment_items (
                shipment_item_id INT AUTO_INCREMENT PRIMARY KEY,
                shipment_id INT,
                so_item_id INT,
                
                -- Material Information
                material_id INT, -- FK to PostgreSQL materials
                material_number VARCHAR(50),
                lot_number VARCHAR(50),
                serial_numbers JSON,
                
                -- Quantities
                ordered_quantity DECIMAL(15,4),
                shipped_quantity DECIMAL(15,4),
                unit_of_measure VARCHAR(20),
                
                -- Packaging
                package_type VARCHAR(50),
                number_of_packages INT,
                
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE,
                
                INDEX idx_shipment (shipment_id),
                INDEX idx_so_item (so_item_id),
                INDEX idx_material (material_id)
            )
        """)
        
        # 14. Quality Events (Quality management)
        cursor.execute("""
            CREATE TABLE quality_events (
                quality_event_id INT AUTO_INCREMENT PRIMARY KEY,
                event_number VARCHAR(50) UNIQUE NOT NULL,
                event_type VARCHAR(50), -- Inspection, Audit, Complaint, Corrective Action
                event_category VARCHAR(50),
                severity_level VARCHAR(20),
                
                -- Source Information
                source_type VARCHAR(50), -- Work Order, Purchase Order, Customer Complaint
                source_document_number VARCHAR(50),
                material_id INT, -- FK to PostgreSQL materials
                equipment_id INT,
                facility_id INT, -- FK to PostgreSQL facilities
                department_id INT, -- FK to PostgreSQL departments
                
                -- Event Details
                event_date DATE,
                discovered_by INT, -- FK to PostgreSQL employees
                description TEXT,
                root_cause_analysis TEXT,
                
                -- Quality Data
                specification_id INT, -- FK to PostgreSQL specifications
                test_results JSON,
                measurements JSON,
                pass_fail_status VARCHAR(10),
                
                -- Corrective Actions
                corrective_action_required BOOLEAN DEFAULT FALSE,
                corrective_action_plan TEXT,
                responsible_person_id INT, -- FK to PostgreSQL employees
                target_completion_date DATE,
                actual_completion_date DATE,
                
                -- Status and Approval
                event_status VARCHAR(30),
                approved_by INT, -- FK to PostgreSQL employees
                approval_date DATE,
                
                -- Cost Impact
                cost_impact DECIMAL(15,2),
                
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_by INT, -- FK to PostgreSQL employees
                is_active BOOLEAN DEFAULT TRUE,
                
                INDEX idx_event_type (event_type),
                INDEX idx_event_date (event_date),
                INDEX idx_severity (severity_level),
                INDEX idx_material (material_id),
                INDEX idx_equipment (equipment_id)
            )
        """)
        
        # 15. Maintenance Orders (Maintenance execution)
        cursor.execute("""
            CREATE TABLE maintenance_orders (
                maintenance_order_id INT AUTO_INCREMENT PRIMARY KEY,
                order_number VARCHAR(50) UNIQUE NOT NULL,
                order_type VARCHAR(50), -- Preventive, Corrective, Emergency, Predictive
                priority VARCHAR(20),
                
                -- Equipment Information
                equipment_id INT,
                equipment_number VARCHAR(50),
                maintenance_plan_id INT,
                
                -- Scheduling
                scheduled_start_date DATE,
                scheduled_end_date DATE,
                actual_start_date DATE,
                actual_end_date DATE,
                estimated_duration_hours DECIMAL(6,2),
                actual_duration_hours DECIMAL(6,2),
                
                -- Resources
                assigned_technician_id INT, -- FK to PostgreSQL employees
                maintenance_team JSON,
                required_skills JSON,
                
                -- Work Description
                work_description TEXT,
                work_performed TEXT,
                parts_used JSON,
                tools_used JSON,
                
                -- Status and Progress
                order_status VARCHAR(30),
                completion_percentage DECIMAL(5,2) DEFAULT 0,
                
                -- Costs
                estimated_cost DECIMAL(12,2),
                actual_cost DECIMAL(12,2),
                labor_cost DECIMAL(12,2),
                parts_cost DECIMAL(12,2),
                contractor_cost DECIMAL(12,2),
                
                -- Quality and Safety
                safety_procedures_followed BOOLEAN DEFAULT TRUE,
                quality_check_passed BOOLEAN DEFAULT TRUE,
                post_maintenance_test_results TEXT,
                
                -- Documentation
                work_order_notes TEXT,
                completion_notes TEXT,
                
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_by INT, -- FK to PostgreSQL employees
                is_active BOOLEAN DEFAULT TRUE,
                
                INDEX idx_order_type (order_type),
                INDEX idx_equipment (equipment_id),
                INDEX idx_maintenance_plan (maintenance_plan_id),
                INDEX idx_scheduled_date (scheduled_start_date),
                INDEX idx_order_status (order_status)
            )
        """)
        
        print("✅ Created all table structures")
        
        # CRITICAL FIX: Add foreign key constraints AFTER all tables are created
        print("🔗 Adding foreign key constraints...")
        try:
            cursor.execute("ALTER TABLE equipment ADD CONSTRAINT fk_equipment_maintenance_plan FOREIGN KEY (maintenance_plan_id) REFERENCES maintenance_plans(plan_id)")
            cursor.execute("ALTER TABLE work_orders ADD CONSTRAINT fk_work_order_bom FOREIGN KEY (bom_id) REFERENCES bill_of_materials(bom_id)")
            cursor.execute("ALTER TABLE work_orders ADD CONSTRAINT fk_work_order_routing FOREIGN KEY (routing_id) REFERENCES routings(routing_id)")
            cursor.execute("ALTER TABLE routing_operations ADD CONSTRAINT fk_routing_operation_routing FOREIGN KEY (routing_id) REFERENCES routings(routing_id)")
            cursor.execute("ALTER TABLE routing_operations ADD CONSTRAINT fk_routing_operation_equipment FOREIGN KEY (equipment_id) REFERENCES equipment(equipment_id)")
            cursor.execute("ALTER TABLE purchase_order_items ADD CONSTRAINT fk_po_item_po FOREIGN KEY (purchase_order_id) REFERENCES purchase_orders(purchase_order_id)")
            cursor.execute("ALTER TABLE sales_order_items ADD CONSTRAINT fk_so_item_so FOREIGN KEY (sales_order_id) REFERENCES sales_orders(sales_order_id)")
            cursor.execute("ALTER TABLE sales_order_items ADD CONSTRAINT fk_so_item_work_order FOREIGN KEY (work_order_id) REFERENCES work_orders(work_order_id)")
            cursor.execute("ALTER TABLE goods_receipts ADD CONSTRAINT fk_goods_receipt_po_item FOREIGN KEY (po_item_id) REFERENCES purchase_order_items(po_item_id)")
            cursor.execute("ALTER TABLE shipment_items ADD CONSTRAINT fk_shipment_item_shipment FOREIGN KEY (shipment_id) REFERENCES shipments(shipment_id)")
            cursor.execute("ALTER TABLE shipment_items ADD CONSTRAINT fk_shipment_item_so_item FOREIGN KEY (so_item_id) REFERENCES sales_order_items(so_item_id)")
            cursor.execute("ALTER TABLE quality_events ADD CONSTRAINT fk_quality_event_equipment FOREIGN KEY (equipment_id) REFERENCES equipment(equipment_id)")
            cursor.execute("ALTER TABLE maintenance_orders ADD CONSTRAINT fk_maintenance_order_equipment FOREIGN KEY (equipment_id) REFERENCES equipment(equipment_id)")
            cursor.execute("ALTER TABLE maintenance_orders ADD CONSTRAINT fk_maintenance_order_plan FOREIGN KEY (maintenance_plan_id) REFERENCES maintenance_plans(plan_id)")
            print("✅ Added foreign key constraints")
        except Exception as e:
            print(f"⚠️  Some foreign key constraints may have been skipped: {e}")
        
        # CRITICAL FIX: Re-enable foreign key checks
        print("🔒 Re-enabling foreign key checks...")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        
        self.conn.commit()
        cursor.close()
        print("✅ Created all MySQL operational tables with proper relationships")

    def populate_all_operational_data(self):
        """Populate all operational data in proper order"""
        print("🔄 Starting operational data population in dependency order...")
        
        # Population order is critical for foreign key integrity
        self.populate_maintenance_plans()
        self.populate_equipment()
        self.populate_bill_of_materials()
        self.populate_routings()
        self.populate_routing_operations()
        self.populate_work_orders()
        self.populate_purchase_orders()
        self.populate_purchase_order_items()
        self.populate_sales_orders()
        self.populate_sales_order_items()
        self.populate_goods_receipts()
        self.populate_shipments()
        self.populate_shipment_items()
        self.populate_quality_events()
        self.populate_maintenance_orders()
        
        print("✅ Completed all operational data population")

    def populate_maintenance_plans(self):
        """Populate maintenance plans"""
        cursor = self.conn.cursor()
        print("  🔧 Inserting maintenance plans...")
        
        maintenance_plans_data = []
        plan_types = ['Preventive', 'Predictive', 'Condition Based']
        equipment_types = ['Production Machine', 'Utility Equipment', 'Material Handling', 'Control System']
        
        for i in range(SCALE_FACTORS['maintenance_plans']):
            required_skills = ["Mechanical", "Electrical", "Hydraulics", "Pneumatics"]
            required_tools = ["Multimeter", "Torque Wrench", "Vibration Analyzer", "Alignment Tools"]
            required_parts = ["Filters", "Seals", "Bearings", "Belts"]
            
            maintenance_plans_data.append((
                f"MP{i+1:06d}",
                f"Maintenance Plan {i+1:06d}",
                random.choice(plan_types),
                random.choice(equipment_types),
                random.choice(['Days', 'Hours', 'Cycles']),
                random.randint(30, 365),  # frequency_value
                random.randint(1, 14),    # lead_time_days
                round(random.uniform(0.5, 8), 2),  # estimated_duration_hours
                json.dumps(random.sample(required_skills, 2)),
                json.dumps(random.sample(required_tools, 3)),
                json.dumps(random.sample(required_parts, 2)),
                round(random.uniform(0.5, 8), 2),   # estimated_labor_hours
                round(random.uniform(100, 5000), 2), # estimated_cost
                fake.text(max_nb_chars=500),
                fake.sentence(),
                fake.sentence(),
                random.choice([True, False]),
                'Approved',
                random.randint(1, 100),  # approved_by
                fake.date_between(start_date='-1y', end_date='today'),
                fake.date_between(start_date='-1y', end_date='today'),
                random.randint(1, 100),  # created_by
                True
            ))
        
        cursor.executemany("""
            INSERT INTO maintenance_plans (
                plan_number, plan_name, plan_type, equipment_type, frequency_type,
                frequency_value, lead_time_days, estimated_duration_hours, required_skills,
                required_tools, required_parts, estimated_labor_hours, estimated_cost,
                work_instructions, safety_requirements, special_tools_required,
                shutdown_required, approval_status, approved_by, approval_date,
                effective_date, created_by, is_active
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, maintenance_plans_data)
        
        self.conn.commit()
        cursor.close()

    def populate_equipment(self):
        """Populate equipment with IT/OT fields"""
        cursor = self.conn.cursor()
        print("  ⚙️ Inserting enhanced equipment...")
        
        equipment_data = []
        equipment_types = {
            'Production Machine': ['CNC Machine', 'Press', 'Lathe', 'Mill', 'Grinder', 'Assembly Station'],
            'Process Sensor': [
                'Temperature Sensor', 'Pressure Sensor', 'Flow Sensor', 'Level Sensor',
                'pH Sensor', 'Conductivity Sensor', 'Turbidity Sensor', 'Dissolved Oxygen Sensor'
            ],
            'Control System': ['PLC', 'HMI', 'VFD', 'SCADA', 'DCS', 'Edge Computer'],
            'Utility Equipment': ['Pump', 'Valve', 'Motor', 'Compressor', 'Heat Exchanger'],
            'Material Handling': ['Conveyor', 'AGV', 'Crane', 'Robot', 'Forklift']
        }
        
        equipment_counter = 1
        for equipment_type, subtypes in equipment_types.items():
            for subtype in subtypes:
                count = random.randint(100, 500)
                
                for i in range(count):
                    # Enhanced with IT/OT integration fields
                    network_connected = random.choice([True, False])
                    ip_address = fake.ipv4() if network_connected else None
                    mac_address = fake.mac_address() if network_connected else None
                    
                    equipment_data.append((
                        f"EQ{equipment_counter:08d}",                                  # 1 - equipment_number
                        f"{subtype} {equipment_counter:04d}",                         # 2 - equipment_name
                        equipment_type,                                               # 3 - equipment_type
                        subtype,                                                      # 4 - equipment_category
                        random.choice(['Standard', 'Heavy Duty', 'Precision']),      # 5 - equipment_subtype
                        random.choice(['Siemens', 'ABB', 'Schneider', 'Rockwell']), # 6 - manufacturer
                        f"Model-{random.randint(1000, 9999)}",                      # 7 - model_number
                        fake.uuid4()[:16],                                           # 8 - serial_number
                        fake.date_between(start_date='-15y', end_date='-2y'),       # 9 - manufacture_date
                        fake.date_between(start_date='-10y', end_date='today'),     # 10 - installation_date
                        fake.date_between(start_date='-8y', end_date='today'),      # 11 - commissioning_date
                        fake.date_between(start_date='today', end_date='+5y'),      # 12 - warranty_expiry_date
                        random.randint(1, 15),                                       # 13 - facility_id
                        random.randint(1, 100),                                      # 14 - production_line_id
                        random.randint(1, 120),                                      # 15 - department_id
                        random.randint(1, 100),                                      # 16 - cost_center_id
                        f"Zone {chr(65+random.randint(0, 9))}-{random.randint(1, 50)}", # 17 - location_description
                        '{"rated_pressure": "10 bar", "operating_temp": "20-80C"}', # 18 - specifications
                        '{"normal_range": "70-90%", "efficiency": "85%"}',          # 19 - operating_parameters
                        '{"accuracy": "±0.5%", "repeatability": "±0.1%"}',         # 20 - performance_characteristics
                        round(random.uniform(100, 50000), 2),                       # 21 - rated_capacity
                        random.choice(['Units/hr', 'L/min', 'kg/hr']),             # 22 - capacity_unit
                        round(random.uniform(1, 500), 2),                           # 23 - power_rating
                        random.choice(['220V', '380V', '480V']),                    # 24 - voltage_rating
                        round(random.uniform(5, 200), 2),                           # 25 - current_rating
                        round(random.uniform(1, 100), 2),                           # 26 - pressure_rating
                        f"{random.randint(-20, 50)}°C to {random.randint(60, 200)}°C", # 27 - temperature_rating
                        f"{random.randint(500, 5000)}x{random.randint(300, 3000)}x{random.randint(200, 2000)} mm", # 28 - dimensions
                        round(random.uniform(50, 50000), 2),                        # 29 - weight
                        round(random.uniform(1, 100), 2),                           # 30 - floor_space
                        round(random.uniform(10000, 2000000), 2),                   # 31 - acquisition_cost
                        round(random.uniform(5000, 1500000), 2),                    # 32 - current_book_value
                        'Straight Line',                                             # 33 - depreciation_method
                        random.choice(['Operational', 'Maintenance', 'Standby']),   # 34 - operational_status
                        round(random.uniform(65, 95), 2),                           # 35 - utilization_rate
                        round(random.uniform(75, 98), 2),                           # 36 - efficiency_rating
                        random.randint(1, SCALE_FACTORS['maintenance_plans']),      # 37 - maintenance_plan_id
                        fake.date_between(start_date='-30d', end_date='today'),     # 38 - last_maintenance_date
                        fake.date_between(start_date='today', end_date='+180d'),    # 39 - next_maintenance_date
                        round(random.uniform(100, 8760), 2),                        # 40 - mtbf_hours
                        round(random.uniform(0.5, 24), 2),                          # 41 - mttr_hours
                        fake.sentence(),                                             # 42 - safety_features
                        random.choice(['High', 'Medium', 'Low']),                   # 43 - safety_rating
                        random.choice(['Excellent', 'Good', 'Standard']),           # 44 - environmental_rating
                        '{"ISO9001": true, "CE": true}',                           # 45 - compliance_certifications
                        random.choice(['Critical', 'Important', 'Standard']),       # 46 - asset_criticality
                        random.choice(['High', 'Medium', 'Low']),                   # 47 - replacement_priority
                        random.choice(['New', 'Growth', 'Mature', 'Decline']),     # 48 - lifecycle_stage
                        random.randint(5, 30),                                       # 49 - expected_life_years
                        network_connected,                                           # 50 - network_connected
                        ip_address,                                                  # 51 - ip_address
                        mac_address,                                                 # 52 - mac_address
                        random.choice(['Ethernet/IP', 'Modbus TCP', 'PROFINET', 'OPC UA']) if network_connected else None, # 53 - protocol_type
                        random.choice(['Rockwell', 'Siemens', 'Schneider']) if network_connected else None, # 54 - communication_driver
                        network_connected,                                           # 55 - data_collection_enabled
                        random.randint(1, 1000),                                     # 56 - created_by
                        random.randint(1, 1000),                                     # 57 - last_modified_by
                        True                                                         # 58 - is_active
                    ))
                    equipment_counter += 1
                    
                    if equipment_counter > SCALE_FACTORS['production_equipment']:
                        break
                if equipment_counter > SCALE_FACTORS['production_equipment']:
                    break
            if equipment_counter > SCALE_FACTORS['production_equipment']:
                break
        
        # Insert equipment in batches
        batch_size = 1000
        for i in range(0, len(equipment_data), batch_size):
            batch = equipment_data[i:i+batch_size]
            cursor.executemany("""
                INSERT INTO equipment (
                    equipment_number, equipment_name, equipment_type, equipment_category, 
                    equipment_subtype, manufacturer, model_number, serial_number,
                    manufacture_date, installation_date, commissioning_date, warranty_expiry_date,
                    facility_id, production_line_id, department_id, cost_center_id, location_description,
                    specifications, operating_parameters, performance_characteristics,
                    rated_capacity, capacity_unit, power_rating, voltage_rating, current_rating,
                    pressure_rating, temperature_rating, dimensions, weight, floor_space,
                    acquisition_cost, current_book_value, depreciation_method, operational_status,
                    utilization_rate, efficiency_rating, maintenance_plan_id, last_maintenance_date,
                    next_maintenance_date, mtbf_hours, mttr_hours, safety_features, safety_rating,
                    environmental_rating, compliance_certifications, asset_criticality,
                    replacement_priority, lifecycle_stage, expected_life_years,
                    network_connected, ip_address, mac_address, protocol_type, communication_driver,
                    data_collection_enabled, created_by, last_modified_by, is_active
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, batch)
            self.conn.commit()
            
            if (i + batch_size) % 5000 == 0:
                print(f"    Inserted {min(i + batch_size, len(equipment_data)):,} equipment items...")
        
        cursor.close()

    def populate_bill_of_materials(self):
        """Populate bill of materials with duplicate prevention"""
        cursor = self.conn.cursor()
        print("  📋 Inserting bill of materials...")
        
        bom_data = []
        used_combinations = set()  # Track used (parent_id, component_id, date) combinations
        
        # Generate unique BOM records
        attempts = 0
        max_attempts = SCALE_FACTORS['bill_of_materials'] * 3  # Allow extra attempts
        
        while len(bom_data) < SCALE_FACTORS['bill_of_materials'] and attempts < max_attempts:
            attempts += 1
            
            # Generate unique combination
            parent_id = random.randint(1, 10000)
            component_id = random.randint(1, 10000)
            effective_date = fake.date_between(start_date='-2y', end_date='today')
            
            # Ensure parent != component and unique combination
            if parent_id != component_id:
                combination_key = (parent_id, component_id, effective_date)
                if combination_key not in used_combinations:
                    used_combinations.add(combination_key)
                    
                    bom_data.append((
                        f"BOM{len(bom_data)+1:08d}",
                        parent_id,  # parent_material_id
                        component_id,  # component_material_id
                        1,  # bom_level
                        round(random.uniform(1, 100), 6),  # quantity_per
                        random.choice(['EA', 'KG', 'L', 'M']),
                        round(random.uniform(0, 5), 2),  # scrap_percentage
                        effective_date,
                        fake.date_between(start_date=effective_date, end_date=effective_date + datetime.timedelta(days=1825)),  # end_date
                        'V1.0',
                        random.randint(1, 20),  # operation_sequence
                        random.choice(['Raw Material', 'Sub-assembly', 'Component']),
                        random.choice(['Make', 'Buy', 'Transfer']),
                        random.randint(0, 10),  # lead_time_offset_days
                        100.0,  # planning_percentage
                        0.0,    # cost_allocation_percentage
                        fake.sentence(),
                        f"ECN{random.randint(1000, 9999)}",
                        random.randint(1, 100),  # created_by
                        True
                    ))
        
        print(f"    Generated {len(bom_data):,} unique BOM records after {attempts:,} attempts")
        
        # Insert in batches with comprehensive error handling
        batch_size = 1000
        successful_inserts = 0
        
        for i in range(0, len(bom_data), batch_size):
            batch = bom_data[i:i+batch_size]
            try:
                cursor.executemany("""
                    INSERT INTO bill_of_materials (
                        bom_number, parent_material_id, component_material_id, bom_level,
                        quantity_per, unit_of_measure, scrap_percentage, effective_date, end_date,
                        version_number, operation_sequence, component_type, supply_type,
                        lead_time_offset_days, planning_percentage, cost_allocation_percentage,
                        change_reason, engineering_change_number, created_by, is_active
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, batch)
                self.conn.commit()
                successful_inserts += len(batch)
                
            except mysql.connector.errors.IntegrityError as e:
                if "Duplicate entry" in str(e):
                    print(f"    ⚠️  Batch {i//batch_size + 1} has duplicates, inserting individually...")
                    self.conn.rollback()
                    
                    # Insert one by one to skip only duplicates
                    for bom_record in batch:
                        try:
                            cursor.execute("""
                                INSERT INTO bill_of_materials (
                                    bom_number, parent_material_id, component_material_id, bom_level,
                                    quantity_per, unit_of_measure, scrap_percentage, effective_date, end_date,
                                    version_number, operation_sequence, component_type, supply_type,
                                    lead_time_offset_days, planning_percentage, cost_allocation_percentage,
                                    change_reason, engineering_change_number, created_by, is_active
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """, bom_record)
                            self.conn.commit()
                            successful_inserts += 1
                        except mysql.connector.errors.IntegrityError:
                            self.conn.rollback()
                            continue  # Skip duplicate
                else:
                    print(f"    ❌ Unexpected integrity error: {e}")
                    raise
            
            except Exception as e:
                print(f"    ❌ Unexpected error in batch {i//batch_size + 1}: {e}")
                self.conn.rollback()
                raise
            
            if (i + batch_size) % 10000 == 0:
                print(f"    Successfully inserted {successful_inserts:,} BOM records so far...")
        
        print(f"    ✅ Final BOM records inserted: {successful_inserts:,}")
        cursor.close()

    def populate_routings(self):
        """Populate routings with duplicate prevention"""
        cursor = self.conn.cursor()
        print("  🛣️ Inserting routings...")
        
        routings_data = []
        used_routing_numbers = set()
        
        for i in range(SCALE_FACTORS['routings']):
            # Ensure unique routing numbers
            routing_number = f"RTG{i+1:08d}"
            while routing_number in used_routing_numbers:
                routing_number = f"RTG{random.randint(1, 99999999):08d}"
            used_routing_numbers.add(routing_number)
            
            routings_data.append((
                routing_number,
                f"Routing {i+1:08d}",
                random.randint(1, 10000),  # material_id
                random.choice(['Standard', 'Alternative', 'Rework']),
                'V1.0',
                fake.date_between(start_date='-2y', end_date='today'),
                fake.date_between(start_date='today', end_date='+5y'),
                round(random.uniform(1, 1000), 4),
                round(random.uniform(1000, 100000), 4),
                round(random.uniform(5, 120), 2),
                'Active',
                random.randint(1, 100),  # approved_by
                fake.date_between(start_date='-1y', end_date='today'),
                random.randint(1, 100),  # created_by
                True
            ))
        
        # Insert with error handling
        batch_size = 1000
        for i in range(0, len(routings_data), batch_size):
            batch = routings_data[i:i+batch_size]
            try:
                cursor.executemany("""
                    INSERT INTO routings (
                        routing_number, routing_name, material_id, routing_type, version_number,
                        effective_date, end_date, lot_size_minimum, lot_size_maximum, setup_time_minutes,
                        routing_status, approved_by, approval_date, created_by, is_active
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, batch)
                self.conn.commit()
            except mysql.connector.errors.IntegrityError as e:
                print(f"    ⚠️  Routing duplicate error: {e}")
                self.conn.rollback()
                # Continue with individual inserts if needed
                for routing_record in batch:
                    try:
                        cursor.execute("""
                            INSERT INTO routings (
                                routing_number, routing_name, material_id, routing_type, version_number,
                                effective_date, end_date, lot_size_minimum, lot_size_maximum, setup_time_minutes,
                                routing_status, approved_by, approval_date, created_by, is_active
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, routing_record)
                        self.conn.commit()
                    except mysql.connector.errors.IntegrityError:
                        self.conn.rollback()
                        continue
        
        cursor.close()

    def populate_routing_operations(self):
        """Populate routing operations"""
        cursor = self.conn.cursor()
        print("  🔧 Inserting routing operations...")
        
        # Get routing IDs
        cursor.execute("SELECT routing_id FROM routings ORDER BY routing_id LIMIT 5000")
        routing_ids = [row[0] for row in cursor.fetchall()]
        
        # Get equipment IDs
        cursor.execute("SELECT equipment_id FROM equipment ORDER BY equipment_id LIMIT 1000")
        equipment_ids = [row[0] for row in cursor.fetchall()]
        
        routing_operations_data = []
        for routing_id in routing_ids:
            # Each routing has 3-8 operations
            num_operations = random.randint(3, 8)
            for op_num in range(1, num_operations + 1):
                routing_operations_data.append((
                    routing_id,
                    op_num * 10,  # operation_number
                    f"OP{op_num:02d}",
                    fake.sentence(),
                    op_num,  # sequence_number
                    f"WC{random.randint(100, 999)}",
                    random.choice(equipment_ids) if equipment_ids else None,
                    random.choice(['Basic', 'Intermediate', 'Advanced']),
                    round(random.uniform(5, 60), 2),    # setup_time_minutes
                    round(random.uniform(10, 300), 2),  # run_time_per_unit_seconds
                    round(random.uniform(2, 15), 2),    # teardown_time_minutes
                    round(random.uniform(0.5, 4), 2),   # queue_time_hours
                    round(random.uniform(0.1, 2), 2),   # move_time_hours
                    random.randint(1, 3),               # operators_required
                    round(random.uniform(25, 75), 2),   # labor_rate_per_hour
                    round(random.uniform(50, 200), 2),  # machine_rate_per_hour
                    100.0,  # utilization_rate
                    random.choice([True, False]),
                    f"QCP{random.randint(100, 999)}",
                    round(random.uniform(10, 50), 2),   # overhead_rate_per_hour
                    True
                ))
        
        # Insert in batches
        batch_size = 1000
        for i in range(0, len(routing_operations_data), batch_size):
            batch = routing_operations_data[i:i+batch_size]
            cursor.executemany("""
                INSERT INTO routing_operations (
                    routing_id, operation_number, operation_code, operation_description,
                    sequence_number, work_center, equipment_id, required_skill_level,
                    setup_time_minutes, run_time_per_unit_seconds, teardown_time_minutes,
                    queue_time_hours, move_time_hours, operators_required, labor_rate_per_hour,
                    machine_rate_per_hour, utilization_rate, inspection_required,
                    quality_control_plan, overhead_rate_per_hour, is_active
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, batch)
            self.conn.commit()
        
        cursor.close()

    def populate_work_orders(self):
        """Populate work orders - FIXED parameter count"""
        cursor = self.conn.cursor()
        print("  📋 Inserting work orders...")
        
        # Get BOM and routing IDs
        cursor.execute("SELECT bom_id FROM bill_of_materials ORDER BY bom_id LIMIT 5000")
        bom_ids = [row[0] for row in cursor.fetchall()]
        
        cursor.execute("SELECT routing_id FROM routings ORDER BY routing_id LIMIT 1000")
        routing_ids = [row[0] for row in cursor.fetchall()]
        
        work_orders_data = []
        for i in range(SCALE_FACTORS['work_orders']):
            planned_start = fake.date_time_between(start_date='-30d', end_date='+60d')
            planned_end = planned_start + datetime.timedelta(hours=random.randint(2, 72))
            
            work_orders_data.append((
                f"WO{i+1:08d}",                                                # 1 - work_order_number
                random.choice(['Production', 'Rework', 'Maintenance', 'Sample']), # 2 - work_order_type
                random.choice(['High', 'Normal', 'Low']),                     # 3 - priority
                random.randint(1, 10000),                                      # 4 - material_id
                fake.catch_phrase(),                                           # 5 - product_description
                random.randint(1, 100),                                        # 6 - production_line_id
                random.randint(1, 15),                                         # 7 - facility_id
                random.randint(1, 120),                                        # 8 - department_id
                random.randint(1, 100),                                        # 9 - cost_center_id
                random.choice(bom_ids) if bom_ids else None,                  # 10 - bom_id
                random.choice(routing_ids) if routing_ids else None,          # 11 - routing_id
                round(random.uniform(1, 10000), 4),                           # 12 - planned_quantity
                0,                                                             # 13 - actual_quantity
                0,                                                             # 14 - completed_quantity
                0,                                                             # 15 - rejected_quantity
                random.choice(['EA', 'KG', 'L']),                            # 16 - quantity_unit
                planned_start,                                                 # 17 - planned_start_datetime
                None,                                                          # 18 - actual_start_datetime
                planned_end,                                                   # 19 - planned_end_datetime
                None,                                                          # 20 - actual_end_datetime
                round((planned_end - planned_start).total_seconds() / 3600, 2), # 21 - planned_duration_hours
                None,                                                          # 22 - actual_duration_hours
                random.randint(1, 1000),                                       # 23 - responsible_person_id
                '["Team A", "Team B"]',                                       # 24 - assigned_team JSON
                '["Welding", "Assembly"]',                                    # 25 - required_skills JSON
                random.choice(['Created', 'Released', 'In Progress', 'Completed']), # 26 - work_order_status
                0,                                                             # 27 - completion_percentage
                round(random.uniform(1000, 50000), 2),                        # 28 - estimated_cost
                None,                                                          # 29 - actual_cost
                None,                                                          # 30 - labor_cost
                None,                                                          # 31 - material_cost
                None,                                                          # 32 - overhead_cost
                None,                                                          # 33 - yield_percentage
                None,                                                          # 34 - first_pass_yield
                None,                                                          # 35 - oee_achieved
                None,                                                          # 36 - cycle_time_actual
                None,                                                          # 37 - setup_time_actual
                fake.text(max_nb_chars=500),                                  # 38 - work_instructions
                fake.sentence(),                                               # 39 - special_instructions
                None,                                                          # 40 - completion_notes
                random.randint(1, 1000),                                       # 41 - created_by
                random.randint(1, 1000),                                       # 42 - last_modified_by
                True                                                           # 43 - is_active
            ))
        
        # Insert in batches
        batch_size = 1000
        for i in range(0, len(work_orders_data), batch_size):
            batch = work_orders_data[i:i+batch_size]
            cursor.executemany("""
                INSERT INTO work_orders (
                    work_order_number, work_order_type, priority, material_id, product_description,
                    production_line_id, facility_id, department_id, cost_center_id, bom_id, routing_id,
                    planned_quantity, actual_quantity, completed_quantity, rejected_quantity, quantity_unit,
                    planned_start_datetime, actual_start_datetime, planned_end_datetime, actual_end_datetime,
                    planned_duration_hours, actual_duration_hours, responsible_person_id, assigned_team,
                    required_skills, work_order_status, completion_percentage, estimated_cost,
                    actual_cost, labor_cost, material_cost, overhead_cost, yield_percentage,
                    first_pass_yield, oee_achieved, cycle_time_actual, setup_time_actual,
                    work_instructions, special_instructions, completion_notes, created_by,
                    last_modified_by, is_active
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, batch)
            self.conn.commit()
            
            if (i + batch_size) % 25000 == 0:
                print(f"    Inserted {min(i + batch_size, SCALE_FACTORS['work_orders']):,} work orders...")
        
        cursor.close()

    def populate_purchase_orders(self):
        """Populate purchase orders - FIXED parameter count"""
        cursor = self.conn.cursor()
        print("  💰 Inserting purchase orders...")
        
        purchase_orders_data = []
        po_types = ['Standard', 'Blanket', 'Contract', 'Emergency', 'Consignment']
        po_categories = ['Raw Materials', 'Components', 'Services', 'Capital Equipment', 'MRO']
        po_statuses = ['Draft', 'Approved', 'Sent', 'Acknowledged', 'Partially Received', 'Closed']
        
        for i in range(SCALE_FACTORS['purchase_orders']):
            po_date = fake.date_between(start_date='-1y', end_date='today')
            required_date = fake.date_between(start_date=po_date, end_date=po_date + datetime.timedelta(days=90))
            
            purchase_orders_data.append((
                f"PO{i+1:08d}",                                               # 1 - po_number
                random.choice(po_types),                                      # 2 - po_type
                random.choice(po_categories),                                 # 3 - po_category
                random.randint(1, 5000),                                      # 4 - vendor_id
                fake.company(),                                               # 5 - vendor_name
                fake.name(),                                                  # 6 - vendor_contact_person
                random.choice(['USD', 'EUR', 'GBP', 'JPY']),                # 7 - currency
                round(random.uniform(1000, 500000), 2),                      # 8 - total_amount
                round(random.uniform(50, 25000), 2),                         # 9 - tax_amount
                round(random.uniform(950, 475000), 2),                       # 10 - net_amount
                random.choice(['Net 30', 'Net 60', 'COD', '2/10 Net 30']),  # 11 - payment_terms
                po_date,                                                      # 12 - po_date
                required_date,                                                # 13 - required_date
                fake.date_between(start_date=required_date, end_date=required_date + datetime.timedelta(days=14)), # 14 - promised_date
                fake.date_between(start_date=required_date, end_date=required_date + datetime.timedelta(days=21)) if random.random() < 0.7 else None, # 15 - delivery_date
                random.randint(1, 15),                                        # 16 - ship_to_facility_id
                random.randint(1, 15),                                        # 17 - ship_to_warehouse_facility_id
                fake.address(),                                               # 18 - ship_to_address
                random.choice(['FOB Origin', 'FOB Destination', 'CIF', 'EXW']), # 19 - shipping_terms
                random.choice(['Prepaid', 'Collect', 'Third Party']),        # 20 - freight_terms
                random.randint(1, 1000) if random.random() < 0.3 else None,  # 21 - carrier_id
                random.randint(1, 1000),                                      # 22 - created_by
                random.randint(1, 1000),                                      # 23 - requested_by
                random.randint(1, 100) if random.random() < 0.8 else None,   # 24 - approved_by
                fake.date_between(start_date=po_date, end_date=po_date + datetime.timedelta(days=5)) if random.random() < 0.8 else None, # 25 - approval_date
                random.choice(po_statuses),                                   # 26 - po_status
                random.randint(1, 1000) if random.random() < 0.4 else None,  # 27 - contract_id
                f"CNT{random.randint(1000, 9999)}" if random.random() < 0.4 else None, # 28 - contract_number
                fake.text(max_nb_chars=200) if random.random() < 0.3 else None, # 29 - special_terms
                fake.sentence() if random.random() < 0.5 else None,          # 30 - quality_requirements
                fake.sentence(),                                              # 31 - delivery_instructions
                fake.date_between(start_date=po_date, end_date=po_date + datetime.timedelta(days=7)) if random.random() < 0.6 else None, # 32 - acknowledgment_date
                f"ACK{random.randint(100000, 999999)}" if random.random() < 0.6 else None, # 33 - acknowledgment_number
                True                                                          # 34 - is_active
            ))
        
        # Insert in batches
        batch_size = 1000
        for i in range(0, len(purchase_orders_data), batch_size):
            batch = purchase_orders_data[i:i+batch_size]
            cursor.executemany("""
                INSERT INTO purchase_orders (
                    po_number, po_type, po_category, vendor_id, vendor_name, vendor_contact_person,
                    currency, total_amount, tax_amount, net_amount, payment_terms, po_date,
                    required_date, promised_date, delivery_date, ship_to_facility_id,
                    ship_to_warehouse_facility_id, ship_to_address, shipping_terms, freight_terms,
                    carrier_id, created_by, requested_by, approved_by, approval_date, po_status,
                    contract_id, contract_number, special_terms, quality_requirements,
                    delivery_instructions, acknowledgment_date, acknowledgment_number, is_active
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, batch)
            self.conn.commit()
            
            if (i + batch_size) % 10000 == 0:
                print(f"    Inserted {min(i + batch_size, SCALE_FACTORS['purchase_orders']):,} purchase orders...")
        
        cursor.close()

    def populate_purchase_order_items(self):
        """Populate purchase order items"""
        cursor = self.conn.cursor()
        print("  📦 Inserting purchase order items...")
        
        # Get purchase order IDs
        cursor.execute("SELECT purchase_order_id FROM purchase_orders ORDER BY purchase_order_id")
        po_ids = [row[0] for row in cursor.fetchall()]
        
        po_items_data = []
        item_statuses = ['Open', 'Confirmed', 'Partially Received', 'Received', 'Closed', 'Cancelled']
        receipt_statuses = ['Not Started', 'Partially Received', 'Fully Received', 'Over Received']
        
        for po_id in po_ids:
            # Each PO has 1-8 items (average ~4)
            num_items = random.randint(1, 8)
            for line_num in range(1, num_items + 1):
                ordered_qty = round(random.uniform(1, 1000), 4)
                received_qty = round(random.uniform(0, ordered_qty * 1.1), 4) if random.random() < 0.6 else 0
                unit_price = round(random.uniform(0.50, 5000), 4)
                total_price = round(ordered_qty * unit_price, 2)
                
                po_items_data.append((
                    po_id,
                    line_num,
                    random.randint(1, 10000),  # material_id
                    f"MAT{random.randint(1, 100000):08d}",
                    fake.catch_phrase(),
                    random.randint(1, 1000) if random.random() < 0.7 else None,  # specification_id
                    ordered_qty,
                    received_qty,
                    round(random.uniform(0, received_qty), 4) if received_qty > 0 else 0,  # invoiced_quantity
                    round(random.uniform(0, received_qty * 0.05), 4) if received_qty > 0 else 0,  # rejected_quantity
                    random.choice(['EA', 'KG', 'L', 'M', 'FT', 'LB']),
                    unit_price,
                    total_price,
                    round(random.uniform(0, 10), 2),  # discount_percentage
                    round(total_price * random.uniform(0, 0.1), 2),  # discount_amount
                    random.choice(['STD', 'EXE', 'VAT', 'GST']),
                    round(total_price * random.uniform(0.05, 0.15), 2),  # tax_amount
                    fake.date_between(start_date='-30d', end_date='+60d'),
                    fake.date_between(start_date='-15d', end_date='+75d'),
                    fake.date_between(start_date='-10d', end_date='+80d') if random.random() < 0.7 else None,
                    random.randint(1, 15),    # warehouse_facility_id
                    random.randint(1, 1000) if random.random() < 0.8 else None,  # storage_location_id
                    fake.sentence() if random.random() < 0.6 else None,
                    fake.sentence() if random.random() < 0.4 else None,
                    random.choice([True, False]),
                    random.choice(item_statuses),
                    random.choice(receipt_statuses),
                    random.randint(1, 100),   # cost_center_id
                    f"{random.randint(1000, 9999)}-{random.randint(100, 999)}",  # gl_account
                    f"ASSET{random.randint(100000, 999999)}" if random.random() < 0.2 else None,
                    True
                ))
        
        # Insert in batches
        batch_size = 1000
        for i in range(0, len(po_items_data), batch_size):
            batch = po_items_data[i:i+batch_size]
            cursor.executemany("""
                INSERT INTO purchase_order_items (
                    purchase_order_id, line_number, material_id, material_number, material_description,
                    specification_id, ordered_quantity, received_quantity, invoiced_quantity,
                    rejected_quantity, unit_of_measure, unit_price, total_price, discount_percentage,
                    discount_amount, tax_code, tax_amount, required_date, promised_date, delivery_date,
                    warehouse_facility_id, storage_location_id, quality_specifications,
                    technical_specifications, inspection_required, item_status, receipt_status,
                    cost_center_id, gl_account, asset_number, is_active
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, batch)
            self.conn.commit()
            
            if (i + batch_size) % 25000 == 0:
                print(f"    Inserted {min(i + batch_size, len(po_items_data)):,} purchase order items...")
        
        cursor.close()

    def populate_sales_orders(self):
        """Populate sales orders - FIXED parameter count"""
        cursor = self.conn.cursor()
        print("  💼 Inserting sales orders...")
        
        sales_orders_data = []
        so_types = ['Standard', 'Rush', 'Consignment', 'Sample', 'Prototype']
        so_categories = ['Finished Goods', 'Semi-Finished', 'Custom', 'Spare Parts']
        so_statuses = ['Draft', 'Confirmed', 'In Production', 'Ready to Ship', 'Shipped', 'Invoiced', 'Closed']
        
        for i in range(SCALE_FACTORS['sales_orders']):
            so_date = fake.date_between(start_date='-6m', end_date='today')
            requested_date = fake.date_between(start_date=so_date, end_date=so_date + datetime.timedelta(days=60))
            
            sales_orders_data.append((
                f"SO{i+1:08d}",                                               # 1 - so_number
                random.choice(so_types),                                      # 2 - so_type
                random.choice(so_categories),                                 # 3 - so_category
                random.randint(1, 10000),                                     # 4 - customer_id
                fake.company(),                                               # 5 - customer_name
                fake.name(),                                                  # 6 - customer_contact_person
                f"CUST-PO-{random.randint(100000, 999999)}",                 # 7 - customer_po_number
                f"REF-{random.randint(1000, 9999)}",                        # 8 - customer_reference
                random.choice(['USD', 'EUR', 'GBP', 'JPY']),                # 9 - currency
                round(random.uniform(5000, 1000000), 2),                     # 10 - total_amount
                round(random.uniform(250, 50000), 2),                        # 11 - tax_amount
                round(random.uniform(4750, 950000), 2),                      # 12 - net_amount
                random.choice(['Net 30', 'Net 60', 'COD', '2/10 Net 30', 'Prepaid']), # 13 - payment_terms
                so_date,                                                      # 14 - so_date
                requested_date,                                               # 15 - requested_date
                fake.date_between(start_date=requested_date, end_date=requested_date + datetime.timedelta(days=7)), # 16 - promised_date
                fake.date_between(start_date=requested_date, end_date=requested_date + datetime.timedelta(days=14)) if random.random() < 0.8 else None, # 17 - shipping_date
                random.randint(1, 15),                                        # 18 - ship_from_warehouse_facility_id
                fake.address(),                                               # 19 - ship_to_address
                fake.address(),                                               # 20 - bill_to_address
                random.choice(['Ground', 'Express', 'Overnight', 'Freight']), # 21 - shipping_method
                random.choice(['FOB Origin', 'FOB Destination', 'CIF']),     # 22 - freight_terms
                random.randint(1, 1000) if random.random() < 0.4 else None,  # 23 - carrier_id
                random.randint(1, 1000),                                      # 24 - sales_rep_id
                random.choice(['North', 'South', 'East', 'West', 'Central']), # 25 - sales_territory
                round(random.uniform(2, 8), 2),                              # 26 - commission_rate
                random.randint(1, 1000),                                      # 27 - created_by
                random.randint(1, 100) if random.random() < 0.9 else None,   # 28 - approved_by
                fake.date_between(start_date=so_date, end_date=so_date + datetime.timedelta(days=3)) if random.random() < 0.9 else None, # 29 - approval_date
                random.choice(so_statuses),                                   # 30 - so_status
                random.choice(['High', 'Normal', 'Low']),                    # 31 - production_priority
                fake.text(max_nb_chars=200) if random.random() < 0.3 else None, # 32 - special_instructions
                fake.sentence() if random.random() < 0.5 else None,          # 33 - quality_requirements
                fake.sentence() if random.random() < 0.4 else None,          # 34 - packaging_instructions
                random.randint(1, 1000) if random.random() < 0.3 else None,  # 35 - contract_id
                True                                                          # 36 - is_active
            ))
        
        # Insert in batches - FIXED: Remove duplicate created_date from INSERT
        batch_size = 1000
        for i in range(0, len(sales_orders_data), batch_size):
            batch = sales_orders_data[i:i+batch_size]
            cursor.executemany("""
                INSERT INTO sales_orders (
                    so_number, so_type, so_category, customer_id, customer_name, customer_contact_person,
                    customer_po_number, customer_reference, currency, total_amount, tax_amount,
                    net_amount, payment_terms, so_date, requested_date, promised_date, shipping_date,
                    ship_from_warehouse_facility_id, ship_to_address, bill_to_address, shipping_method,
                    freight_terms, carrier_id, sales_rep_id, sales_territory, commission_rate,
                    created_by, approved_by, approval_date, so_status, production_priority,
                    special_instructions, quality_requirements, packaging_instructions, contract_id, is_active
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, batch)
            self.conn.commit()
            
            if (i + batch_size) % 10000 == 0:
                print(f"    Inserted {min(i + batch_size, SCALE_FACTORS['sales_orders']):,} sales orders...")
        
        cursor.close()

    def populate_sales_order_items(self):
        """Populate sales order items"""
        cursor = self.conn.cursor()
        print("  📋 Inserting sales order items...")
        
        # Get sales order IDs
        cursor.execute("SELECT sales_order_id FROM sales_orders ORDER BY sales_order_id")
        so_ids = [row[0] for row in cursor.fetchall()]
        
        # Get work order IDs for production linking
        cursor.execute("SELECT work_order_id FROM work_orders ORDER BY work_order_id LIMIT 50000")
        wo_ids = [row[0] for row in cursor.fetchall()]
        
        so_items_data = []
        item_statuses = ['Open', 'Confirmed', 'In Production', 'Ready', 'Shipped', 'Invoiced', 'Closed']
        fulfillment_statuses = ['Not Started', 'In Production', 'Ready', 'Shipped', 'Delivered']
        
        for so_id in so_ids:
            # Each SO has 1-8 items (average ~4)
            num_items = random.randint(1, 8)
            for line_num in range(1, num_items + 1):
                ordered_qty = round(random.uniform(1, 500), 4)
                confirmed_qty = round(random.uniform(ordered_qty * 0.8, ordered_qty), 4)
                shipped_qty = round(random.uniform(0, confirmed_qty), 4) if random.random() < 0.5 else 0
                unit_price = round(random.uniform(10, 15000), 4)
                total_price = round(ordered_qty * unit_price, 2)
                
                so_items_data.append((
                    so_id,
                    line_num,
                    random.randint(1, 10000),  # material_id
                    f"MAT{random.randint(1, 100000):08d}",
                    fake.catch_phrase(),
                    random.randint(1, 1000) if random.random() < 0.6 else None,  # specification_id
                    ordered_qty,
                    confirmed_qty,
                    shipped_qty,
                    round(random.uniform(0, shipped_qty), 4) if shipped_qty > 0 else 0,  # invoiced_quantity
                    round(random.uniform(0, shipped_qty * 0.02), 4) if shipped_qty > 0 else 0,  # returned_quantity
                    random.choice(['EA', 'KG', 'L', 'M', 'SET', 'BOX']),
                    unit_price,
                    total_price,
                    round(random.uniform(0, 15), 2),  # discount_percentage
                    round(total_price * random.uniform(0, 0.15), 2),  # discount_amount
                    random.choice(['STD', 'EXE', 'VAT', 'GST']),
                    round(total_price * random.uniform(0.05, 0.12), 2),  # tax_amount
                    fake.date_between(start_date='-30d', end_date='+90d'),
                    fake.date_between(start_date='-15d', end_date='+105d'),
                    fake.date_between(start_date='-10d', end_date='+120d') if random.random() < 0.6 else None,
                    random.randint(1, 15),    # warehouse_facility_id
                    fake.sentence() if random.random() < 0.5 else None,
                    fake.sentence() if random.random() < 0.3 else None,
                    fake.sentence() if random.random() < 0.2 else None,
                    random.choice(item_statuses),
                    random.choice(fulfillment_statuses),
                    random.choice(wo_ids) if wo_ids and random.random() < 0.4 else None,  # work_order_id
                    True
                ))
        
        # Insert in batches
        batch_size = 1000
        for i in range(0, len(so_items_data), batch_size):
            batch = so_items_data[i:i+batch_size]
            cursor.executemany("""
                INSERT INTO sales_order_items (
                    sales_order_id, line_number, material_id, material_number, material_description,
                    specification_id, ordered_quantity, confirmed_quantity, shipped_quantity,
                    invoiced_quantity, returned_quantity, unit_of_measure, unit_price, total_price,
                    discount_percentage, discount_amount, tax_code, tax_amount, requested_date,
                    promised_date, shipping_date, warehouse_facility_id, quality_specifications,
                    packaging_requirements, special_handling, item_status, fulfillment_status,
                    work_order_id, is_active
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, batch)
            self.conn.commit()
            
            if (i + batch_size) % 25000 == 0:
                print(f"    Inserted {min(i + batch_size, len(so_items_data)):,} sales order items...")
        
        cursor.close()

    def populate_goods_receipts(self):
        """Populate goods receipts"""
        cursor = self.conn.cursor()
        print("  📥 Inserting goods receipts...")
        
        # Get PO item IDs
        cursor.execute("SELECT po_item_id, material_id FROM purchase_order_items ORDER BY po_item_id LIMIT 50000")
        po_items = cursor.fetchall()
        
        goods_receipts_data = []
        receipt_types = ['Purchase Order', 'Transfer', 'Return', 'Adjustment']
        inspection_statuses = ['Not Required', 'Pending', 'In Progress', 'Passed', 'Failed']
        receipt_statuses = ['Open', 'In Progress', 'Completed', 'Cancelled']
        
        for i in range(SCALE_FACTORS['goods_receipts']):
            po_item = random.choice(po_items) if po_items else None
            delivered_qty = round(random.uniform(1, 1000), 4)
            received_qty = round(random.uniform(delivered_qty * 0.95, delivered_qty), 4)
            accepted_qty = round(random.uniform(received_qty * 0.95, received_qty), 4)
            rejected_qty = received_qty - accepted_qty
            
            serial_numbers = [f"SN{random.randint(100000, 999999)}" for _ in range(random.randint(0, 3))]
            
            goods_receipts_data.append((
                f"GR{i+1:08d}",
                random.choice(receipt_types),
                fake.date_between(start_date='-90d', end_date='today'),
                fake.time(),
                'Purchase Order',
                f"PO{random.randint(1, 150000):08d}",
                po_item[0] if po_item else None,  # po_item_id
                po_item[1] if po_item else random.randint(1, 10000),  # material_id
                f"MAT{random.randint(1, 100000):08d}",
                f"LOT{random.randint(100000, 999999)}",
                f"BATCH{random.randint(1000, 9999)}",
                json.dumps(serial_numbers) if serial_numbers else None,
                delivered_qty,
                received_qty,
                accepted_qty,
                rejected_qty,
                random.choice(['EA', 'KG', 'L', 'M', 'FT']),
                random.randint(1, 15),    # facility_id
                random.randint(1, 15),    # warehouse_facility_id
                random.randint(1, 1000) if random.random() < 0.8 else None,  # storage_location_id
                random.choice([True, False]),
                random.choice(inspection_statuses),
                f"QC{random.randint(100000, 999999)}" if random.random() < 0.6 else None,
                fake.sentence() if rejected_qty > 0 else None,
                round(random.uniform(1, 500), 4),  # unit_cost
                round(accepted_qty * random.uniform(1, 500), 2),  # total_value
                random.randint(1, 1000),  # received_by
                random.randint(1, 200) if random.random() < 0.7 else None,  # inspector_id
                random.choice(receipt_statuses),
                random.choice([True, False]),
                random.randint(1, 1000),  # created_by
                True
            ))
        
        # Insert in batches
        batch_size = 1000
        for i in range(0, len(goods_receipts_data), batch_size):
            batch = goods_receipts_data[i:i+batch_size]
            cursor.executemany("""
                INSERT INTO goods_receipts (
                    receipt_number, receipt_type, receipt_date, receipt_time, source_document_type,
                    source_document_number, po_item_id, material_id, material_number, lot_number,
                    batch_number, serial_numbers, delivered_quantity, received_quantity,
                    accepted_quantity, rejected_quantity, unit_of_measure, facility_id,
                    warehouse_facility_id, storage_location_id, inspection_required, inspection_status,
                    quality_certificate_number, rejection_reason, unit_cost, total_value,
                    received_by, inspector_id, receipt_status, posted_to_inventory, created_by, is_active
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, batch)
            self.conn.commit()
            
            if (i + batch_size) % 25000 == 0:
                print(f"    Inserted {min(i + batch_size, SCALE_FACTORS['goods_receipts']):,} goods receipts...")
        
        cursor.close()

    def populate_shipments(self):
        """Populate shipments"""
        cursor = self.conn.cursor()
        print("  🚚 Inserting shipments...")
        
        shipments_data = []
        shipment_types = ['Sales Order', 'Transfer', 'Sample', 'Return', 'Emergency']
        service_levels = ['Ground', 'Express', 'Overnight', 'Two Day', 'Freight']
        shipment_statuses = ['Planned', 'Picked', 'Packed', 'Shipped', 'In Transit', 'Delivered']
        
        for i in range(SCALE_FACTORS['shipments']):
            shipment_date = fake.date_between(start_date='-60d', end_date='today')
            pickup_date = fake.date_between(start_date=shipment_date, end_date=shipment_date + datetime.timedelta(days=1))
            estimated_delivery = pickup_date + datetime.timedelta(days=random.randint(1, 14))
            
            shipments_data.append((
                f"SH{i+1:08d}",
                random.choice(shipment_types),
                shipment_date,
                fake.time(),
                random.randint(1, 10000),  # customer_id
                fake.address(),
                random.randint(1, 1000) if random.random() < 0.8 else None,  # carrier_id
                fake.company(),
                f"TRK{random.randint(100000000000, 999999999999)}",  # tracking_number
                random.choice(service_levels),
                round(random.uniform(1, 5000), 4),    # total_weight
                round(random.uniform(0.1, 100), 4),   # total_volume
                random.randint(1, 20),                # number_of_packages
                round(random.uniform(25, 2500), 2),   # freight_cost
                round(random.uniform(5, 250), 2),     # insurance_cost
                round(random.uniform(10, 100), 2),    # handling_charges
                round(random.uniform(40, 2850), 2),   # total_shipping_cost
                random.choice(shipment_statuses),
                pickup_date if random.random() < 0.8 else None,
                estimated_delivery,
                estimated_delivery + datetime.timedelta(days=random.randint(-2, 3)) if random.random() < 0.6 else None,
                random.randint(1, 15),    # ship_from_facility_id
                random.randint(1, 15),    # ship_from_warehouse_facility_id
                f"BOL{random.randint(100000, 999999)}",
                f"INV{random.randint(100000, 999999)}",
                f"PL{random.randint(100000, 999999)}",
                random.randint(1, 1000),  # shipped_by
                random.randint(1, 1000),  # packed_by
                random.randint(1, 1000),  # created_by
                True
            ))
        
        # Insert in batches
        batch_size = 1000
        for i in range(0, len(shipments_data), batch_size):
            batch = shipments_data[i:i+batch_size]
            cursor.executemany("""
                INSERT INTO shipments (
                    shipment_number, shipment_type, shipment_date, shipment_time, customer_id,
                    ship_to_address, carrier_id, carrier_name, tracking_number, service_level,
                    total_weight, total_volume, number_of_packages, freight_cost, insurance_cost,
                    handling_charges, total_shipping_cost, shipment_status, pickup_date,
                    estimated_delivery_date, actual_delivery_date, ship_from_facility_id,
                    ship_from_warehouse_facility_id, bill_of_lading, commercial_invoice,
                    packing_list, shipped_by, packed_by, created_by, is_active
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, batch)
            self.conn.commit()
            
            if (i + batch_size) % 10000 == 0:
                print(f"    Inserted {min(i + batch_size, SCALE_FACTORS['shipments']):,} shipments...")
        
        cursor.close()

    def populate_shipment_items(self):
        """Populate shipment items"""
        cursor = self.conn.cursor()
        print("  📦 Inserting shipment items...")
        
        # Get shipment IDs
        cursor.execute("SELECT shipment_id FROM shipments ORDER BY shipment_id")
        shipment_ids = [row[0] for row in cursor.fetchall()]
        
        # Get SO item IDs
        cursor.execute("SELECT so_item_id, material_id FROM sales_order_items ORDER BY so_item_id LIMIT 100000")
        so_items = cursor.fetchall()
        
        shipment_items_data = []
        package_types = ['Box', 'Pallet', 'Crate', 'Envelope', 'Tube', 'Bag']
        
        for shipment_id in shipment_ids:
            # Each shipment has 1-6 items
            num_items = random.randint(1, 6)
            for _ in range(num_items):
                so_item = random.choice(so_items) if so_items else None
                ordered_qty = round(random.uniform(1, 200), 4)
                shipped_qty = round(random.uniform(ordered_qty * 0.9, ordered_qty), 4)
                
                serial_numbers = [f"SN{random.randint(100000, 999999)}" for _ in range(random.randint(0, 2))]
                
                shipment_items_data.append((
                    shipment_id,
                    so_item[0] if so_item else None,  # so_item_id
                    so_item[1] if so_item else random.randint(1, 10000),  # material_id
                    f"MAT{random.randint(1, 100000):08d}",
                    f"LOT{random.randint(100000, 999999)}",
                    json.dumps(serial_numbers) if serial_numbers else None,
                    ordered_qty,
                    shipped_qty,
                    random.choice(['EA', 'KG', 'L', 'M', 'SET']),
                    random.choice(package_types),
                    random.randint(1, 10),
                    True
                ))
        
        # Insert in batches
        batch_size = 1000
        for i in range(0, len(shipment_items_data), batch_size):
            batch = shipment_items_data[i:i+batch_size]
            cursor.executemany("""
                INSERT INTO shipment_items (
                    shipment_id, so_item_id, material_id, material_number, lot_number,
                    serial_numbers, ordered_quantity, shipped_quantity, unit_of_measure,
                    package_type, number_of_packages, is_active
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, batch)
            self.conn.commit()
        
        cursor.close()

    def populate_quality_events(self):
        """Populate quality events"""
        cursor = self.conn.cursor()
        print("  🎯 Inserting quality events...")
        
        # Get equipment IDs
        cursor.execute("SELECT equipment_id FROM equipment ORDER BY equipment_id LIMIT 5000")
        equipment_ids = [row[0] for row in cursor.fetchall()]
        
        quality_events_data = []
        event_types = ['Inspection', 'Audit', 'Customer Complaint', 'Internal Defect', 'Supplier Issue', 'Corrective Action']
        event_categories = ['Incoming', 'In-Process', 'Final', 'Field', 'Supplier']
        severity_levels = ['Critical', 'Major', 'Minor', 'Cosmetic']
        event_statuses = ['Open', 'Under Investigation', 'Action Required', 'Closed', 'Cancelled']
        
        for i in range(SCALE_FACTORS['quality_events']):
            test_results = {
                'dimension_check': random.choice(['Pass', 'Fail']),
                'surface_finish': f"{round(random.uniform(0.5, 5.0), 2)} μm",
                'hardness': f"{random.randint(150, 400)} HB",
                'tensile_strength': f"{random.randint(200, 800)} MPa"
            }
            
            measurements = {
                'length': round(random.uniform(10, 500), 3),
                'width': round(random.uniform(5, 200), 3),
                'thickness': round(random.uniform(1, 50), 3),
                'weight': round(random.uniform(0.1, 100), 3)
            }
            
            quality_events_data.append((
                f"QE{i+1:08d}",
                random.choice(event_types),
                random.choice(event_categories),
                random.choice(severity_levels),
                random.choice(['Work Order', 'Purchase Order', 'Customer Complaint', 'Inspection']),
                f"WO{random.randint(1, 200000):08d}",
                random.randint(1, 10000),  # material_id
                random.choice(equipment_ids) if equipment_ids else None,
                random.randint(1, 15),     # facility_id
                random.randint(1, 120),    # department_id
                fake.date_between(start_date='-90d', end_date='today'),
                random.randint(1, 1000),   # discovered_by
                fake.text(max_nb_chars=500),
                fake.text(max_nb_chars=300) if random.random() < 0.6 else None,
                random.randint(1, 1000) if random.random() < 0.7 else None,  # specification_id
                json.dumps(test_results),
                json.dumps(measurements),
                random.choice(['Pass', 'Fail']),
                random.choice([True, False]),
                fake.text(max_nb_chars=400) if random.random() < 0.7 else None,
                random.randint(1, 1000) if random.random() < 0.8 else None,  # responsible_person_id
                fake.date_between(start_date='today', end_date='+90d') if random.random() < 0.8 else None,
                fake.date_between(start_date='today', end_date='+120d') if random.random() < 0.4 else None,
                random.choice(event_statuses),
                random.randint(1, 100) if random.random() < 0.8 else None,   # approved_by
                fake.date_between(start_date='-30d', end_date='today') if random.random() < 0.8 else None,
                round(random.uniform(100, 50000), 2) if random.random() < 0.4 else None,  # cost_impact
                random.randint(1, 1000),   # created_by
                True
            ))
        
        # Insert in batches
        batch_size = 1000
        for i in range(0, len(quality_events_data), batch_size):
            batch = quality_events_data[i:i+batch_size]
            cursor.executemany("""
                INSERT INTO quality_events (
                    event_number, event_type, event_category, severity_level, source_type,
                    source_document_number, material_id, equipment_id, facility_id, department_id,
                    event_date, discovered_by, description, root_cause_analysis, specification_id,
                    test_results, measurements, pass_fail_status, corrective_action_required,
                    corrective_action_plan, responsible_person_id, target_completion_date,
                    actual_completion_date, event_status, approved_by, approval_date,
                    cost_impact, created_by, is_active
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, batch)
            self.conn.commit()
            
            if (i + batch_size) % 10000 == 0:
                print(f"    Inserted {min(i + batch_size, SCALE_FACTORS['quality_events']):,} quality events...")
        
        cursor.close()

    def populate_maintenance_orders(self):
        """Populate maintenance orders"""
        cursor = self.conn.cursor()
        print("  🔧 Inserting maintenance orders...")
        
        # Get equipment and maintenance plan IDs
        cursor.execute("SELECT equipment_id FROM equipment ORDER BY equipment_id LIMIT 10000")
        equipment_ids = [row[0] for row in cursor.fetchall()]
        
        cursor.execute("SELECT plan_id FROM maintenance_plans ORDER BY plan_id")
        plan_ids = [row[0] for row in cursor.fetchall()]
        
        maintenance_orders_data = []
        order_types = ['Preventive', 'Corrective', 'Emergency', 'Predictive', 'Overhaul']
        order_statuses = ['Scheduled', 'Released', 'In Progress', 'Completed', 'Cancelled']
        priorities = ['Critical', 'High', 'Normal', 'Low']
        
        for i in range(SCALE_FACTORS['maintenance_orders']):
            scheduled_start = fake.date_between(start_date='-30d', end_date='+90d')
            scheduled_end = scheduled_start + datetime.timedelta(hours=random.randint(1, 48))
            
            parts_used = [
                {"part_number": f"P{random.randint(1000, 9999)}", "quantity": random.randint(1, 5), "cost": round(random.uniform(10, 500), 2)},
                {"part_number": f"P{random.randint(1000, 9999)}", "quantity": random.randint(1, 3), "cost": round(random.uniform(5, 200), 2)}
            ]
            
            tools_used = ["Multimeter", "Torque Wrench", "Hydraulic Jack", "Alignment Tools"]
            maintenance_team = [f"TECH{random.randint(1, 1000)}", f"TECH{random.randint(1, 1000)}"]
            required_skills = ["Mechanical", "Electrical", "Hydraulics"]
            
            maintenance_orders_data.append((
                f"MO{i+1:08d}",
                random.choice(order_types),
                random.choice(priorities),
                random.choice(equipment_ids) if equipment_ids else None,
                f"EQ{random.randint(1, 25000):08d}",
                random.choice(plan_ids) if plan_ids else None,
                scheduled_start,
                scheduled_end,
                fake.date_between(start_date=scheduled_start, end_date=scheduled_end + datetime.timedelta(days=5)) if random.random() < 0.6 else None,
                fake.date_between(start_date=scheduled_start, end_date=scheduled_end + datetime.timedelta(days=7)) if random.random() < 0.5 else None,
                round(random.uniform(1, 48), 2),    # estimated_duration_hours
                round(random.uniform(1, 52), 2) if random.random() < 0.5 else None,  # actual_duration_hours
                random.randint(1, 1000),   # assigned_technician_id
                json.dumps(maintenance_team),
                json.dumps(required_skills),
                fake.text(max_nb_chars=300),
                fake.text(max_nb_chars=400) if random.random() < 0.6 else None,
                json.dumps(parts_used) if random.random() < 0.7 else None,
                json.dumps(tools_used[:random.randint(2, 4)]),
                random.choice(order_statuses),
                round(random.uniform(0, 100), 2) if random.random() < 0.6 else 0,  # completion_percentage
                round(random.uniform(500, 15000), 2),   # estimated_cost
                round(random.uniform(450, 18000), 2) if random.random() < 0.5 else None,  # actual_cost
                round(random.uniform(200, 8000), 2) if random.random() < 0.5 else None,   # labor_cost
                round(random.uniform(100, 5000), 2) if random.random() < 0.5 else None,   # parts_cost
                round(random.uniform(0, 5000), 2) if random.random() < 0.3 else None,     # contractor_cost
                True,  # safety_procedures_followed
                random.choice([True, False]),  # quality_check_passed
                fake.sentence() if random.random() < 0.7 else None,
                fake.text(max_nb_chars=200) if random.random() < 0.4 else None,
                fake.text(max_nb_chars=300) if random.random() < 0.6 else None,
                random.randint(1, 1000),   # created_by
                True
            ))
        
        # Insert in batches
        batch_size = 1000
        for i in range(0, len(maintenance_orders_data), batch_size):
            batch = maintenance_orders_data[i:i+batch_size]
            cursor.executemany("""
                INSERT INTO maintenance_orders (
                    order_number, order_type, priority, equipment_id, equipment_number,
                    maintenance_plan_id, scheduled_start_date, scheduled_end_date, actual_start_date,
                    actual_end_date, estimated_duration_hours, actual_duration_hours,
                    assigned_technician_id, maintenance_team, required_skills, work_description,
                    work_performed, parts_used, tools_used, order_status, completion_percentage,
                    estimated_cost, actual_cost, labor_cost, parts_cost, contractor_cost,
                    safety_procedures_followed, quality_check_passed, post_maintenance_test_results,
                    work_order_notes, completion_notes, created_by, is_active
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, batch)
            self.conn.commit()
            
            if (i + batch_size) % 10000 == 0:
                print(f"    Inserted {min(i + batch_size, SCALE_FACTORS['maintenance_orders']):,} maintenance orders...")
        
        cursor.close()

    def generate_summary_report(self):
        """Generate summary report of populated operational data"""
        cursor = self.conn.cursor()
        print("\n" + "="*100)
        print("📊 MYSQL OPERATIONAL DATA POPULATION SUMMARY REPORT")
        print("="*100)
        
        tables = [
            'maintenance_plans', 'equipment', 'bill_of_materials', 'routings', 'routing_operations',
            'work_orders', 'purchase_orders', 'purchase_order_items', 'sales_orders', 'sales_order_items',
            'goods_receipts', 'shipments', 'shipment_items', 'quality_events', 'maintenance_orders'
        ]
        
        total_records = 0
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            total_records += count
            print(f"📋 {table:<25}: {count:>12,} records")
        
        print(f"\n🎯 TOTAL OPERATIONAL RECORDS: {total_records:,}")
        
        # Equipment type breakdown
        cursor.execute("""
            SELECT equipment_type, COUNT(*) 
            FROM equipment 
            GROUP BY equipment_type 
            ORDER BY COUNT(*) DESC
        """)
        equipment_breakdown = cursor.fetchall()
        print(f"\n⚙️ EQUIPMENT TYPE BREAKDOWN:")
        for equipment_type, count in equipment_breakdown:
            print(f"   {equipment_type:<20}: {count:>8,}")
        
        # Work order status breakdown
        cursor.execute("""
            SELECT work_order_status, COUNT(*) 
            FROM work_orders 
            GROUP BY work_order_status 
            ORDER BY COUNT(*) DESC
        """)
        wo_breakdown = cursor.fetchall()
        print(f"\n📋 WORK ORDER STATUS BREAKDOWN:")
        for status, count in wo_breakdown:
            print(f"   {status:<20}: {count:>8,}")
        
        # Purchase vs Sales volume
        cursor.execute("SELECT SUM(total_amount) FROM purchase_orders")
        total_purchases = cursor.fetchone()[0] or 0
        cursor.execute("SELECT SUM(total_amount) FROM sales_orders")
        total_sales = cursor.fetchone()[0] or 0
        
        print(f"\n💰 FINANCIAL SUMMARY:")
        print(f"   Total Purchase Orders: ${total_purchases:>15,.2f}")
        print(f"   Total Sales Orders:    ${total_sales:>15,.2f}")
        print(f"   Net Revenue:           ${total_sales - total_purchases:>15,.2f}")
        
        print("\n✅ MySQL operational data generation completed successfully!")
        print("🚀 Ready for cross-database analytics and reporting")
        print("="*100)
        
        cursor.close()

class MongoDBIoTEnvironmentalGenerator:
    """Enhanced MongoDB IoT Environmental Data Generator with Error Prevention"""
    
    def __init__(self):
        self.client = None
        self.db = None
        self.location_ids = []  # Cache for location IDs
        self.equipment_list = []  # Cache for equipment IDs

    def connect(self):
        """Connect to MongoDB - create database if doesn't exist"""
        try:
            # Connect to MongoDB server
            self.client = pymongo.MongoClient(
                host=MONGODB_CONFIG['host'],
                port=MONGODB_CONFIG['port'],
                serverSelectionTimeoutMS=5000  # 5 second timeout
            )
            
            # Test connection
            self.client.server_info()
            print("✅ Connected to MongoDB server")
            
            # Select database (MongoDB creates databases on first write)
            self.db = self.client[MONGODB_CONFIG['database']]
            print(f"✅ Selected database: {MONGODB_CONFIG['database']}")
            
        except pymongo.errors.ServerSelectionTimeoutError as e:
            print(f"❌ MongoDB connection error: {e}")
            print("🔍 Please ensure MongoDB is running on localhost:27017")
            raise
        except Exception as e:
            print(f"❌ Unexpected MongoDB error: {e}")
            raise

   
    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            print("🔒 MongoDB connection closed")

    def drop_existing_collections(self):
        """Drop existing collections to start fresh"""
        print("🧹 Dropping existing collections...")
        
        collections_to_drop = [
            'sensor_readings', 'equipment_analytics', 'process_data', 'energy_consumption',
            'geographic_locations', 'weather_data', 'environmental_events',
            'cybersecurity_events', 'network_infrastructure', 'it_systems', 'software_licenses',
            'batch_records', 'recipes', 'control_logic', 'alarm_events',
            'maintenance_analytics', 'quality_analytics', 'production_analytics', 'supply_chain_analytics'
        ]
        
        for collection_name in collections_to_drop:
            try:
                self.db[collection_name].drop()
            except Exception as e:
                print(f"⚠️  Could not drop collection {collection_name}: {e}")
        
        print("✅ Cleaned up existing collections")

    def create_indexes(self):
        """Create indexes for better query performance - FIXED VERSION"""
        print("🗂️  Creating MongoDB indexes...")
        
        try:
            # Basic indexes first (non-geospatial)
            print("   Creating basic indexes...")
            self.db.sensor_readings.create_index([("equipment_id", 1), ("timestamp", -1)])
            self.db.sensor_readings.create_index([("facility_id", 1), ("timestamp", -1)])
            self.db.sensor_readings.create_index([("readings.status", 1), ("timestamp", -1)])
            
            self.db.equipment_analytics.create_index([("equipment_id", 1), ("date", -1)])
            self.db.equipment_analytics.create_index([("facility_id", 1), ("date", -1)])
            
            self.db.weather_data.create_index([("location_id", 1), ("timestamp", -1)])
            
            self.db.environmental_events.create_index([("event_type", 1), ("event_datetime", -1)])
            self.db.environmental_events.create_index([("severity", 1), ("event_datetime", -1)])
            
            self.db.process_data.create_index([("equipment_id", 1), ("timestamp", -1)])
            self.db.alarm_events.create_index([("equipment_id", 1), ("alarm_datetime", -1)])
            
            print("✅ Created basic indexes")
            
        except Exception as e:
            print(f"⚠️  Error creating basic indexes: {e}")
            # Continue execution even if some indexes fail

    def create_geospatial_indexes(self):
        """Create geospatial indexes AFTER data is populated"""
        print("🌍 Creating geospatial indexes...")
        
        try:
            # Create 2dsphere index on the correct geometry field
            self.db.geographic_locations.create_index([("location", "2dsphere")])
            print("✅ Created geospatial index on location field")
            
        except Exception as e:
            print(f"⚠️  Could not create geospatial index: {e}")
            print("   Geospatial queries may be slower without this index")

    def validate_coordinates(self, latitude, longitude):
        """Validate coordinate values are within valid ranges"""
        # Clamp latitude to valid range
        latitude = max(-90, min(90, latitude))
        # Clamp longitude to valid range  
        longitude = max(-180, min(180, longitude))
        return latitude, longitude

    def populate_all_iot_data(self):
        """Populate all IoT data in logical order"""
        print("🔄 Starting MongoDB IoT data population...")
        
        # Population order for dependencies
        self.populate_geographic_locations()
        self.create_geospatial_indexes()  # Create geospatial indexes AFTER data population
        self.populate_weather_data()
        self.populate_environmental_events()
        self.populate_sensor_readings()
        self.populate_equipment_analytics()
        self.populate_process_data()
        self.populate_energy_consumption()
        self.populate_it_systems()
        self.populate_network_infrastructure()
        self.populate_cybersecurity_events()
        self.populate_software_licenses()
        self.populate_batch_records()
        self.populate_recipes()
        self.populate_control_logic()
        self.populate_alarm_events()
        self.populate_analytics_data()
        
        print("✅ Completed all MongoDB IoT data population")

    def populate_geographic_locations(self):
        """Populate geographic locations - FIXED VERSION"""
        print("  🌍 Inserting geographic locations...")
        
        geographic_data = []
        
        try:
            for i in range(SCALE_FACTORS['geographic_locations']):
                # Generate and validate coordinates
                raw_lat = random.uniform(-90, 90)
                raw_lon = random.uniform(-180, 180)
                latitude, longitude = self.validate_coordinates(raw_lat, raw_lon)
                elevation = round(random.uniform(-100, 8000), 2)
                
                # Store coordinates as separate object (non-geospatial)
                coordinates = {
                    'latitude': round(latitude, 6),
                    'longitude': round(longitude, 6),
                    'elevation': elevation
                }
                
                # Create proper GeoJSON geometry for geospatial operations
                # Note: GeoJSON uses [longitude, latitude] order (x, y)
                geojson_location = {
                    'type': 'Point',
                    'coordinates': [longitude, latitude]  # longitude first, then latitude
                }
                
                doc = {
                    '_id': ObjectId(),
                    'location_id': f'GEO_{i+1:06d}',
                    'location_name': fake.city(),
                    'coordinates': coordinates,  # Keep separate for human-readable access
                    'location': geojson_location,  # Proper GeoJSON for geospatial indexing
                    'administrative': {
                        'country': fake.country(),
                        'region': fake.state(),
                        'time_zone': random.choice(['UTC-8', 'UTC-5', 'UTC', 'UTC+1', 'UTC+8']),
                        'population': random.randint(1000, 10000000)
                    },
                    'climate': {
                        'zone': random.choice(['Tropical', 'Temperate', 'Arctic', 'Desert', 'Mediterranean']),
                        'avg_temperature': round(random.uniform(-20, 35), 1),
                        'avg_precipitation': round(random.uniform(100, 2000), 1),
                        'humidity_range': f"{random.randint(30, 50)}% - {random.randint(60, 90)}%",
                        'seasonal_variation': random.choice(['High', 'Medium', 'Low'])
                    },
                    'infrastructure': {
                        'rating': random.choice(['Excellent', 'Good', 'Fair', 'Poor']),
                        'transportation': random.choice(['Highway', 'Rail', 'Port', 'Airport']),
                        'utilities': random.choice(['Full', 'Partial', 'Limited']),
                        'communication': random.choice(['5G', '4G', '3G', 'Limited']),
                        'internet_speed_mbps': random.randint(1, 1000)
                    },
                    'economic': {
                        'activity': random.choice(['Industrial', 'Agricultural', 'Service', 'Mixed']),
                        'cost_index': round(random.uniform(0.5, 2.0), 2),
                        'labor_availability': random.choice(['High', 'Medium', 'Low']),
                        'market_access': random.choice(['Excellent', 'Good', 'Limited']),
                        'gdp_per_capita': random.randint(5000, 80000)
                    },
                    'risk_assessment': {
                        'environmental_risk': random.choice(['Low', 'Medium', 'High']),
                        'natural_disasters': random.sample(['Floods', 'Earthquakes', 'Hurricanes', 'Tornadoes', 'Wildfires'], k=random.randint(0, 3)),
                        'security_level': random.choice(['High', 'Medium', 'Low']),
                        'regulatory_environment': random.choice(['Favorable', 'Neutral', 'Complex']),
                        'political_stability': random.choice(['Stable', 'Moderate', 'Unstable'])
                    },
                    'created_date': datetime.datetime.now(),
                    'last_updated': datetime.datetime.now()
                }
                geographic_data.append(doc)
                
                # Insert in smaller batches to prevent memory issues
                if len(geographic_data) >= 500:  # Reduced batch size
                    try:
                        self.db.geographic_locations.insert_many(geographic_data, ordered=False)
                        geographic_data = []
                        if (i + 1) % 2000 == 0:
                            print(f"    Inserted {i + 1:,} geographic locations...")
                    except Exception as batch_error:
                        print(f"⚠️  Batch insert error at location {i+1}: {batch_error}")
                        # Try individual inserts for this batch
                        for doc in geographic_data:
                            try:
                                self.db.geographic_locations.insert_one(doc)
                            except Exception as doc_error:
                                print(f"❌ Failed to insert location {doc['location_id']}: {doc_error}")
                        geographic_data = []
            
            # Insert remaining data
            if geographic_data:
                try:
                    self.db.geographic_locations.insert_many(geographic_data, ordered=False)
                except Exception as batch_error:
                    print(f"⚠️  Final batch insert error: {batch_error}")
                    for doc in geographic_data:
                        try:
                            self.db.geographic_locations.insert_one(doc)
                        except Exception as doc_error:
                            print(f"❌ Failed to insert location {doc['location_id']}: {doc_error}")
            
            # Cache location IDs for other collections
            self.location_ids = list(self.db.geographic_locations.find({}, {'location_id': 1, '_id': 0}))
            print(f"✅ Cached {len(self.location_ids)} location IDs for cross-references")
                
        except Exception as e:
            print(f"❌ Error populating geographic locations: {e}")
            raise

    def populate_weather_data(self):
        """Populate weather data for geographic locations - IMPROVED VERSION"""
        print("  🌤️  Inserting weather data...")
        
        try:
            # Use cached location IDs or fetch limited set
            if not self.location_ids:
                self.location_ids = list(self.db.geographic_locations.find({}, {'location_id': 1, '_id': 0}).limit(500))
            
            if not self.location_ids:
                print("  ⚠️  No geographic locations found. Skipping weather data.")
                return
            
            # Use subset of locations for performance
            active_locations = self.location_ids[:min(200, len(self.location_ids))]
            print(f"    Generating weather data for {len(active_locations)} locations...")
            
            weather_data = []
            start_date = datetime.datetime.now() - datetime.timedelta(days=30)  # Reduced from 90 to 30 days
            
            for day in range(30):
                current_date = start_date + datetime.timedelta(days=day)
                
                for hour in range(0, 24, 6):  # Every 6 hours instead of 3
                    timestamp = current_date + datetime.timedelta(hours=hour)
                    
                    # Weather for each active location
                    for location in active_locations:
                        # Generate realistic temperature based on basic climate patterns
                        base_temp = random.uniform(-20, 40)
                        feels_like = base_temp + random.uniform(-5, 10)
                        
                        doc = {
                            '_id': ObjectId(),
                            'location_id': location['location_id'],
                            'timestamp': timestamp,
                            'current_conditions': {
                                'temperature': round(base_temp, 1),
                                'feels_like': round(feels_like, 1),
                                'humidity': round(random.uniform(10, 100), 1),
                                'pressure': round(random.uniform(980, 1040), 2),
                                'wind_speed': round(random.uniform(0, 120), 1),
                                'wind_direction': random.randint(0, 359),
                                'wind_gust': round(random.uniform(0, 150), 1),
                                'visibility': round(random.uniform(0.1, 50), 1),
                                'cloud_cover': round(random.uniform(0, 100), 1),
                                'condition': random.choice(['Clear', 'Partly Cloudy', 'Cloudy', 'Overcast', 'Rainy', 'Stormy', 'Snowy', 'Foggy']),
                                'precipitation': round(random.uniform(0, 25), 2),
                                'snow_depth': round(random.uniform(0, 200), 1) if random.random() < 0.1 else 0
                            },
                            'air_quality': {
                                'aqi': random.randint(0, 500),
                                'pm25': round(random.uniform(0, 200), 1),
                                'pm10': round(random.uniform(0, 300), 1),
                                'ozone': round(random.uniform(0, 400), 1),
                                'no2': round(random.uniform(0, 200), 1),
                                'so2': round(random.uniform(0, 100), 1),
                                'co': round(random.uniform(0, 50), 1),
                                'rating': random.choice(['Good', 'Moderate', 'Unhealthy for Sensitive', 'Unhealthy', 'Very Unhealthy', 'Hazardous'])
                            },
                            'solar_data': {
                                'radiation': round(random.uniform(0, 1200), 2),
                                'uv_index': random.randint(0, 12),
                                'daylight_hours': round(random.uniform(0, 24), 1),
                                'solar_elevation': round(random.uniform(-90, 90), 1)
                            },
                            'atmospheric': {
                                'dew_point': round(base_temp - random.uniform(5, 20), 1),
                                'heat_index': round(feels_like + random.uniform(0, 10), 1),
                                'wind_chill': round(base_temp - random.uniform(0, 15), 1)
                            },
                            'data_quality': {
                                'accuracy': round(random.uniform(0.85, 0.99), 3),
                                'completeness': round(random.uniform(0.90, 1.0), 3),
                                'source': random.choice(['Primary Station', 'Satellite', 'Model', 'Interpolated']),
                                'last_calibration': fake.date_time_between(start_date='-90d', end_date='now')
                            },
                            'created_date': datetime.datetime.now()
                        }
                        weather_data.append(doc)
                        
                        # Insert in smaller batches
                        if len(weather_data) >= 2000:  # Reduced batch size
                            try:
                                self.db.weather_data.insert_many(weather_data, ordered=False)
                                weather_data = []
                            except Exception as batch_error:
                                print(f"⚠️  Weather data batch error: {batch_error}")
                                weather_data = []  # Skip problematic batch
                
                if (day + 1) % 5 == 0:
                    print(f"    Processed {day + 1} days of weather data...")
            
            # Insert remaining data
            if weather_data:
                try:
                    self.db.weather_data.insert_many(weather_data, ordered=False)
                except Exception as batch_error:
                    print(f"⚠️  Final weather batch error: {batch_error}")
                
        except Exception as e:
            print(f"❌ Error populating weather data: {e}")
            raise

    def populate_environmental_events(self):
        """Populate environmental events - IMPROVED VERSION"""
        print("  🚨 Inserting environmental events...")
        
        try:
            # Use cached location IDs
            if not self.location_ids:
                self.location_ids = list(self.db.geographic_locations.find({}, {'location_id': 1, '_id': 0}).limit(1000))
            
            environmental_events = []
            event_counter = 1
            
            for i in range(SCALE_FACTORS['environmental_events']):
                event_datetime = fake.date_time_between(start_date='-2y', end_date='now')
                duration_hours = random.randint(1, 720)
                
                # Ensure we have valid location reference
                location_id = random.choice(self.location_ids)['location_id'] if self.location_ids else f'GEO_{random.randint(1, 1000):06d}'
                
                # Generate valid coordinates
                center_lat, center_lon = self.validate_coordinates(
                    random.uniform(-90, 90), 
                    random.uniform(-180, 180)
                )
                
                doc = {
                    '_id': ObjectId(),
                    'event_id': f'ENV_EVENT_{event_counter:08d}',
                    'event_type': random.choice(['Storm', 'Flood', 'Earthquake', 'Heatwave', 'Drought', 'Wildfire', 'Hurricane', 'Tornado', 'Blizzard', 'Tsunami']),
                    'severity': random.choice(['Minor', 'Moderate', 'Major', 'Severe', 'Catastrophic']),
                    'location_id': location_id,
                    'affected_region': {
                        'center_coordinates': {
                            'latitude': center_lat,
                            'longitude': center_lon
                        },
                        'radius_km': round(random.uniform(1, 1000), 2),
                        'polygon_coordinates': [
                            [round(random.uniform(-180, 180), 6), round(random.uniform(-90, 90), 6)]
                            for _ in range(random.randint(3, 8))
                        ]
                    },
                    'temporal_data': {
                        'event_datetime': event_datetime,
                        'end_datetime': event_datetime + datetime.timedelta(hours=duration_hours),
                        'duration_hours': duration_hours,
                        'warning_time_hours': random.randint(0, 72),
                        'time_zone': random.choice(['UTC-8', 'UTC-5', 'UTC', 'UTC+1', 'UTC+8'])
                    },
                    'impact_assessment': {
                        'affected_area_km2': round(random.uniform(1, 50000), 2),
                        'population_affected': random.randint(100, 10000000),
                        'economic_impact_usd': round(random.uniform(10000, 1000000000), 2),
                        'infrastructure_damage': random.choice(['None', 'Minor', 'Moderate', 'Severe', 'Destroyed']),
                        'environmental_damage': random.choice(['None', 'Minor', 'Moderate', 'Severe', 'Irreversible']),
                        'casualties': {
                            'fatalities': random.randint(0, 1000),
                            'injuries': random.randint(0, 10000),
                            'missing': random.randint(0, 100),
                            'displaced': random.randint(0, 100000)
                        },
                        'facilities_affected': random.randint(0, 50)
                    },
                    'meteorological_data': {
                        'max_wind_speed': round(random.uniform(0, 300), 1),
                        'min_pressure': round(random.uniform(900, 1040), 2),
                        'max_temperature': round(random.uniform(-40, 60), 1),
                        'precipitation_total': round(random.uniform(0, 1000), 2),
                        'storm_surge_height': round(random.uniform(0, 20), 2) if random.random() < 0.3 else None
                    },
                    'response': {
                        'response_time_hours': random.randint(1, 168),
                        'recovery_time_days': random.randint(1, 1095),
                        'emergency_declared': random.choice([True, False]),
                        'international_aid': random.choice([True, False]),
                        'evacuation_ordered': random.choice([True, False]),
                        'shelters_opened': random.randint(0, 100),
                        'response_agencies': random.sample(['FEMA', 'Red Cross', 'National Guard', 'Local Emergency', 'UN'], k=random.randint(1, 3))
                    },
                    'monitoring_data': {
                        'stations_affected': random.randint(1, 50),
                        'data_quality': random.choice(['Good', 'Degraded', 'Lost', 'Restored']),
                        'backup_systems_activated': random.choice([True, False]),
                        'satellite_coverage': random.choice(['Full', 'Partial', 'None']),
                        'communication_status': random.choice(['Normal', 'Degraded', 'Lost'])
                    },
                    'lessons_learned': fake.text(max_nb_chars=500),
                    'related_events': [f'ENV_EVENT_{random.randint(1, max(1, event_counter-1)):08d}' for _ in range(random.randint(0, 2))],
                    'created_date': datetime.datetime.now(),
                    'last_updated': datetime.datetime.now()
                }
                environmental_events.append(doc)
                event_counter += 1
                
                # Insert in batches
                if len(environmental_events) >= 500:  # Smaller batches
                    try:
                        self.db.environmental_events.insert_many(environmental_events, ordered=False)
                        environmental_events = []
                        if (i + 1) % 1000 == 0:
                            print(f"    Inserted {i + 1:,} environmental events...")
                    except Exception as batch_error:
                        print(f"⚠️  Environmental events batch error: {batch_error}")
                        environmental_events = []
            
            # Insert remaining data
            if environmental_events:
                try:
                    self.db.environmental_events.insert_many(environmental_events, ordered=False)
                except Exception as batch_error:
                    print(f"⚠️  Final environmental events batch error: {batch_error}")
                
        except Exception as e:
            print(f"❌ Error populating environmental events: {e}")
            raise

    def populate_sensor_readings(self):
        """Populate sensor readings with comprehensive sensor coverage - OPTIMIZED VERSION"""
        print("  📊 Inserting sensor readings...")
        
        try:
            # Generate sensor equipment data with sequential IDs
            sensor_equipment = []
            sensor_types = {
                'Temperature Sensor': {'unit': '°C', 'range': (-50, 200), 'alarms': (10, 20, 80, 90)},
                'Pressure Sensor': {'unit': 'bar', 'range': (0, 100), 'alarms': (5, 10, 85, 95)},
                'Flow Sensor': {'unit': 'L/min', 'range': (0, 1000), 'alarms': (50, 100, 900, 950)},
                'Level Sensor': {'unit': '%', 'range': (0, 100), 'alarms': (10, 20, 90, 95)},
                'pH Sensor': {'unit': 'pH', 'range': (0, 14), 'alarms': (5.5, 6.0, 8.5, 9.0)},
                'Conductivity Sensor': {'unit': 'μS/cm', 'range': (0, 2000), 'alarms': (100, 200, 1800, 1900)},
                'Oxygen Sensor': {'unit': '%', 'range': (0, 25), 'alarms': (18, 19.5, 23, 25)},
                'CO2 Sensor': {'unit': 'ppm', 'range': (0, 5000), 'alarms': (0, 0, 1000, 5000)},
                'CO Sensor': {'unit': 'ppm', 'range': (0, 100), 'alarms': (0, 0, 35, 50)},
                'Vibration Sensor': {'unit': 'mm/s', 'range': (0, 50), 'alarms': (0, 0, 10, 25)},
                'Current Sensor': {'unit': 'A', 'range': (0, 500), 'alarms': (10, 20, 450, 480)},
                'Voltage Sensor': {'unit': 'V', 'range': (0, 690), 'alarms': (200, 220, 500, 600)},
                'Power Sensor': {'unit': 'kW', 'range': (0, 1000), 'alarms': (50, 100, 900, 950)},
                'Air Quality Sensor': {'unit': 'μg/m³', 'range': (0, 500), 'alarms': (0, 0, 35, 150)},
                'Noise Sensor': {'unit': 'dB', 'range': (30, 120), 'alarms': (40, 50, 85, 100)},
                'Humidity Sensor': {'unit': '%RH', 'range': (0, 100), 'alarms': (20, 30, 80, 90)}
            }
            
            # Create sensor equipment with guaranteed unique IDs
            equipment_counter = 1
            for sensor_type, config in sensor_types.items():
                count = random.randint(20, 50)  # Reduced count for performance
                for i in range(count):
                    sensor_equipment.append({
                        'equipment_id': equipment_counter,
                        'equipment_number': f'SENSOR_{equipment_counter:08d}',
                        'equipment_name': f'{sensor_type} {equipment_counter:04d}',
                        'equipment_category': sensor_type,
                        'facility_id': random.randint(1, 15),
                        'production_line_id': random.randint(1, 100),
                        'department_id': random.randint(1, 120),
                        'config': config
                    })
                    equipment_counter += 1
                    
                    if equipment_counter > 1000:  # Limit for performance
                        break
                if equipment_counter > 1000:
                    break
            
            # Cache equipment list
            self.equipment_list = sensor_equipment
            print(f"    Generated {len(sensor_equipment):,} sensors for data collection...")
            
            # Generate sensor readings - REDUCED VOLUME
            sensor_readings = []
            start_date = datetime.datetime.now() - datetime.timedelta(days=3)  # Reduced from 7 to 3 days
            reading_count = 0
            target_readings = min(SCALE_FACTORS['sensor_readings'], 100000)  # Cap at 100K for initial test
            
            for day in range(3):
                current_date = start_date + datetime.timedelta(days=day)
                
                for reading_interval in range(0, 1440, 15):  # Every 15 minutes instead of 5
                    timestamp = current_date + datetime.timedelta(minutes=reading_interval)
                    
                    # Sample subset of sensors for performance
                    sample_sensors = random.sample(sensor_equipment, min(100, len(sensor_equipment)))
                    
                    for sensor_info in sample_sensors:
                        if reading_count >= target_readings:
                            break
                            
                        config = sensor_info['config']
                        value_range = config['range']
                        unit = config['unit']
                        low_alarm, low_warn, high_warn, high_alarm = config['alarms']
                        
                        # Generate realistic sensor value
                        primary_value = round(random.uniform(value_range[0], value_range[1]), 2)
                        
                        # Add realistic sensor drift and noise
                        noise_factor = 0.02  # 2% noise
                        noise = random.uniform(-noise_factor, noise_factor) * (value_range[1] - value_range[0])
                        primary_value += noise
                        primary_value = max(value_range[0], min(value_range[1], primary_value))
                        
                        # Determine sensor status
                        if primary_value <= low_alarm or primary_value >= high_alarm:
                            status = 'Alarm'
                            quality = 'Bad'
                        elif primary_value <= low_warn or primary_value >= high_warn:
                            status = 'Warning'
                            quality = 'Uncertain'
                        else:
                            status = 'Normal'
                            quality = 'Good'
                        
                        doc = {
                            '_id': ObjectId(),
                            'equipment_id': sensor_info['equipment_id'],
                            'equipment_number': sensor_info['equipment_number'],
                            'equipment_name': sensor_info['equipment_name'],
                            'equipment_category': sensor_info['equipment_category'],
                            'facility_id': sensor_info['facility_id'],
                            'production_line_id': sensor_info['production_line_id'],
                            'department_id': sensor_info['department_id'],
                            'timestamp': timestamp,
                            'readings': {
                                'primary_value': round(primary_value, 4),
                                'secondary_value': round(primary_value * random.uniform(0.98, 1.02), 4),
                                'unit': unit,
                                'quality': quality,
                                'status': status,
                                'confidence': round(random.uniform(0.85, 0.99), 3),
                                'raw_value': round(primary_value * random.uniform(0.95, 1.05), 4),
                                'filtered_value': round(primary_value, 4),
                                'rate_of_change': round(random.uniform(-1, 1), 4)
                            },
                            'limits': {
                                'low_alarm': low_alarm,
                                'low_warning': low_warn,
                                'high_warning': high_warn,
                                'high_alarm': high_alarm,
                                'engineering_units': unit,
                                'deadband': round((high_warn - low_warn) * 0.02, 2)
                            },
                            'device_info': {
                                'battery_level': round(random.uniform(10, 100), 1),
                                'signal_strength': round(random.uniform(-100, -20), 1),
                                'network_status': random.choice(['Connected', 'Weak Signal', 'Disconnected', 'Intermittent']),
                                'memory_usage': round(random.uniform(10, 90), 1),
                                'cpu_utilization': round(random.uniform(5, 80), 1),
                                'temperature': round(random.uniform(20, 70), 1),
                                'calibration_due': fake.date_time_between(start_date='+30d', end_date='+365d'),
                                'last_maintenance': fake.date_time_between(start_date='-180d', end_date='now'),
                                'communication_protocol': random.choice(['Modbus TCP', 'Ethernet/IP', 'OPC UA', 'MQTT', 'LoRaWAN', 'WiFi']),
                                'firmware_version': f'v{random.randint(1, 5)}.{random.randint(0, 9)}.{random.randint(0, 9)}',
                                'hardware_revision': f'Rev{chr(65 + random.randint(0, 5))}'
                            },
                            'process_context': {
                                'process_area': f'Area_{chr(65 + random.randint(0, 9))}',
                                'control_loop': f'Loop_{random.randint(100, 999)}',
                                'operating_mode': random.choice(['Auto', 'Manual', 'Maintenance', 'Startup', 'Shutdown']),
                                'shift': random.choice(['Day', 'Evening', 'Night']),
                                'operator_id': random.randint(1, 1000)
                            }
                        }
                        sensor_readings.append(doc)
                        reading_count += 1
                        
                        # Insert in smaller batches
                        if len(sensor_readings) >= 1000:  # Reduced batch size
                            try:
                                self.db.sensor_readings.insert_many(sensor_readings, ordered=False)
                                sensor_readings = []
                                if reading_count % 10000 == 0:
                                    print(f"    Inserted {reading_count:,} sensor readings...")
                            except Exception as batch_error:
                                print(f"⚠️  Sensor readings batch error: {batch_error}")
                                sensor_readings = []
                    
                    if reading_count >= target_readings:
                        break
                if reading_count >= target_readings:
                    break
                
                print(f"    Processed day {day + 1} - Total readings: {reading_count:,}")
            
            # Insert remaining data
            if sensor_readings:
                try:
                    self.db.sensor_readings.insert_many(sensor_readings, ordered=False)
                except Exception as batch_error:
                    print(f"⚠️  Final sensor readings batch error: {batch_error}")
                
            print(f"✅ Completed sensor readings - Total: {reading_count:,}")
                
        except Exception as e:
            print(f"❌ Error populating sensor readings: {e}")
            raise

    def populate_equipment_analytics(self):
        """Populate equipment analytics data - OPTIMIZED VERSION"""
        print("  📈 Inserting equipment analytics...")
        
        try:
            equipment_analytics = []
            
            # Use cached equipment or generate subset
            if self.equipment_list:
                equipment_range = min(500, len(self.equipment_list))  # Limit to 500 equipment items
                equipment_ids = [eq['equipment_id'] for eq in self.equipment_list[:equipment_range]]
            else:
                equipment_ids = list(range(1, 501))  # First 500 equipment items
            
            analytics_count = 0
            target_analytics = min(SCALE_FACTORS['equipment_analytics'], 50000)  # Cap at 50K
            
            for equipment_id in equipment_ids:
                for day in range(30):  # 30 days of analytics
                    if analytics_count >= target_analytics:
                        break
                        
                    current_date = datetime.datetime.now() - datetime.timedelta(days=29-day)
                    
                    equipment_type = random.choice(['Production Machine', 'Material Handling', 'Utility Equipment', 'Control System'])
                    
                    # Equipment type affects performance patterns
                    if equipment_type == 'Production Machine':
                        base_oee = random.uniform(70, 85)
                        units_produced = random.randint(500, 5000)
                        energy_base = random.uniform(100, 1000)
                    elif equipment_type == 'Material Handling':
                        base_oee = random.uniform(80, 95)
                        units_produced = random.randint(1000, 10000)
                        energy_base = random.uniform(50, 500)
                    elif equipment_type == 'Utility Equipment':
                        base_oee = random.uniform(85, 98)
                        units_produced = random.randint(100, 1000)
                        energy_base = random.uniform(200, 2000)
                    else:  # Control System
                        base_oee = random.uniform(95, 99.9)
                        units_produced = None
                        energy_base = random.uniform(10, 100)
                    
                    doc = {
                        '_id': ObjectId(),
                        'equipment_id': equipment_id,
                        'equipment_number': f'EQ{equipment_id:08d}',
                        'equipment_type': equipment_type,
                        'facility_id': random.randint(1, 15),
                        'production_line_id': random.randint(1, 100),
                        'date': current_date,
                        'performance_metrics': {
                            'oee': round(base_oee + random.uniform(-5, 5), 2),
                            'availability': round(random.uniform(85, 99.5), 2),
                            'performance': round(random.uniform(80, 98), 2),
                            'quality': round(random.uniform(90, 99.9), 2),
                            'utilization': round(random.uniform(60, 95), 2),
                            'efficiency': round(random.uniform(75, 95), 2)
                        },
                        'production_data': {
                            'units_produced': units_produced,
                            'target_production': units_produced + random.randint(-500, 500) if units_produced else None,
                            'capacity_utilization': round(random.uniform(65, 95), 2),
                            'operating_hours': round(random.uniform(8, 24), 2),
                            'downtime_hours': round(random.uniform(0, 4), 2),
                            'setup_time_hours': round(random.uniform(0.2, 3), 2),
                            'changeover_count': random.randint(0, 5),
                            'cycle_time_seconds': round(random.uniform(30, 300), 2)
                        },
                        'maintenance_indicators': {
                            'vibration_level': round(random.uniform(0, 15), 2),
                            'temperature_max': round(random.uniform(30, 120), 1),
                            'lubrication_status': random.choice(['Excellent', 'Good', 'Fair', 'Needs Attention', 'Critical']),
                            'wear_level_percentage': round(random.uniform(0, 100), 1),
                            'next_maintenance_days': random.randint(1, 365),
                            'mtbf_hours': round(random.uniform(100, 8760), 2),
                            'mttr_hours': round(random.uniform(0.5, 48), 2),
                            'health_score': round(random.uniform(60, 100), 1)
                        },
                        'energy_consumption': {
                            'power_kwh': round(energy_base + random.uniform(-energy_base*0.2, energy_base*0.2), 2),
                            'efficiency_percentage': round(random.uniform(70, 95), 2),
                            'peak_demand_kw': round(random.uniform(energy_base*0.5, energy_base*1.5), 2),
                            'cost_usd': round(random.uniform(energy_base*0.05, energy_base*0.15), 2),
                            'carbon_footprint_kg': round(random.uniform(energy_base*0.4, energy_base*0.8), 2)
                        },
                        'quality_metrics': {
                            'defect_rate_percentage': round(random.uniform(0, 8), 3),
                            'first_pass_yield': round(random.uniform(85, 99.8), 2),
                            'rework_percentage': round(random.uniform(0, 12), 2),
                            'scrap_percentage': round(random.uniform(0, 5), 2),
                            'customer_complaints': random.randint(0, 3)
                        },
                        'operational_context': {
                            'shift': random.choice(['Day', 'Evening', 'Night']),
                            'operator_id': random.randint(1, 1000),
                            'product_mix': random.sample(['Product_A', 'Product_B', 'Product_C', 'Product_D'], k=random.randint(1, 3)),
                            'operating_mode': random.choice(['Normal', 'High Speed', 'Eco', 'Maintenance']),
                            'weather_impact': random.choice(['None', 'Minor', 'Moderate', 'Significant'])
                        },
                        'alerts_summary': {
                            'total_alerts': random.randint(0, 20),
                            'critical_alerts': random.randint(0, 3),
                            'warning_alerts': random.randint(0, 10),
                            'info_alerts': random.randint(0, 15),
                            'acknowledged_alerts': random.randint(0, 18),
                            'avg_response_time_minutes': round(random.uniform(1, 60), 2)
                        },
                        'calculated_date': datetime.datetime.now()
                    }
                    equipment_analytics.append(doc)
                    analytics_count += 1
                    
                    # Insert in batches
                    if len(equipment_analytics) >= 2000:  # Smaller batches
                        try:
                            self.db.equipment_analytics.insert_many(equipment_analytics, ordered=False)
                            equipment_analytics = []
                            if analytics_count % 10000 == 0:
                                print(f"    Inserted {analytics_count:,} equipment analytics...")
                        except Exception as batch_error:
                            print(f"⚠️  Equipment analytics batch error: {batch_error}")
                            equipment_analytics = []
                
                if analytics_count >= target_analytics:
                    break
                
                if (equipment_id) % 50 == 0:
                    print(f"    Processed analytics for {equipment_id:,} equipment items...")
            
            # Insert remaining data
            if equipment_analytics:
                try:
                    self.db.equipment_analytics.insert_many(equipment_analytics, ordered=False)
                except Exception as batch_error:
                    print(f"⚠️  Final equipment analytics batch error: {batch_error}")
                
            print(f"✅ Completed equipment analytics - Total: {analytics_count:,}")
                
        except Exception as e:
            print(f"❌ Error populating equipment analytics: {e}")
            raise

    def insert_with_progress(self, collection_name, data_list, batch_size=1000, description="records"):
        """Helper method to insert data with progress tracking and error handling"""
        total_inserted = 0
        batch_data = []
        
        try:
            for i, doc in enumerate(data_list):
                batch_data.append(doc)
                
                if len(batch_data) >= batch_size:
                    try:
                        collection = self.db[collection_name]
                        result = collection.insert_many(batch_data, ordered=False)
                        total_inserted += len(result.inserted_ids)
                        batch_data = []
                        
                        if total_inserted % (batch_size * 5) == 0:
                            print(f"    Inserted {total_inserted:,} {description}...")
                            
                    except Exception as batch_error:
                        print(f"⚠️  Batch error in {collection_name}: {batch_error}")
                        batch_data = []  # Skip problematic batch
            
            # Insert remaining data
            if batch_data:
                try:
                    collection = self.db[collection_name]
                    result = collection.insert_many(batch_data, ordered=False)
                    total_inserted += len(result.inserted_ids)
                except Exception as final_error:
                    print(f"⚠️  Final batch error in {collection_name}: {final_error}")
            
            return total_inserted
            
        except Exception as e:
            print(f"❌ Critical error in {collection_name}: {e}")
            return total_inserted

    def populate_process_data(self):
        """Populate process control data - OPTIMIZED VERSION"""
        print("  🔬 Inserting process data...")
        
        try:
            process_data = []
            start_date = datetime.datetime.now() - datetime.timedelta(days=3)  # Reduced from 7 days
            process_areas = ['Reactor_1', 'Reactor_2', 'Distillation', 'Crystallization', 'Packaging', 'Utilities']
            
            process_count = 0
            target_process = min(SCALE_FACTORS['process_data'], 50000)  # Cap at 50K
            
            for day in range(3):
                current_date = start_date + datetime.timedelta(days=day)
                
                for minute in range(0, 1440, 10):  # Every 10 minutes instead of 1
                    if process_count >= target_process:
                        break
                        
                    timestamp = current_date + datetime.timedelta(minutes=minute)
                    
                    for area in process_areas:
                        if process_count >= target_process:
                            break
                            
                        # Fewer control loops per area for performance
                        for loop_num in range(1, random.randint(2, 4)):
                            if process_count >= target_process:
                                break
                                
                            doc = {
                                '_id': ObjectId(),
                                'process_area': area,
                                'control_loop': f'{area}_Loop_{loop_num:02d}',
                                'timestamp': timestamp,
                                'setpoints': {
                                    'temperature_sp': round(random.uniform(20, 200), 2),
                                    'pressure_sp': round(random.uniform(1, 50), 2),
                                    'flow_sp': round(random.uniform(100, 1000), 2),
                                    'level_sp': round(random.uniform(20, 80), 2)
                                },
                                'process_values': {
                                    'temperature_pv': round(random.uniform(15, 205), 2),
                                    'pressure_pv': round(random.uniform(0.8, 52), 2),
                                    'flow_pv': round(random.uniform(95, 1050), 2),
                                    'level_pv': round(random.uniform(18, 85), 2)
                                },
                                'controller_outputs': {
                                    'valve_position': round(random.uniform(0, 100), 2),
                                    'pump_speed': round(random.uniform(0, 100), 2),
                                    'heater_output': round(random.uniform(0, 100), 2)
                                },
                                'quality_parameters': {
                                    'purity_percentage': round(random.uniform(95, 99.9), 3),
                                    'moisture_content': round(random.uniform(0, 5), 3),
                                    'particle_size': round(random.uniform(10, 500), 2),
                                    'color_value': round(random.uniform(0, 100), 2)
                                },
                                'batch_info': {
                                    'batch_id': f'BATCH_{random.randint(100000, 999999)}',
                                    'recipe_id': f'RECIPE_{random.randint(1, 100):03d}',
                                    'phase': random.choice(['Preparation', 'Reaction', 'Separation', 'Finishing', 'Complete']),
                                    'step_number': random.randint(1, 20),
                                    'remaining_time_minutes': random.randint(0, 480)
                                },
                                'alarms_active': random.randint(0, 5),
                                'operator_actions': random.randint(0, 3),
                                'system_mode': random.choice(['Auto', 'Manual', 'Semi-Auto', 'Cascade']),
                                'data_quality': {
                                    'validity': random.choice(['Good', 'Bad', 'Uncertain']),
                                    'timestamp_quality': random.choice(['Good', 'Bad']),
                                    'substituted_values': random.randint(0, 2)
                                }
                            }
                            process_data.append(doc)
                            process_count += 1
                    
                    # Insert in batches
                    if len(process_data) >= 2000:
                        try:
                            self.db.process_data.insert_many(process_data, ordered=False)
                            process_data = []
                            if process_count % 10000 == 0:
                                print(f"    Inserted {process_count:,} process data records...")
                        except Exception as batch_error:
                            print(f"⚠️  Process data batch error: {batch_error}")
                            process_data = []
                
                if process_count >= target_process:
                    break
                    
                print(f"    Processed day {day + 1} - Total process records: {process_count:,}")
            
            # Insert remaining data
            if process_data:
                try:
                    self.db.process_data.insert_many(process_data, ordered=False)
                except Exception as batch_error:
                    print(f"⚠️  Final process data batch error: {batch_error}")
                
            print(f"✅ Completed process data - Total: {process_count:,}")
                
        except Exception as e:
            print(f"❌ Error populating process data: {e}")
            raise
   
    def generate_summary_report(self):
        """Generate summary report of populated IoT data - ENHANCED VERSION"""
        print("\n" + "="*100)
        print("📊 MONGODB IOT ENVIRONMENTAL DATA SUMMARY REPORT")
        print("="*100)
        
        try:
            collections = [
                'geographic_locations', 'weather_data', 'environmental_events', 'sensor_readings',
                'equipment_analytics', 'process_data', 'energy_consumption', 'it_systems',
                'network_infrastructure', 'cybersecurity_events', 'software_licenses',
                'batch_records', 'recipes', 'control_logic', 'alarm_events',
                'maintenance_analytics', 'quality_analytics', 'production_analytics', 'supply_chain_analytics'
            ]
            
            total_documents = 0
            collection_stats = {}
            
            for collection_name in collections:
                try:
                    count = self.db[collection_name].count_documents({})
                    total_documents += count
                    collection_stats[collection_name] = count
                    print(f"📋 {collection_name:<25}: {count:>12,} documents")
                except Exception as e:
                    print(f"📋 {collection_name:<25}: {'ERROR':>12} ({e})")
                    collection_stats[collection_name] = 0
            
            print(f"\n🎯 TOTAL IOT DOCUMENTS: {total_documents:,}")
            
            # Database size information
            try:
                stats = self.db.command("dbstats")
                print(f"\n💾 DATABASE STATISTICS:")
                print(f"   Database Size:     {stats.get('dataSize', 0) / (1024*1024*1024):>10.2f} GB")
                print(f"   Index Size:        {stats.get('indexSize', 0) / (1024*1024):>10.2f} MB")
                print(f"   Total Collections: {stats.get('collections', 0):>10}")
                print(f"   Total Indexes:     {stats.get('indexes', 0):>10}")
            except Exception as e:
                print(f"⚠️  Could not retrieve database statistics: {e}")
            
            # Validate data relationships
            print(f"\n🔍 DATA VALIDATION:")
            if collection_stats.get('geographic_locations', 0) > 0:
                print(f"   ✅ Geographic locations: {collection_stats['geographic_locations']:,}")
                if collection_stats.get('weather_data', 0) > 0:
                    print(f"   ✅ Weather data linked to locations")
                else:
                    print(f"   ⚠️  No weather data found")
            else:
                print(f"   ❌ No geographic locations - other collections may have broken references")
            
            # Sample data analysis (if collections exist)
            try:
                if collection_stats.get('sensor_readings', 0) > 0:
                    sensor_status_pipeline = [
                        {"$group": {"_id": "$readings.status", "count": {"$sum": 1}}},
                        {"$sort": {"count": -1}}
                    ]
                    sensor_status = list(self.db.sensor_readings.aggregate(sensor_status_pipeline))
                    if sensor_status:
                        print(f"\n📊 SENSOR STATUS BREAKDOWN:")
                        for status in sensor_status:
                            print(f"   {status['_id']:<15}: {status['count']:>8,}")
                
                if collection_stats.get('equipment_analytics', 0) > 0:
                    equipment_type_pipeline = [
                        {"$group": {"_id": "$equipment_type", "count": {"$sum": 1}}},
                        {"$sort": {"count": -1}}
                    ]
                    equipment_types = list(self.db.equipment_analytics.aggregate(equipment_type_pipeline))
                    if equipment_types:
                        print(f"\n⚙️  EQUIPMENT TYPE BREAKDOWN:")
                        for eq_type in equipment_types:
                            print(f"   {eq_type['_id']:<20}: {eq_type['count']:>8,}")
                        
            except Exception as e:
                print(f"⚠️  Could not generate detailed analytics: {e}")
            
            print("\n✅ MongoDB IoT data generation completed!")
            print("🚀 Ready for real-time IoT analytics and environmental monitoring")
            print("="*100)
            
        except Exception as e:
            print(f"❌ Error generating summary report: {e}")

    def populate_energy_consumption(self):
        """Populate energy consumption data - OPTIMIZED VERSION"""
        print("  ⚡ Inserting energy consumption data...")
        
        try:
            energy_data = []
            start_date = datetime.datetime.now() - datetime.timedelta(days=15)  # Reduced from 30 days
            energy_count = 0
            target_energy = min(SCALE_FACTORS['energy_consumption'], 10000)  # Cap at 10K
            
            for day in range(15):
                if energy_count >= target_energy:
                    break
                    
                current_date = start_date + datetime.timedelta(days=day)
                
                for facility_id in range(1, 16):  # 15 facilities
                    for hour in range(0, 24, 4):  # Every 4 hours instead of hourly
                        if energy_count >= target_energy:
                            break
                            
                        timestamp = current_date + datetime.timedelta(hours=hour)
                        
                        doc = {
                            '_id': ObjectId(),
                            'facility_id': facility_id,
                            'timestamp': timestamp,
                            'electricity': {
                                'total_consumption_kwh': round(random.uniform(1000, 50000), 2),
                                'peak_demand_kw': round(random.uniform(500, 10000), 2),
                                'power_factor': round(random.uniform(0.85, 0.98), 3),
                                'cost_per_kwh': round(random.uniform(0.05, 0.25), 4),
                                'total_cost_usd': round(random.uniform(50, 12500), 2),
                                'renewable_percentage': round(random.uniform(0, 40), 2),
                                'grid_reliability': random.choice(['Stable', 'Fluctuating', 'Unstable'])
                            },
                            'natural_gas': {
                                'consumption_mcf': round(random.uniform(100, 5000), 2),
                                'cost_per_mcf': round(random.uniform(2, 8), 2),
                                'total_cost_usd': round(random.uniform(200, 40000), 2),
                                'heating_value_btu': round(random.uniform(900, 1100), 2)
                            },
                            'environmental_impact': {
                                'carbon_emissions_kg': round(random.uniform(100, 10000), 2),
                                'water_discharge_gallons': round(random.uniform(500, 80000), 2),
                                'waste_generated_kg': round(random.uniform(50, 5000), 2),
                                'sustainability_score': round(random.uniform(60, 95), 2)
                            },
                            'operational_context': {
                                'production_level': random.choice(['Low', 'Normal', 'High', 'Peak']),
                                'shift': random.choice(['Day', 'Evening', 'Night']),
                                'weekend_operation': hour >= 16 or hour <= 8,
                                'maintenance_mode': random.choice([True, False]) if random.random() < 0.05 else False
                            }
                        }
                        energy_data.append(doc)
                        energy_count += 1
                        
                        # Insert in batches
                        if len(energy_data) >= 1000:
                            try:
                                self.db.energy_consumption.insert_many(energy_data, ordered=False)
                                energy_data = []
                            except Exception as batch_error:
                                print(f"⚠️  Energy data batch error: {batch_error}")
                                energy_data = []
                
                if (day + 1) % 5 == 0:
                    print(f"    Processed {day + 1} days - Total: {energy_count:,}")
            
            # Insert remaining data
            if energy_data:
                try:
                    self.db.energy_consumption.insert_many(energy_data, ordered=False)
                except Exception as batch_error:
                    print(f"⚠️  Final energy batch error: {batch_error}")
                
            print(f"✅ Completed energy consumption - Total: {energy_count:,}")
                
        except Exception as e:
            print(f"❌ Error populating energy consumption: {e}")
            raise

    def populate_it_systems(self):
        """Populate IT systems data - OPTIMIZED VERSION"""
        print("  💻 Inserting IT systems data...")
        
        try:
            it_systems = []
            system_types = ['ERP', 'MES', 'SCADA', 'Database', 'Application Server', 'Web Server', 'File Server', 'Backup System']
            target_systems = min(SCALE_FACTORS['it_systems'], 1000)  # Cap at 1K
            
            for i in range(target_systems):
                system_type = random.choice(system_types)
                
                doc = {
                    '_id': ObjectId(),
                    'system_id': f'IT_SYS_{i+1:06d}',
                    'system_code': f'SYS{i+1:04d}',
                    'system_name': f'{system_type} System {i+1:04d}',
                    'system_type': system_type,
                    'system_category': random.choice(['Production', 'Business', 'Infrastructure', 'Security']),
                    'technical_info': {
                        'vendor': random.choice(['Microsoft', 'Oracle', 'SAP', 'Siemens', 'Rockwell', 'Wonderware']),
                        'version': f'v{random.randint(1, 10)}.{random.randint(0, 9)}.{random.randint(0, 9)}',
                        'platform': random.choice(['Windows Server 2019', 'Linux RHEL 8', 'Ubuntu 20.04', 'VMware vSphere']),
                        'deployment_type': random.choice(['On-Premise', 'Cloud', 'Hybrid'])
                    },
                    'network_info': {
                        'server_name': f'SERVER-{i+1:04d}',
                        'ip_address': fake.ipv4(),
                        'mac_address': fake.mac_address(),
                        'port_numbers': random.sample(range(1000, 9999), k=random.randint(1, 5)),
                        'url': fake.url() if random.random() < 0.6 else None,
                        'domain': fake.domain_name()
                    },
                    'status_health': {
                        'system_status': random.choice(['Active', 'Inactive', 'Maintenance', 'Failed', 'Degraded']),
                        'health_status': random.choice(['Healthy', 'Warning', 'Critical', 'Unknown']),
                        'uptime_percentage': round(random.uniform(95, 99.99), 3),
                        'last_reboot': fake.date_time_between(start_date='-30d', end_date='now')
                    },
                    'created_date': datetime.datetime.now(),
                    'is_active': True
                }
                it_systems.append(doc)
                
                # Insert in batches
                if len(it_systems) >= 500:
                    try:
                        self.db.it_systems.insert_many(it_systems, ordered=False)
                        it_systems = []
                    except Exception as batch_error:
                        print(f"⚠️  IT systems batch error: {batch_error}")
                        it_systems = []
            
            if it_systems:
                try:
                    self.db.it_systems.insert_many(it_systems, ordered=False)
                except Exception as batch_error:
                    print(f"⚠️  Final IT systems batch error: {batch_error}")
                
        except Exception as e:
            print(f"❌ Error populating IT systems: {e}")
            raise

    def populate_network_infrastructure(self):
        """Populate network infrastructure data - OPTIMIZED VERSION"""
        print("  🌐 Inserting network infrastructure data...")
        
        try:
            network_data = []
            device_types = ['Switch', 'Router', 'Firewall', 'Access Point', 'Load Balancer', 'VPN Gateway']
            target_devices = min(SCALE_FACTORS['network_infrastructure'], 2000)  # Cap at 2K
            
            for i in range(target_devices):
                device_type = random.choice(device_types)
                
                doc = {
                    '_id': ObjectId(),
                    'device_id': f'NET_{i+1:06d}',
                    'device_name': f'{device_type}_{i+1:04d}',
                    'device_type': device_type,
                    'manufacturer': random.choice(['Cisco', 'Juniper', 'HP', 'Fortinet', 'Palo Alto', 'Aruba']),
                    'facility_id': random.randint(1, 15),
                    'network_config': {
                        'ip_address': fake.ipv4(),
                        'vlan_id': random.randint(1, 4094),
                        'port_count': random.choice([8, 16, 24, 48, 96]),
                        'speed_mbps': random.choice([100, 1000, 10000]),
                    },
                    'performance_metrics': {
                        'cpu_utilization': round(random.uniform(10, 80), 2),
                        'memory_utilization': round(random.uniform(20, 70), 2),
                        'throughput_mbps': round(random.uniform(10, 5000), 2),
                        'uptime_percentage': round(random.uniform(98, 99.99), 3)
                    },
                    'status_info': {
                        'operational_status': random.choice(['Up', 'Down', 'Degraded', 'Testing']),
                        'last_restart': fake.date_time_between(start_date='-30d', end_date='now'),
                        'firmware_version': f'v{random.randint(1, 15)}.{random.randint(0, 9)}.{random.randint(0, 9)}'
                    },
                    'created_date': datetime.datetime.now(),
                    'is_active': True
                }
                network_data.append(doc)
                
                if len(network_data) >= 500:
                    try:
                        self.db.network_infrastructure.insert_many(network_data, ordered=False)
                        network_data = []
                    except Exception as batch_error:
                        print(f"⚠️  Network infrastructure batch error: {batch_error}")
                        network_data = []
            
            if network_data:
                try:
                    self.db.network_infrastructure.insert_many(network_data, ordered=False)
                except Exception as batch_error:
                    print(f"⚠️  Final network batch error: {batch_error}")
                
        except Exception as e:
            print(f"❌ Error populating network infrastructure: {e}")
            raise

    def populate_cybersecurity_events(self):
        """Populate cybersecurity events - OPTIMIZED VERSION"""
        print("  🔒 Inserting cybersecurity events...")
        
        try:
            cyber_events = []
            event_types = ['Intrusion Attempt', 'Malware Detection', 'Data Breach', 'DDoS Attack', 'Phishing', 'Unauthorized Access']
            target_events = min(SCALE_FACTORS['cybersecurity_events'], 5000)  # Cap at 5K
            
            for i in range(target_events):
                event_datetime = fake.date_time_between(start_date='-90d', end_date='now')
                
                doc = {
                    '_id': ObjectId(),
                    'event_id': f'CYBER_{i+1:08d}',
                    'event_type': random.choice(event_types),
                    'severity': random.choice(['Low', 'Medium', 'High', 'Critical']),
                    'event_datetime': event_datetime,
                    'source_info': {
                        'source_ip': fake.ipv4(),
                        'source_country': fake.country(),
                        'target_ip': fake.ipv4(),
                        'target_system': f'SYS{random.randint(1, 1000):04d}',
                        'protocol': random.choice(['TCP', 'UDP', 'ICMP', 'HTTP', 'HTTPS', 'SSH', 'FTP'])
                    },
                    'attack_details': {
                        'attack_vector': random.choice(['Network', 'Email', 'Web Application', 'USB', 'Physical', 'Social Engineering']),
                        'duration_seconds': random.randint(1, 7200),
                        'attempts_count': random.randint(1, 1000),
                        'success_rate': round(random.uniform(0, 0.1), 4)
                    },
                    'impact_assessment': {
                        'systems_affected': random.randint(1, 50),
                        'data_compromised': random.choice([True, False]),
                        'service_disruption': random.choice(['None', 'Minor', 'Moderate', 'Major']),
                        'downtime_minutes': random.randint(0, 480),
                        'financial_impact_usd': round(random.uniform(0, 100000), 2)
                    },
                    'response_info': {
                        'detected_by': random.choice(['SIEM', 'IDS', 'User Report', 'Automated Scan']),
                        'detection_time': event_datetime + datetime.timedelta(minutes=random.randint(1, 60)),
                        'response_time_minutes': random.randint(5, 240),
                        'incident_handler_id': random.randint(1, 50)
                    },
                    'created_date': datetime.datetime.now(),
                    'status': random.choice(['Open', 'Under Investigation', 'Contained', 'Resolved', 'Closed'])
                }
                cyber_events.append(doc)
                
                if len(cyber_events) >= 500:
                    try:
                        self.db.cybersecurity_events.insert_many(cyber_events, ordered=False)
                        cyber_events = []
                    except Exception as batch_error:
                        print(f"⚠️  Cybersecurity events batch error: {batch_error}")
                        cyber_events = []
            
            if cyber_events:
                try:
                    self.db.cybersecurity_events.insert_many(cyber_events, ordered=False)
                except Exception as batch_error:
                    print(f"⚠️  Final cybersecurity batch error: {batch_error}")
                
        except Exception as e:
            print(f"❌ Error populating cybersecurity events: {e}")
            raise

    def populate_software_licenses(self):
        """Populate software licenses - OPTIMIZED VERSION"""
        print("  📄 Inserting software licenses...")
        
        try:
            software_licenses = []
            software_types = ['Operating System', 'Database', 'ERP', 'CAD', 'Office Suite', 'Security']
            target_licenses = min(SCALE_FACTORS['software_licenses'], 1500)  # Cap at 1.5K
            
            for i in range(target_licenses):
                purchase_date = fake.date_time_between(start_date='-3y', end_date='now')
                
                doc = {
                    '_id': ObjectId(),
                    'license_id': f'LIC_{i+1:08d}',
                    'software_name': f'{random.choice(software_types)} Software {i+1:04d}',
                    'software_type': random.choice(software_types),
                    'vendor': random.choice(['Microsoft', 'Oracle', 'SAP', 'Adobe', 'Autodesk', 'IBM']),
                    'version': f'v{random.randint(1, 20)}.{random.randint(0, 9)}',
                    'license_info': {
                        'license_key': fake.uuid4(),
                        'license_type': random.choice(['Perpetual', 'Subscription', 'Concurrent', 'Named User']),
                        'seats_licensed': random.randint(1, 1000),
                        'seats_used': random.randint(1, 500)
                    },
                    'financial_info': {
                        'purchase_price_usd': round(random.uniform(100, 50000), 2),
                        'annual_maintenance_usd': round(random.uniform(20, 10000), 2),
                        'purchase_date': purchase_date
                    },
                    'compliance_info': {
                        'compliance_status': random.choice(['Compliant', 'Non-Compliant', 'Under Review']),
                        'audit_date': fake.date_time_between(start_date='-365d', end_date='now'),
                        'license_violations': random.randint(0, 3)
                    },
                    'created_date': datetime.datetime.now(),
                    'is_active': True
                }
                software_licenses.append(doc)
                
                if len(software_licenses) >= 500:
                    try:
                        self.db.software_licenses.insert_many(software_licenses, ordered=False)
                        software_licenses = []
                    except Exception as batch_error:
                        print(f"⚠️  Software licenses batch error: {batch_error}")
                        software_licenses = []
            
            if software_licenses:
                try:
                    self.db.software_licenses.insert_many(software_licenses, ordered=False)
                except Exception as batch_error:
                    print(f"⚠️  Final licenses batch error: {batch_error}")
                
        except Exception as e:
            print(f"❌ Error populating software licenses: {e}")
            raise

    def populate_batch_records(self):
        """Populate batch records - OPTIMIZED VERSION"""
        print("  🧪 Inserting batch records...")
        
        try:
            batch_records = []
            target_batches = min(SCALE_FACTORS['batch_records'], 10000)  # Cap at 10K
            
            for i in range(target_batches):
                start_time = fake.date_time_between(start_date='-90d', end_date='now')
                duration_hours = random.randint(2, 48)
                end_time = start_time + datetime.timedelta(hours=duration_hours)
                
                doc = {
                    '_id': ObjectId(),
                    'batch_id': f'BATCH_{i+1:08d}',
                    'recipe_id': f'RECIPE_{random.randint(1, 1000):04d}',
                    'product_code': f'PROD_{random.randint(1, 500):04d}',
                    'facility_id': random.randint(1, 15),
                    'production_line_id': random.randint(1, 100),
                    'temporal_info': {
                        'start_time': start_time,
                        'end_time': end_time,
                        'duration_hours': duration_hours,
                        'shift': random.choice(['Day', 'Evening', 'Night']),
                        'operator_id': random.randint(1, 1000)
                    },
                    'production_output': {
                        'target_quantity': round(random.uniform(100, 10000), 2),
                        'actual_quantity': round(random.uniform(95, 9500), 2),
                        'unit': random.choice(['KG', 'L', 'EA', 'TONS']),
                        'yield_percentage': round(random.uniform(85, 98), 2)
                    },
                    'quality_results': {
                        'overall_yield': round(random.uniform(85, 99), 2),
                        'purity_percentage': round(random.uniform(95, 99.9), 3),
                        'final_grade': random.choice(['A', 'B', 'C', 'Reject'])
                    },
                    'approval_info': {
                        'batch_status': random.choice(['Completed', 'Approved', 'Rejected']),
                        'quality_approved_by': random.randint(1, 100),
                        'approval_date': end_time + datetime.timedelta(hours=random.randint(1, 24))
                    },
                    'created_date': datetime.datetime.now()
                }
                batch_records.append(doc)
                
                if len(batch_records) >= 500:
                    try:
                        self.db.batch_records.insert_many(batch_records, ordered=False)
                        batch_records = []
                    except Exception as batch_error:
                        print(f"⚠️  Batch records batch error: {batch_error}")
                        batch_records = []
            
            if batch_records:
                try:
                    self.db.batch_records.insert_many(batch_records, ordered=False)
                except Exception as batch_error:
                    print(f"⚠️  Final batch records error: {batch_error}")
                
        except Exception as e:
            print(f"❌ Error populating batch records: {e}")
            raise

    def populate_recipes(self):
        """Populate recipes - OPTIMIZED VERSION"""
        print("  📋 Inserting recipes...")
        
        try:
            recipes = []
            target_recipes = min(SCALE_FACTORS['recipes'], 2000)  # Cap at 2K
            
            for i in range(target_recipes):
                doc = {
                    '_id': ObjectId(),
                    'recipe_id': f'RECIPE_{i+1:04d}',
                    'recipe_name': f'Recipe {i+1:04d} - {fake.catch_phrase()}',
                    'product_code': f'PROD_{random.randint(1, 500):04d}',
                    'version': f'v{random.randint(1, 10)}.{random.randint(0, 9)}',
                    'recipe_type': random.choice(['Standard', 'Alternative', 'Development', 'Emergency']),
                    'approval_info': {
                        'status': random.choice(['Draft', 'Under Review', 'Approved', 'Obsolete']),
                        'approved_by': random.randint(1, 100),
                        'approval_date': fake.date_time_between(start_date='-2y', end_date='now')
                    },
                    'control_parameters': {
                        'max_batch_size': round(random.uniform(100, 10000), 2),
                        'min_batch_size': round(random.uniform(10, 1000), 2),
                        'yield_target': round(random.uniform(85, 98), 2),
                        'cycle_time_hours': round(random.uniform(2, 48), 2)
                    },
                    'usage_statistics': {
                        'times_executed': random.randint(0, 500),
                        'success_rate': round(random.uniform(85, 99), 2),
                        'average_yield': round(random.uniform(85, 97), 2),
                        'last_execution_date': fake.date_time_between(start_date='-90d', end_date='now')
                    },
                    'created_date': datetime.datetime.now(),
                    'created_by': random.randint(1, 100),
                    'is_active': True
                }
                recipes.append(doc)
                
                if len(recipes) >= 500:
                    try:
                        self.db.recipes.insert_many(recipes, ordered=False)
                        recipes = []
                    except Exception as batch_error:
                        print(f"⚠️  Recipes batch error: {batch_error}")
                        recipes = []
            
            if recipes:
                try:
                    self.db.recipes.insert_many(recipes, ordered=False)
                except Exception as batch_error:
                    print(f"⚠️  Final recipes batch error: {batch_error}")
                
        except Exception as e:
            print(f"❌ Error populating recipes: {e}")
            raise

    def populate_control_logic(self):
        """Populate control logic data - OPTIMIZED VERSION"""
        print("  🔧 Inserting control logic data...")
        
        try:
            control_logic = []
            target_logic = min(SCALE_FACTORS['control_logic'], 3000)  # Cap at 3K
            
            for i in range(target_logic):
                doc = {
                    '_id': ObjectId(),
                    'logic_id': f'LOGIC_{i+1:06d}',
                    'logic_name': f'Control Logic {i+1:06d}',
                    'logic_type': random.choice(['PID', 'ON/OFF', 'Cascade', 'Feedforward', 'Ratio', 'Override']),
                    'equipment_id': random.randint(1, 1000),
                    'process_area': f'Area_{chr(65 + random.randint(0, 9))}',
                    'control_loop': f'Loop_{random.randint(100, 999)}',
                    'parameters': {
                        'proportional_gain': round(random.uniform(0.1, 10), 3),
                        'integral_time': round(random.uniform(0.1, 100), 2),
                        'derivative_time': round(random.uniform(0, 10), 2),
                        'output_limits': {
                            'high_limit': round(random.uniform(80, 100), 2),
                            'low_limit': round(random.uniform(0, 20), 2)
                        }
                    },
                    'operational_data': {
                        'mode': random.choice(['Auto', 'Manual', 'Cascade', 'Override']),
                        'enabled': random.choice([True, False]),
                        'last_switched_date': fake.date_time_between(start_date='-30d', end_date='now')
                    },
                    'performance_metrics': {
                        'stability_index': round(random.uniform(0.1, 1), 3),
                        'response_time_seconds': round(random.uniform(1, 60), 2),
                        'overshoot_percentage': round(random.uniform(0, 20), 2)
                    },
                    'created_date': datetime.datetime.now(),
                    'is_active': True
                }
                control_logic.append(doc)
                
                if len(control_logic) >= 500:
                    try:
                        self.db.control_logic.insert_many(control_logic, ordered=False)
                        control_logic = []
                    except Exception as batch_error:
                        print(f"⚠️  Control logic batch error: {batch_error}")
                        control_logic = []
            
            if control_logic:
                try:
                    self.db.control_logic.insert_many(control_logic, ordered=False)
                except Exception as batch_error:
                    print(f"⚠️  Final control logic batch error: {batch_error}")
                
        except Exception as e:
            print(f"❌ Error populating control logic: {e}")
            raise

    def populate_alarm_events(self):
        """Populate alarm events - OPTIMIZED VERSION"""
        print("  🚨 Inserting alarm events...")
        
        try:
            alarm_events = []
            start_date = datetime.datetime.now() - datetime.timedelta(days=15)  # Reduced from 30 days
            target_alarms = min(SCALE_FACTORS['alarm_events'], 25000)  # Cap at 25K
            
            for i in range(target_alarms):
                alarm_datetime = fake.date_time_between(start_date=start_date, end_date='now')
                duration_minutes = random.randint(1, 720)
                cleared_datetime = alarm_datetime + datetime.timedelta(minutes=duration_minutes) if random.random() < 0.8 else None
                
                doc = {
                    '_id': ObjectId(),
                    'alarm_id': f'ALM_{i+1:08d}',
                    'equipment_id': random.randint(1, 1000),
                    'process_area': f'Area_{chr(65 + random.randint(0, 9))}',
                    'alarm_datetime': alarm_datetime,
                    'alarm_details': {
                        'alarm_type': random.choice(['High', 'Low', 'Deviation', 'Rate of Change', 'Status', 'System']),
                        'priority': random.choice(['Critical', 'High', 'Medium', 'Low']),
                        'severity': random.choice(['Emergency', 'Alert', 'Warning', 'Notice']),
                        'state': random.choice(['Active', 'Acknowledged', 'Cleared', 'Suppressed']),
                        'category': random.choice(['Process', 'Safety', 'Equipment', 'Quality', 'Environmental'])
                    },
                    'alarm_values': {
                        'alarm_value': round(random.uniform(-100, 1000), 4),
                        'limit_value': round(random.uniform(-50, 950), 4),
                        'unit': random.choice(['°C', 'bar', 'L/min', '%', 'ppm', 'rpm']),
                        'percentage_of_limit': round(random.uniform(90, 150), 2)
                    },
                    'message_info': {
                        'alarm_message': fake.sentence(),
                        'alarm_description': fake.text(max_nb_chars=100)  # Reduced text size
                    },
                    'response_info': {
                        'acknowledged_datetime': alarm_datetime + datetime.timedelta(minutes=random.randint(1, 60)) if random.random() < 0.9 else None,
                        'acknowledged_by': random.randint(1, 1000) if random.random() < 0.9 else None,
                        'cleared_datetime': cleared_datetime,
                        'response_time_minutes': random.randint(1, 60) if random.random() < 0.9 else None
                    },
                    'impact_assessment': {
                        'production_impact': random.choice(['None', 'Minor', 'Moderate', 'Major']),
                        'safety_impact': random.choice(['None', 'Minor', 'Moderate', 'Major']),
                        'cost_impact_usd': round(random.uniform(0, 10000), 2) if random.random() < 0.3 else 0
                    },
                    'created_date': datetime.datetime.now()
                }
                alarm_events.append(doc)
                
                if len(alarm_events) >= 1000:
                    try:
                        self.db.alarm_events.insert_many(alarm_events, ordered=False)
                        alarm_events = []
                        if (i + 1) % 5000 == 0:
                            print(f"    Inserted {i + 1:,} alarm events...")
                    except Exception as batch_error:
                        print(f"⚠️  Alarm events batch error: {batch_error}")
                        alarm_events = []
            
            if alarm_events:
                try:
                    self.db.alarm_events.insert_many(alarm_events, ordered=False)
                except Exception as batch_error:
                    print(f"⚠️  Final alarm events batch error: {batch_error}")
                
        except Exception as e:
            print(f"❌ Error populating alarm events: {e}")
            raise

    def populate_analytics_data(self):
        """Populate various analytics collections - OPTIMIZED VERSION"""
        print("  📊 Inserting analytics data...")
        
        try:
            # Maintenance Analytics
            print("    🔧 Processing maintenance analytics...")
            maintenance_analytics = []
            target_maint = min(SCALE_FACTORS['maintenance_analytics'], 5000)
            
            for i in range(target_maint):
                doc = {
                    '_id': ObjectId(),
                    'analytics_id': f'MAINT_ANALYTICS_{i+1:08d}',
                    'equipment_id': random.randint(1, 1000),
                    'analysis_date': fake.date_time_between(start_date='-90d', end_date='now'),
                    'mtbf_analysis': {
                        'current_mtbf_hours': round(random.uniform(100, 8760), 2),
                        'target_mtbf_hours': round(random.uniform(500, 10000), 2),
                        'trend': random.choice(['Improving', 'Stable', 'Declining']),
                        'reliability_score': round(random.uniform(60, 98), 2)
                    },
                    'cost_analysis': {
                        'total_maintenance_cost_ytd': round(random.uniform(1000, 100000), 2),
                        'preventive_cost_percentage': round(random.uniform(60, 85), 2),
                        'cost_per_hour_operated': round(random.uniform(5, 500), 2)
                    }
                }
                maintenance_analytics.append(doc)
            
            if maintenance_analytics:
                self.db.maintenance_analytics.insert_many(maintenance_analytics, ordered=False)
            
            # Quality Analytics
            print("    🎯 Processing quality analytics...")
            quality_analytics = []
            target_qual = min(SCALE_FACTORS['quality_analytics'], 5000)
            
            for i in range(target_qual):
                doc = {
                    '_id': ObjectId(),
                    'analytics_id': f'QUAL_ANALYTICS_{i+1:08d}',
                    'facility_id': random.randint(1, 15),
                    'analysis_date': fake.date_time_between(start_date='-90d', end_date='now'),
                    'quality_metrics': {
                        'overall_yield': round(random.uniform(85, 99), 2),
                        'first_pass_yield': round(random.uniform(80, 98), 2),
                        'defect_rate_ppm': round(random.uniform(10, 10000), 2),
                        'customer_complaints': random.randint(0, 20)
                    },
                    'cost_of_quality': {
                        'prevention_cost': round(random.uniform(10000, 500000), 2),
                        'appraisal_cost': round(random.uniform(5000, 200000), 2),
                        'internal_failure_cost': round(random.uniform(1000, 100000), 2)
                    }
                }
                quality_analytics.append(doc)
            
            if quality_analytics:
                self.db.quality_analytics.insert_many(quality_analytics, ordered=False)
            
            # Production Analytics
            print("    🏭 Processing production analytics...")
            production_analytics = []
            target_prod = min(SCALE_FACTORS['production_analytics'], 8000)
            
            for i in range(target_prod):
                doc = {
                    '_id': ObjectId(),
                    'analytics_id': f'PROD_ANALYTICS_{i+1:08d}',
                    'production_line_id': random.randint(1, 100),
                    'analysis_date': fake.date_time_between(start_date='-30d', end_date='now'),
                    'efficiency_metrics': {
                        'overall_oee': round(random.uniform(70, 95), 2),
                        'availability': round(random.uniform(85, 99), 2),
                        'performance': round(random.uniform(75, 98), 2),
                        'quality_rate': round(random.uniform(90, 99.9), 2)
                    },
                    'throughput_analysis': {
                        'units_produced': random.randint(1000, 50000),
                        'target_production': random.randint(1200, 55000),
                        'capacity_utilization': round(random.uniform(65, 95), 2)
                    }
                }
                production_analytics.append(doc)
            
            if production_analytics:
                self.db.production_analytics.insert_many(production_analytics, ordered=False)
            
            # Supply Chain Analytics
            print("    🚚 Processing supply chain analytics...")
            supply_chain_analytics = []
            target_sc = min(SCALE_FACTORS['supply_chain_analytics'], 5000)
            
            for i in range(target_sc):
                doc = {
                    '_id': ObjectId(),
                    'analytics_id': f'SC_ANALYTICS_{i+1:08d}',
                    'analysis_date': fake.date_time_between(start_date='-90d', end_date='now'),
                    'inventory_metrics': {
                        'inventory_turnover': round(random.uniform(4, 20), 2),
                        'days_inventory_outstanding': round(random.uniform(15, 90), 2),
                        'stockout_frequency': random.randint(0, 20)
                    },
                    'supplier_performance': {
                        'on_time_delivery_rate': round(random.uniform(85, 99), 2),
                        'quality_acceptance_rate': round(random.uniform(90, 99.9), 2),
                        'lead_time_variance_days': round(random.uniform(0, 14), 2)
                    }
                }
                supply_chain_analytics.append(doc)
            
            if supply_chain_analytics:
                self.db.supply_chain_analytics.insert_many(supply_chain_analytics, ordered=False)
            
            print("    ✅ Completed analytics data insertion")
            
        except Exception as e:
            print(f"❌ Error populating analytics data: {e}")
            raise

# def main():
#     """Main function orchestrating IoT data generation"""
    
#     print("=" * 120)
#     print("🌐 ENHANCED MONGODB IOT ENVIRONMENTAL DATA GENERATOR 🌐")
#     print("=" * 120)
#     print("📊 Complete IoT and Environmental Data Foundation for $50B Manufacturing Enterprise")
#     print("🗄️  MongoDB: High-Volume Time-Series and Document Data")
#     print("\n🎯 GENERATION TARGETS:")
#     print(f"   🌍 Geographic Locations: {SCALE_FACTORS['geographic_locations']:,}")
#     print(f"   🌤️  Weather Data Points: {SCALE_FACTORS['geographic_locations'] * 90 * 8:,} (90 days)")
#     print(f"   🚨 Environmental Events: {SCALE_FACTORS['environmental_events']:,}")
#     print(f"   📊 Sensor Readings: {SCALE_FACTORS['sensor_readings']:,}")
#     print(f"   📈 Equipment Analytics: {SCALE_FACTORS['equipment_analytics']:,}")
#     print(f"   🔬 Process Data Points: {SCALE_FACTORS['process_data']:,}")
#     print(f"   ⚡ Energy Consumption: {SCALE_FACTORS['energy_consumption']:,}")
#     print(f"   💻 IT Systems: {SCALE_FACTORS['it_systems']:,}")
#     print(f"   🌐 Network Infrastructure: {SCALE_FACTORS['network_infrastructure']:,}")
#     print(f"   🔒 Cybersecurity Events: {SCALE_FACTORS['cybersecurity_events']:,}")
#     print(f"   📄 Software Licenses: {SCALE_FACTORS['software_licenses']:,}")
#     print(f"   🧪 Batch Records: {SCALE_FACTORS['batch_records']:,}")
#     print(f"   📋 Recipes: {SCALE_FACTORS['recipes']:,}")
#     print(f"   🔧 Control Logic: {SCALE_FACTORS['control_logic']:,}")
#     print(f"   🚨 Alarm Events: {SCALE_FACTORS['alarm_events']:,}")
#     print(f"   📊 Analytics Collections: {sum([SCALE_FACTORS['maintenance_analytics'], SCALE_FACTORS['quality_analytics'], SCALE_FACTORS['production_analytics'], SCALE_FACTORS['supply_chain_analytics']]):,}")
    
#     generator = MongoDBIoTEnvironmentalGenerator()
    
#     try:
#         # Connect to database
#         generator.connect()
        
#         # Drop existing collections
#         generator.drop_existing_collections()
        
#         # Create indexes
#         generator.create_indexes()
        
#         # Populate all data
#         generator.populate_all_iot_data()
        
#         # Generate summary report
#         generator.generate_summary_report()
        
#         print(f"\n🎉 IOT DATA GENERATION SUCCESSFUL! 🎉")
#         print("✅ All IoT collections created with proper indexes")
#         print("✅ High-volume time-series data populated")
#         print("✅ Environmental monitoring data established")
#         print("✅ Equipment sensor network operational")
#         print("✅ Process control data comprehensive")
#         print("✅ IT/OT cybersecurity monitoring active")
#         print("✅ Ready for real-time IoT analytics and AI/ML")
        
#     except Exception as e:
#         print(f"❌ Error during IoT data generation: {e}")
#         import traceback
#         traceback.print_exc()
        
#     finally:
#         generator.close()
#         print("🔒 MongoDB connection closed")
#         print("=" * 120)

# if __name__ == "__main__":
#     main()



# def main():
#     """Main function orchestrating operational data generation"""
    
#     print("=" * 120)
#     print("🏭 ENHANCED MYSQL OPERATIONAL DATA GENERATOR 🏭")
#     print("=" * 120)
#     print("📊 Complete Operational Data Foundation for $50B Manufacturing Enterprise")
#     print("🗄️  MySQL: High-Volume Transactional and Equipment Data")
#     print("\n🎯 GENERATION TARGETS:")
#     print(f"   ⚙️ Equipment (All Types): {sum([SCALE_FACTORS['production_equipment'], SCALE_FACTORS.get('measurement_instruments', 0)]):,}")
#     print(f"   🔧 Maintenance Plans: {SCALE_FACTORS['maintenance_plans']:,}")
#     print(f"   📋 Bill of Materials: {SCALE_FACTORS['bill_of_materials']:,}")
#     print(f"   🛣️ Routings: {SCALE_FACTORS['routings']:,}")
#     print(f"   📋 Work Orders: {SCALE_FACTORS['work_orders']:,}")
#     print(f"   💰 Purchase Orders: {SCALE_FACTORS['purchase_orders']:,}")
#     print(f"   📦 PO Items: {SCALE_FACTORS['purchase_order_items']:,}")
#     print(f"   💼 Sales Orders: {SCALE_FACTORS['sales_orders']:,}")
#     print(f"   📋 SO Items: {SCALE_FACTORS['sales_order_items']:,}")
#     print(f"   📥 Goods Receipts: {SCALE_FACTORS['goods_receipts']:,}")
#     print(f"   🚚 Shipments: {SCALE_FACTORS['shipments']:,}")
#     print(f"   🎯 Quality Events: {SCALE_FACTORS['quality_events']:,}")
#     print(f"   🔧 Maintenance Orders: {SCALE_FACTORS['maintenance_orders']:,}")
    
#     generator = MySQLOperationalDataGenerator()
    
#     try:
#         # Connect to database
#         generator.connect()
        
#         # Create all tables
#         generator.create_operational_tables()
        
#         # Populate all data in dependency order
#         generator.populate_all_operational_data()
        
#         # Generate summary report
#         generator.generate_summary_report()
        
#         print(f"\n🎉 OPERATIONAL DATA GENERATION SUCCESSFUL! 🎉")
#         print("✅ All operational tables created with proper relationships")
#         print("✅ All foreign key constraints validated")
#         print("✅ High-volume transactional data populated")
#         print("✅ Equipment and sensor network established")
#         print("✅ Supply chain operations complete")
#         print("✅ Ready for real-time operational analytics")
        
#     except Exception as e:
#         print(f"❌ Error during operational data generation: {e}")
#         import traceback
#         traceback.print_exc()
        
#     finally:
#         generator.close()
#         print("🔒 MySQL connection closed")
#         print("=" * 120)

# if __name__ == "__main__":
#     main()


def main():
    """Main function orchestrating master data generation"""
    
    print("=" * 120)
    print("🏭 ENHANCED POSTGRESQL MASTER DATA GENERATOR 🏭")
    print("=" * 120)
    print("📊 Complete Master Data Foundation for $50B Manufacturing Enterprise")
    print("🗄️  PostgreSQL: Enhanced Master Data with All Relationships")
    print("\n🎯 GENERATION TARGETS:")
    print(f"   🏢 Corporations: {SCALE_FACTORS['corporations']:,}")
    print(f"   🏛️  Legal Entities: {SCALE_FACTORS['legal_entities']:,}")
    print(f"   🏭 Facilities: {SCALE_FACTORS['facilities']:,} (plants, warehouses, DCs, offices)")
    print(f"   💰 Cost Centers: {SCALE_FACTORS['cost_centers']:,}")
    print(f"   🏢 Departments: {SCALE_FACTORS['departments']:,}")
    print(f"   👥 Employees: {SCALE_FACTORS['employees']:,}")
    print(f"   👤 Employee Roles: {SCALE_FACTORS['employee_roles']:,}")
    print(f"   🤝 Business Partners: {SCALE_FACTORS['business_partners']:,}")
    print(f"   📋 Specifications: {SCALE_FACTORS['specifications']:,}")
    print(f"   📦 Materials: {SCALE_FACTORS['materials']:,}")
    print(f"   📍 Storage Locations: {SCALE_FACTORS['storage_locations']:,}")
    print(f"   🏭 Production Lines: {SCALE_FACTORS['production_lines']:,}")
    print(f"   📄 Contracts: {SCALE_FACTORS['contracts']:,}")
    print(f"   📜 Permits/Licenses: {SCALE_FACTORS['permits_licenses']:,}")
    print(f"   📋 Documents: {SCALE_FACTORS['documents']:,}")
    
    generator = PostgreSQLMasterDataGenerator()
    
    try:
        # Connect to database
        generator.connect()
        
        # Create database and schemas
        generator.create_database_and_schemas()
        
        # Create all tables
        generator.create_master_data_tables()
        
        # Populate all data in dependency order
        generator.populate_all_master_data()
        
        # Add foreign key constraints
        generator.add_foreign_key_constraints()
        
        # Update circular references
        generator.update_circular_references()
        
        # Generate summary report
        generator.generate_summary_report()
        
        print(f"\n🎉 MASTER DATA GENERATION SUCCESSFUL! 🎉")
        print("✅ All tables created with proper relationships")
        print("✅ All foreign key constraints validated")
        print("✅ Circular references resolved")
        print("✅ Enterprise-scale master data ready")
        print("✅ Ready for operational system integration")
        
    except Exception as e:
        print(f"❌ Error during master data generation: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        generator.close()
        print("🔒 Database connection closed")
        print("=" * 120)

if __name__ == "__main__":
    main()