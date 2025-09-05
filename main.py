"""
Wakefit Complete Supply Chain Data Generation System
====================================================

This script generates realistic, correlated datasets for Wakefit's supply chain analytics.
All data follows business logic and maintains referential integrity.
"""

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta, date, time
import json
from faker import Faker
import uuid
import itertools
from typing import Dict, List, Tuple, Any
import warnings
warnings.filterwarnings('ignore')

# Initialize Faker
fake = Faker('en_IN')  # Indian locale for realistic Indian names and addresses

class WakefitDataGenerator:
    """Complete data generation system for Wakefit supply chain analytics"""
    
    def __init__(self, start_date='2023-01-01', end_date='2024-12-31'):
        self.start_date = datetime.strptime(start_date, '%Y-%m-%d')
        self.end_date = datetime.strptime(end_date, '%Y-%m-%d')
        self.current_date = self.end_date  # For "current" analysis
        
        # Business constants based on research
        self.MONTHLY_ORDERS = 8000  # 7500-9000 mattresses + furniture
        self.TOTAL_SKUS = 500
        self.MANUFACTURING_FACILITIES = 5
        self.DISTRIBUTION_CENTERS = 8
        self.STORES = 98
        self.STATES = 28
        
        # Initialize storage for generated data
        self.customers = None
        self.products = None
        self.facilities = None
        self.suppliers = None
        self.orders = None
        self.order_line_items = None
        self.inventory_movements = None
        self.production_batches = None
        self.purchase_orders = None
        self.logistics_shipments = None
        self.demand_forecasts = None
        self.supply_chain_events = None
        
        # Business logic parameters
        self.otif_target = 0.92  # 92% OTIF target
        self.seasonal_factors = {
            'MATTRESS': {1: 0.8, 2: 0.9, 3: 1.1, 4: 1.2, 5: 1.1, 6: 0.9, 
                        7: 0.8, 8: 0.9, 9: 1.1, 10: 1.3, 11: 1.4, 12: 1.2},
            'FURNITURE': {1: 0.9, 2: 1.0, 3: 1.2, 4: 1.1, 5: 1.0, 6: 0.8,
                         7: 0.7, 8: 0.8, 9: 1.0, 10: 1.2, 11: 1.3, 12: 1.1}
        }
        
        print("🏭 Wakefit Data Generator Initialized")
        print(f"📅 Date Range: {start_date} to {end_date}")
        print(f"🎯 Target Monthly Orders: {self.MONTHLY_ORDERS:,}")

    def generate_all_data(self) -> Dict[str, pd.DataFrame]:
        """Generate complete interconnected dataset"""
        print("\n🚀 Starting Complete Data Generation...")
        
        # Step 1: Generate Master Data
        print("\n📋 Generating Master Data...")
        self.customers = self.generate_customers()
        print(f"✅ Generated {len(self.customers):,} customers")
        
        self.products = self.generate_products()
        print(f"✅ Generated {len(self.products):,} products")
        
        self.facilities = self.generate_facilities()
        print(f"✅ Generated {len(self.facilities):,} facilities")
        
        self.suppliers = self.generate_suppliers()
        print(f"✅ Generated {len(self.suppliers):,} suppliers")
        
        # Step 2: Generate Transactional Data
        print("\n📦 Generating Transactional Data...")
        self.orders = self.generate_orders()
        print(f"✅ Generated {len(self.orders):,} orders")
        
        self.order_line_items = self.generate_order_line_items()
        print(f"✅ Generated {len(self.order_line_items):,} order line items")
        
        self.purchase_orders = self.generate_purchase_orders()
        print(f"✅ Generated {len(self.purchase_orders):,} purchase orders")
        
        self.production_batches = self.generate_production_batches()
        print(f"✅ Generated {len(self.production_batches):,} production batches")
        
        self.inventory_movements = self.generate_inventory_movements()
        print(f"✅ Generated {len(self.inventory_movements):,} inventory movements")
        
        self.logistics_shipments = self.generate_logistics_shipments()
        print(f"✅ Generated {len(self.logistics_shipments):,} shipments")
        
        # Step 3: Generate Analytical Data
        print("\n📊 Generating Analytical Data...")
        self.demand_forecasts = self.generate_demand_forecasts()
        print(f"✅ Generated {len(self.demand_forecasts):,} demand forecasts")
        
        self.supply_chain_events = self.generate_supply_chain_events()
        print(f"✅ Generated {len(self.supply_chain_events):,} supply chain events")
        
        # Return all datasets
        datasets = {
            'customers': self.customers,
            'products': self.products,
            'facilities': self.facilities,
            'suppliers': self.suppliers,
            'orders': self.orders,
            'order_line_items': self.order_line_items,
            'inventory_movements': self.inventory_movements,
            'production_batches': self.production_batches,
            'purchase_orders': self.purchase_orders,
            'logistics_shipments': self.logistics_shipments,
            'demand_forecasts': self.demand_forecasts,
            'supply_chain_events': self.supply_chain_events
        }
        
        print(f"\n🎉 Data Generation Complete! Generated {sum(len(df) for df in datasets.values()):,} total records")
        return datasets

    def generate_customers(self) -> pd.DataFrame:
        """Generate realistic customer master data"""
        num_customers = 50000  # Based on 8 lakh customers served
        
        # Indian cities for realistic distribution
        major_cities = ['Bangalore', 'Mumbai', 'Delhi', 'Hyderabad', 'Chennai', 'Pune', 'Kolkata', 'Ahmedabad',
                       'Jaipur', 'Lucknow', 'Kanpur', 'Nagpur', 'Indore', 'Bhopal', 'Visakhapatnam', 'Kochi']
        
        states = ['Karnataka', 'Maharashtra', 'Delhi', 'Telangana', 'Tamil Nadu', 'West Bengal', 'Gujarat',
                 'Rajasthan', 'Uttar Pradesh', 'Madhya Pradesh', 'Andhra Pradesh', 'Kerala']
        
        customers = []
        for i in range(num_customers):
            reg_date = fake.date_between(start_date=date(2018, 1, 1), end_date=date(2024, 11, 30))
            city = random.choice(major_cities)
            state = random.choice(states)
            
            # Customer segmentation logic
            segment_weights = {'REGULAR': 0.6, 'PREMIUM': 0.25, 'BULK': 0.1, 'PRICE_SENSITIVE': 0.05}
            segment = np.random.choice(list(segment_weights.keys()), p=list(segment_weights.values()))
            
            # Delivery sensitivity based on segment
            sensitivity_mapping = {'PREMIUM': (8, 10), 'REGULAR': (5, 7), 'BULK': (3, 5), 'PRICE_SENSITIVE': (2, 4)}
            sensitivity_range = sensitivity_mapping[segment]
            delivery_sensitivity = random.randint(*sensitivity_range)
            
            # Lifetime metrics based on segment and registration date
            days_active = (date(2024, 12, 31) - reg_date).days
            if segment == 'PREMIUM':
                order_frequency = random.randint(60, 120)  # Every 2-4 months
                avg_order_value = random.uniform(25000, 75000)
            elif segment == 'BULK':
                order_frequency = random.randint(180, 365)  # Every 6-12 months
                avg_order_value = random.uniform(100000, 500000)
            else:
                order_frequency = random.randint(180, 720)  # Every 6 months to 2 years
                avg_order_value = random.uniform(8000, 35000)
            
            estimated_orders = max(1, days_active // order_frequency)
            lifetime_value = estimated_orders * avg_order_value * random.uniform(0.7, 1.3)
            
            customer = {
                'customer_id': f"CUS-{reg_date.strftime('%Y%m%d')}-{str(i+1).zfill(4)}",
                'customer_type': 'B2B_HOSPITALITY' if segment == 'BULK' else 'B2C',
                'registration_date': reg_date,
                'primary_channel': np.random.choice(['WEBSITE', 'APP', 'AMAZON', 'FLIPKART', 'STORE'], 
                                                   p=[0.35, 0.25, 0.2, 0.15, 0.05]),
                'delivery_city': city,
                'delivery_state': state,
                'pincode': fake.postcode(),
                'customer_segment': segment,
                'delivery_sensitivity_score': delivery_sensitivity,
                'lifetime_orders': estimated_orders,
                'lifetime_value': round(lifetime_value, 2),
                'avg_order_frequency_days': order_frequency,
                'preferred_delivery_window': np.random.choice(['MORNING', 'AFTERNOON', 'EVENING', 'ANYTIME'],
                                                            p=[0.3, 0.25, 0.2, 0.25]),
                'last_order_date': fake.date_between(start_date=reg_date, end_date=date(2024, 12, 31))
            }
            customers.append(customer)
        
        return pd.DataFrame(customers)

    def generate_products(self) -> pd.DataFrame:
        """Generate comprehensive product catalog"""
        products = []
        
        # Product categories with realistic distribution
        categories = {
            'MATTRESS': {
                'sub_categories': ['MEMORY_FOAM', 'ORTHOPEDIC', 'LATEX', 'DUAL_COMFORT', 'POCKET_SPRING'],
                'sizes': ['SINGLE', 'DOUBLE', 'QUEEN', 'KING'],
                'thickness': [4, 5, 6, 8, 10],
                'base_price': 8000,
                'complexity': 'MEDIUM'
            },
            'BED': {
                'sub_categories': ['PLATFORM', 'STORAGE', 'UPHOLSTERED', 'WOODEN'],
                'sizes': ['SINGLE', 'DOUBLE', 'QUEEN', 'KING'],
                'base_price': 15000,
                'complexity': 'COMPLEX'
            },
            'SOFA': {
                'sub_categories': ['2_SEATER', '3_SEATER', 'L_SHAPED', 'RECLINER', 'SOFA_CUM_BED'],
                'sizes': ['COMPACT', 'REGULAR', 'LARGE'],
                'base_price': 25000,
                'complexity': 'COMPLEX'
            },
            'CHAIR': {
                'sub_categories': ['OFFICE', 'DINING', 'ACCENT', 'RECLINER'],
                'base_price': 5000,
                'complexity': 'SIMPLE'
            },
            'TABLE': {
                'sub_categories': ['DINING', 'COFFEE', 'STUDY', 'CONSOLE'],
                'sizes': ['2_SEATER', '4_SEATER', '6_SEATER', 'STANDARD'],
                'base_price': 8000,
                'complexity': 'MEDIUM'
            },
            'STORAGE': {
                'sub_categories': ['WARDROBE', 'CHEST', 'BOOKSHELF', 'SHOE_RACK'],
                'base_price': 12000,
                'complexity': 'COMPLEX'
            },
            'PILLOW': {
                'sub_categories': ['MEMORY_FOAM', 'FIBER', 'LATEX', 'CERVICAL'],
                'base_price': 800,
                'complexity': 'SIMPLE'
            },
            'BEDDING': {
                'sub_categories': ['COMFORTER', 'BEDSHEET', 'MATTRESS_PROTECTOR'],
                'sizes': ['SINGLE', 'DOUBLE', 'QUEEN', 'KING'],
                'base_price': 2000,
                'complexity': 'SIMPLE'
            }
        }
        
        sku_counter = 1
        for category, cat_info in categories.items():
            for sub_category in cat_info['sub_categories']:
                # Generate size variants if applicable
                sizes = cat_info.get('sizes', ['STANDARD'])
                
                for size in sizes:
                    # Generate thickness variants for mattresses
                    if category == 'MATTRESS':
                        for thickness in cat_info['thickness']:
                            sku_code = f"{category[:3]}-{sub_category[:6]}-{size[:3]}-{thickness}IN"
                            product_name = f"Wakefit {sub_category.replace('_', ' ')} {thickness}inch {size} Mattress"
                            
                            # Calculate dimensions and weight based on size and thickness
                            dimensions = self._get_mattress_dimensions(size, thickness)
                            weight = self._calculate_mattress_weight(size, thickness)
                            
                            products.append(self._create_product_record(
                                sku_code, product_name, category, sub_category, size, 
                                cat_info, weight, dimensions, thickness
                            ))
                            sku_counter += 1
                    else:
                        sku_code = f"{category[:3]}-{sub_category[:6]}-{size[:3]}-STD"
                        product_name = f"Wakefit {sub_category.replace('_', ' ')} {size}"
                        
                        dimensions, weight = self._get_product_dimensions_weight(category, sub_category, size)
                        
                        products.append(self._create_product_record(
                            sku_code, product_name, category, sub_category, size, 
                            cat_info, weight, dimensions
                        ))
                        sku_counter += 1
                        
                        if sku_counter > self.TOTAL_SKUS:
                            break
                    
                    if sku_counter > self.TOTAL_SKUS:
                        break
                if sku_counter > self.TOTAL_SKUS:
                    break
            if sku_counter > self.TOTAL_SKUS:
                break
        
        return pd.DataFrame(products)

    def _create_product_record(self, sku_code, product_name, category, sub_category, size, 
                              cat_info, weight, dimensions, thickness=None):
        """Create individual product record with business logic"""
        base_price = cat_info['base_price']
        complexity = cat_info['complexity']
        
        # Price calculation based on size, thickness, complexity
        size_multiplier = {'SINGLE': 0.7, 'DOUBLE': 1.0, 'QUEEN': 1.3, 'KING': 1.6, 
                          'COMPACT': 0.8, 'REGULAR': 1.0, 'LARGE': 1.4, 'STANDARD': 1.0}
        
        price = base_price * size_multiplier.get(size, 1.0)
        if thickness:
            price *= (1 + (thickness - 4) * 0.15)  # 15% increase per inch above 4"
        
        # Add random variation
        price *= random.uniform(0.8, 1.2)
        
        # Manufacturing time based on complexity
        complexity_hours = {'SIMPLE': random.uniform(0.5, 2), 'MEDIUM': random.uniform(2, 6), 
                           'COMPLEX': random.uniform(6, 24)}
        
        # Material requirements (simplified)
        materials = self._get_material_requirements(category, sub_category)
        
        return {
            'sku_code': sku_code,
            'product_name': product_name,
            'category': category,
            'sub_category': sub_category,
            'size_variant': size,
            'manufacturing_complexity': complexity,
            'standard_production_time_hours': round(complexity_hours[complexity], 2),
            'is_customizable': category in ['MATTRESS', 'BED', 'SOFA', 'STORAGE'],
            'weight_kg': round(weight, 2),
            'dimensions_lxwxh_cm': dimensions,
            'is_bulky_item': category in ['BED', 'SOFA', 'STORAGE'],
            'raw_materials_list': json.dumps(materials),
            'minimum_inventory_days': random.randint(7, 21),
            'maximum_inventory_days': random.randint(60, 180),
            'supplier_lead_time_days': random.randint(15, 45),
            'seasonal_demand_factor': self.seasonal_factors.get(category, 
                                    {i: random.uniform(0.8, 1.2) for i in range(1, 13)}),
            'price_inr': round(price, 2),
            'cost_inr': round(price * random.uniform(0.4, 0.7), 2),  # 40-70% cost ratio
            'launch_date': fake.date_between(start_date=date(2018, 1, 1), end_date=date(2024, 6, 30)),
            'discontinuation_date': None if random.random() > 0.05 else fake.date_between(
                start_date=date(2024, 1, 1), end_date=date(2025, 12, 31))
        }

    def _get_mattress_dimensions(self, size, thickness):
        """Get realistic mattress dimensions"""
        size_dims = {
            'SINGLE': '190x90',
            'DOUBLE': '190x120', 
            'QUEEN': '190x150',
            'KING': '190x180'
        }
        base_dim = size_dims[size]
        return f"{base_dim}x{thickness*2.54:.0f}"  # Convert inches to cm for height

    def _calculate_mattress_weight(self, size, thickness):
        """Calculate realistic mattress weight"""
        base_weights = {'SINGLE': 15, 'DOUBLE': 25, 'QUEEN': 35, 'KING': 45}
        return base_weights[size] * (thickness / 6)  # Scale by thickness

    def _get_product_dimensions_weight(self, category, sub_category, size):
        """Get dimensions and weight for non-mattress products"""
        # Simplified dimension and weight calculation
        category_weights = {
            'BED': (25, 80), 'SOFA': (40, 120), 'CHAIR': (8, 25),
            'TABLE': (15, 50), 'STORAGE': (30, 100), 'PILLOW': (0.5, 2),
            'BEDDING': (1, 3)
        }
        
        weight_range = category_weights.get(category, (5, 20))
        weight = random.uniform(*weight_range)
        
        # Generic dimensions
        if size in ['SINGLE', 'COMPACT']:
            dims = f"{random.randint(80,120)}x{random.randint(60,100)}x{random.randint(20,80)}"
        elif size in ['LARGE', 'KING']:
            dims = f"{random.randint(180,250)}x{random.randint(80,120)}x{random.randint(40,100)}"
        else:
            dims = f"{random.randint(120,180)}x{random.randint(70,100)}x{random.randint(30,90)}"
        
        return dims, weight

    def _get_material_requirements(self, category, sub_category):
        """Get material requirements for each product"""
        base_materials = {
            'MATTRESS': ['foam', 'fabric', 'zipper'],
            'BED': ['wood', 'hardware', 'finish'],
            'SOFA': ['wood_frame', 'foam', 'fabric', 'springs'],
            'CHAIR': ['wood', 'fabric', 'foam', 'hardware'],
            'TABLE': ['wood', 'finish', 'hardware'],
            'STORAGE': ['wood', 'hardware', 'finish'],
            'PILLOW': ['foam', 'fabric'],
            'BEDDING': ['fabric', 'filling']
        }
        return base_materials.get(category, ['basic_materials'])

    def generate_facilities(self) -> pd.DataFrame:
        """Generate facilities (manufacturing, warehouses, stores)"""
        facilities = []
        
        # Manufacturing facilities (5 locations as per research)
        manufacturing_locations = [
            ('Hosur', 'Tamil Nadu', '635109'),
            ('Bangalore', 'Karnataka', '560045'), 
            ('Chennai', 'Tamil Nadu', '600001'),
            ('Gurgaon', 'Haryana', '122001'),
            ('Pune', 'Maharashtra', '411001')
        ]
        
        for i, (city, state, pincode) in enumerate(manufacturing_locations):
            facilities.append({
                'facility_id': f"FAC-{city.upper()[:3]}-MFG",
                'facility_name': f"Wakefit Manufacturing - {city}",
                'facility_type': 'MANUFACTURING',
                'location_city': city,
                'location_state': state,
                'pincode': pincode,
                'capacity_units_per_day': random.randint(200, 500),
                'product_capabilities': json.dumps(['MATTRESS', 'BED', 'PILLOW'] if i < 3 else ['SOFA', 'CHAIR', 'TABLE', 'STORAGE']),
                'serving_regions': json.dumps([state] + random.sample(['Karnataka', 'Tamil Nadu', 'Maharashtra', 'Haryana'], 2)),
                'operational_status': 'ACTIVE',
                'setup_date': fake.date_between(start_date=date(2018, 1, 1), end_date=date(2022, 12, 31))
            })
        
        # Main Distribution Center (Hosur)
        facilities.append({
            'facility_id': 'FAC-HOS-DC',
            'facility_name': 'Wakefit Main Distribution Center - Hosur',
            'facility_type': 'DC',
            'location_city': 'Hosur',
            'location_state': 'Tamil Nadu',
            'pincode': '635109',
            'capacity_units_per_day': 2000,
            'product_capabilities': json.dumps(['ALL_PRODUCTS']),
            'serving_regions': json.dumps(['ALL_INDIA']),
            'operational_status': 'ACTIVE',
            'setup_date': date(2020, 6, 1)
        })
        
        # Regional Distribution Centers (8 locations)
        dc_locations = [
            ('Mumbai', 'Maharashtra'), ('Delhi', 'Delhi'), ('Bangalore', 'Karnataka'),
            ('Chennai', 'Tamil Nadu'), ('Hyderabad', 'Telangana'), ('Pune', 'Maharashtra'),
            ('Kolkata', 'West Bengal'), ('Ahmedabad', 'Gujarat')
        ]
        
        for city, state in dc_locations:
            facilities.append({
                'facility_id': f"FAC-{city.upper()[:3]}-DC",
                'facility_name': f"Wakefit Distribution Center - {city}",
                'facility_type': 'WAREHOUSE',
                'location_city': city,
                'location_state': state,
                'pincode': fake.postcode(),
                'capacity_units_per_day': random.randint(300, 800),
                'product_capabilities': json.dumps(['ALL_PRODUCTS']),
                'serving_regions': json.dumps([state]),
                'operational_status': 'ACTIVE',
                'setup_date': fake.date_between(start_date=date(2019, 1, 1), end_date=date(2023, 12, 31))
            })
        
        # Sample of retail stores (98 total, generating 30 for demo)
        store_cities = ['Mumbai', 'Delhi', 'Bangalore', 'Chennai', 'Hyderabad', 'Pune', 
                       'Kolkata', 'Ahmedabad', 'Jaipur', 'Lucknow'] * 3
        
        for i, city in enumerate(store_cities[:30]):
            facilities.append({
                'facility_id': f"FAC-{city.upper()[:3]}-ST{str(i+1).zfill(2)}",
                'facility_name': f"Wakefit Store - {city} {i+1}",
                'facility_type': 'STORE',
                'location_city': city,
                'location_state': random.choice(['Maharashtra', 'Delhi', 'Karnataka', 'Tamil Nadu', 'Telangana']),
                'pincode': fake.postcode(),
                'capacity_units_per_day': random.randint(10, 50),
                'product_capabilities': json.dumps(['MATTRESS', 'PILLOW', 'BEDDING']),
                'serving_regions': json.dumps([f"{city}_LOCAL"]),
                'operational_status': 'ACTIVE',
                'setup_date': fake.date_between(start_date=date(2022, 1, 1), end_date=date(2024, 6, 30))
            })
        
        return pd.DataFrame(facilities)

    def generate_suppliers(self) -> pd.DataFrame:
        """Generate supplier master data"""
        suppliers = []
        
        # Supplier types and their characteristics
        supplier_types = {
            'FOAM': {
                'countries': ['India', 'Germany', 'Belgium'],
                'lead_times': (15, 30),
                'moq_range': (100, 1000)
            },
            'FABRIC': {
                'countries': ['India', 'Turkey', 'China'],
                'lead_times': (20, 35),
                'moq_range': (500, 2000)
            },
            'SPRING': {
                'countries': ['India', 'Germany', 'Belgium'],
                'lead_times': (25, 40),
                'moq_range': (200, 800)
            },
            'WOOD': {
                'countries': ['India', 'Malaysia', 'Myanmar'],
                'lead_times': (30, 45),
                'moq_range': (50, 300)
            },
            'HARDWARE': {
                'countries': ['India', 'China', 'Taiwan'],
                'lead_times': (15, 25),
                'moq_range': (1000, 5000)
            },
            'PACKAGING': {
                'countries': ['India'],
                'lead_times': (10, 20),
                'moq_range': (500, 2000)
            }
        }
        
        supplier_id = 1
        for supplier_type, info in supplier_types.items():
            # Generate 3-5 suppliers per type for redundancy
            num_suppliers = random.randint(3, 5)
            
            for i in range(num_suppliers):
                country = random.choice(info['countries'])
                
                # Quality and reliability based on country and type
                if country == 'Germany' or country == 'Belgium':
                    quality_rating = random.uniform(4.2, 5.0)
                    reliability_rating = random.uniform(4.0, 4.8)
                    cost_competitiveness = 'HIGH'
                elif country == 'India':
                    quality_rating = random.uniform(3.5, 4.5)
                    reliability_rating = random.uniform(3.8, 4.6)
                    cost_competitiveness = 'LOW'
                else:
                    quality_rating = random.uniform(3.0, 4.2)
                    reliability_rating = random.uniform(3.2, 4.2)
                    cost_competitiveness = 'MEDIUM'
                
                suppliers.append({
                    'supplier_id': f"SUP-{country[:3].upper()}-{str(supplier_id).zfill(3)}",
                    'supplier_name': f"{fake.company()} {supplier_type.title()} Co.",
                    'supplier_country': country,
                    'supplier_type': supplier_type,
                    'materials_supplied': json.dumps([supplier_type.lower()]),
                    'standard_lead_time_days': random.randint(*info['lead_times']),
                    'minimum_order_quantity': random.randint(*info['moq_range']),
                    'quality_rating_5': round(quality_rating, 2),
                    'reliability_rating_5': round(reliability_rating, 2),
                    'cost_competitiveness': cost_competitiveness,
                    'contract_start_date': fake.date_between(start_date=date(2020, 1, 1), end_date=date(2023, 12, 31)),
                    'contract_end_date': fake.date_between(start_date=date(2024, 1, 1), end_date=date(2026, 12, 31)),
                    'payment_terms_days': random.choice([15, 30, 45, 60])
                })
                supplier_id += 1
        
        return pd.DataFrame(suppliers)

    def generate_orders(self) -> pd.DataFrame:
        """Generate realistic order data with business logic"""
        # Calculate total orders for the period
        total_days = (self.end_date - self.start_date).days
        total_orders = int((self.MONTHLY_ORDERS * (total_days / 30)) * random.uniform(0.9, 1.1))
        
        orders = []
        order_counter = 1
        
        # Generate orders day by day to maintain realistic patterns
        current_date = self.start_date
        while current_date <= self.end_date:
            # Daily order calculation with seasonality and day-of-week effects
            month = current_date.month
            day_of_week = current_date.weekday()
            
            # Base daily orders
            daily_base = self.MONTHLY_ORDERS / 30
            
            # Seasonal adjustment
            seasonal_factor = random.choice([0.9, 1.0, 1.1, 1.2])  # Simplified seasonality
            
            # Day of week adjustment (higher on weekends)
            dow_factors = {0: 1.0, 1: 1.0, 2: 1.0, 3: 1.0, 4: 1.1, 5: 1.3, 6: 1.4}  # Mon-Sun
            dow_factor = dow_factors[day_of_week]
            
            # Calculate daily orders
            daily_orders = int(daily_base * seasonal_factor * dow_factor * random.uniform(0.7, 1.3))
            
            # Generate orders for this day
            for i in range(daily_orders):
                if order_counter > total_orders:
                    break
                
                # Select customer (80% existing, 20% might be new)
                customer = self.customers.sample(n=1).iloc[0]
                
                # Order timing during the day
                order_time = time(
                    hour=random.randint(6, 23),
                    minute=random.randint(0, 59),
                    second=random.randint(0, 59)
                )
                
                # Channel distribution based on customer preference
                channel_weights = {
                    'WEBSITE': 0.35, 'APP': 0.25, 'AMAZON': 0.2, 'FLIPKART': 0.15, 'STORE': 0.05
                }
                if customer['primary_channel'] in channel_weights:
                    # 70% chance customer uses their preferred channel
                    if random.random() < 0.7:
                        channel = customer['primary_channel']
                    else:
                        channel = np.random.choice(list(channel_weights.keys()), p=list(channel_weights.values()))
                else:
                    channel = np.random.choice(list(channel_weights.keys()), p=list(channel_weights.values()))
                
                # Delivery expectation based on customer segment and channel
                if customer['customer_segment'] == 'PREMIUM':
                    delivery_days = random.randint(2, 5)
                elif channel in ['AMAZON', 'FLIPKART']:
                    delivery_days = random.randint(3, 7)
                else:
                    delivery_days = random.randint(5, 12)
                
                customer_expectation = current_date + timedelta(days=delivery_days)
                
                # Wakefit's promise (usually 1-2 days more than customer expectation)
                promised_delivery = customer_expectation + timedelta(days=random.randint(1, 3))
                
                # Order items count based on customer segment
                if customer['customer_segment'] == 'BULK':
                    total_items = random.randint(5, 20)
                elif customer['customer_segment'] == 'PREMIUM':
                    total_items = random.randint(2, 8)
                else:
                    total_items = random.randint(1, 4)
                
                # Payment method based on channel and customer segment
                if channel in ['AMAZON', 'FLIPKART']:
                    payment_method = np.random.choice(['PREPAID', 'COD'], p=[0.6, 0.4])
                elif customer['customer_segment'] == 'PREMIUM':
                    payment_method = np.random.choice(['PREPAID', 'EMI', 'COD'], p=[0.5, 0.3, 0.2])
                else:
                    payment_method = np.random.choice(['COD', 'PREPAID'], p=[0.6, 0.4])
                
                # Order priority
                if customer['customer_segment'] == 'PREMIUM':
                    priority = np.random.choice(['URGENT', 'STANDARD'], p=[0.3, 0.7])
                elif customer['customer_segment'] == 'BULK':
                    priority = 'BULK'
                else:
                    priority = 'STANDARD'
                
                # Trial order flag (applicable for mattresses)
                is_trial = random.random() < 0.15  # 15% of orders are trial orders
                
                # Calculate delivery performance (OTIF simulation)
                # 92% OTIF target with variations by channel and customer segment
                otif_probability = self.otif_target
                if channel in ['AMAZON', 'FLIPKART']:
                    otif_probability -= 0.05  # Marketplace orders slightly more complex
                if customer['delivery_sensitivity_score'] >= 8:
                    otif_probability += 0.03  # Extra care for sensitive customers
                
                # Determine if this order will be OTIF
                will_be_otif = random.random() < otif_probability
                
                if will_be_otif:
                    # On-time delivery
                    actual_delivery = promised_delivery - timedelta(days=random.randint(0, 2))
                    delay_days = (actual_delivery - promised_delivery).days
                    otif_status = 'ON_TIME_IN_FULL'
                    delivery_status = 'DELIVERED'
                else:
                    # Late delivery
                    delay_days = random.randint(1, 8)
                    actual_delivery = promised_delivery + timedelta(days=delay_days)
                    
                    if delay_days <= 2:
                        otif_status = 'LATE'
                    elif delay_days <= 5:
                        otif_status = 'LATE' if random.random() < 0.8 else 'INCOMPLETE'
                    else:
                        otif_status = np.random.choice(['LATE', 'INCOMPLETE', 'FAILED'], p=[0.5, 0.3, 0.2])
                    
                    delivery_status = 'DELIVERED' if otif_status != 'FAILED' else 'FAILED'
                
                # Estimated dispatch date (working backwards from delivery)
                estimated_dispatch = actual_delivery - timedelta(days=random.randint(1, 3))
                actual_dispatch = estimated_dispatch + timedelta(days=random.randint(-1, 2))
                
                # Customer satisfaction based on delivery performance
                if otif_status == 'ON_TIME_IN_FULL':
                    satisfaction = random.randint(4, 5)
                    nps = random.randint(7, 10)
                elif otif_status == 'LATE' and delay_days <= 2:
                    satisfaction = random.randint(3, 4)
                    nps = random.randint(5, 8)
                else:
                    satisfaction = random.randint(1, 3)
                    nps = random.randint(-10, 5)
                
                order = {
                    'order_id': f"ORD-{current_date.strftime('%Y%m%d')}-{str(order_counter).zfill(6)}",
                    'customer_id': customer['customer_id'],
                    'order_date': current_date,
                    'order_time': order_time,
                    'channel': channel,
                    'store_id': None,  # Will be filled for store orders
                    'total_items': total_items,
                    'total_quantity': 0,  # Will be calculated from line items
                    'gross_order_value': 0,  # Will be calculated from line items
                    'discount_amount': 0,  # Will be calculated
                    'net_order_value': 0,  # Will be calculated
                    'payment_method': payment_method,
                    'payment_status': 'PAID' if payment_method != 'COD' else 'PENDING',
                    'customer_delivery_expectation': customer_expectation,
                    'promised_delivery_date': promised_delivery,
                    'delivery_address_full': f"{fake.street_address()}, {customer['delivery_city']}, {customer['delivery_state']}",
                    'delivery_pincode': customer['pincode'],
                    'delivery_instructions': fake.sentence() if random.random() < 0.3 else None,
                    'order_priority': priority,
                    'is_trial_order': is_trial,
                    'estimated_dispatch_date': estimated_dispatch,
                    'actual_dispatch_date': actual_dispatch if current_date < self.current_date else None,
                    'estimated_delivery_date': promised_delivery,
                    'actual_delivery_date': actual_delivery if current_date < self.current_date else None,
                    'delivery_status': delivery_status if current_date < self.current_date else 'PENDING',
                    'delivery_attempts': random.randint(1, 3) if delivery_status == 'DELIVERED' else 1,
                    'otif_status': otif_status if current_date < self.current_date else 'PENDING',
                    'delay_days': delay_days if current_date < self.current_date else 0,
                    'customer_satisfaction_rating': satisfaction if current_date < self.current_date else None,
                    'nps_score': nps if current_date < self.current_date else None
                }
                
                orders.append(order)
                order_counter += 1
            
            current_date += timedelta(days=1)
            
            if order_counter > total_orders:
                break
        
        return pd.DataFrame(orders)

    def generate_order_line_items(self) -> pd.DataFrame:
        """Generate order line items based on orders"""
        line_items = []
        line_counter = 1
        
        for _, order in self.orders.iterrows():
            total_items = order['total_items']
            order_value = 0
            order_quantity = 0
            
            # Generate line items for this order
            for item_seq in range(total_items):
                # Product selection based on customer segment and seasonality
                if order['customer_id'].startswith('CUS-') and 'BULK' in order['order_priority']:
                    # Bulk orders prefer mattresses and basic furniture
                    product = self.products[
                        self.products['category'].isin(['MATTRESS', 'BED', 'PILLOW'])
                    ].sample(n=1).iloc[0]
                    quantity = random.randint(5, 20)
                else:
                    # Regular customers
                    product = self.products.sample(n=1).iloc[0]
                    if product['category'] == 'MATTRESS':
                        quantity = 1  # Usually one mattress
                    elif product['category'] in ['PILLOW', 'BEDDING']:
                        quantity = random.randint(1, 4)
                    else:
                        quantity = random.randint(1, 2)
                
                # Pricing with discounts
                unit_price = product['price_inr']
                if order['channel'] in ['AMAZON', 'FLIPKART']:
                    # Marketplace orders might have different pricing
                    unit_price *= random.uniform(0.95, 1.05)
                
                line_total = unit_price * quantity
                
                # Customization based on product type
                is_customized = product['is_customizable'] and random.random() < 0.2
                customization_details = None
                if is_customized:
                    customization_details = {
                        'custom_size': f"{random.randint(180,200)}x{random.randint(90,180)}x{random.randint(15,25)}",
                        'custom_firmness': random.choice(['SOFT', 'MEDIUM', 'FIRM']),
                        'special_requirements': fake.sentence()
                    }
                
                # Manufacturing and quality timeline
                production_lead_time = product['standard_production_time_hours'] / 24  # Convert to days
                if is_customized:
                    production_lead_time *= 1.5  # Custom products take longer
                
                estimated_manufacturing = order['order_date'] + timedelta(days=int(production_lead_time) + random.randint(1, 3))
                
                # Select manufacturing facility based on product capability
                available_facilities = []
                for _, facility in self.facilities[self.facilities['facility_type'] == 'MANUFACTURING'].iterrows():
                    capabilities = json.loads(facility['product_capabilities'])
                    if product['category'] in capabilities or 'ALL_PRODUCTS' in capabilities:
                        available_facilities.append(facility['facility_id'])
                
                manufacturing_facility = random.choice(available_facilities) if available_facilities else 'FAC-BAN-MFG'
                
                # Quality check status based on order delivery status
                if order['actual_delivery_date'] and pd.notna(order['actual_delivery_date']):
                    qc_status = 'PASSED' if order['otif_status'] == 'ON_TIME_IN_FULL' else random.choice(['PASSED', 'REWORK'])
                    qc_date = estimated_manufacturing + timedelta(days=1)
                    actual_manufacturing = estimated_manufacturing + timedelta(days=random.randint(-1, 2))
                    
                    # Delivered quantities based on OTIF status
                    if order['otif_status'] == 'INCOMPLETE':
                        quantity_delivered = int(quantity * random.uniform(0.6, 0.9))
                    elif order['otif_status'] == 'FAILED':
                        quantity_delivered = 0
                    else:
                        quantity_delivered = quantity
                        
                    quantity_dispatched = quantity_delivered
                    line_status = 'DELIVERED' if quantity_delivered > 0 else 'FAILED'
                else:
                    # Future or pending orders
                    qc_status = 'PENDING'
                    qc_date = None
                    actual_manufacturing = None
                    quantity_delivered = 0
                    quantity_dispatched = 0
                    line_status = 'PENDING'
                
                # Dispatch facility (usually same as manufacturing or nearest DC)
                dispatch_facility = manufacturing_facility
                
                line_item = {
                    'line_item_id': f"LI-{order['order_id']}-{str(item_seq+1).zfill(3)}",
                    'order_id': order['order_id'],
                    'sku_code': product['sku_code'],
                    'quantity_ordered': quantity,
                    'quantity_confirmed': quantity,
                    'quantity_dispatched': quantity_dispatched,
                    'quantity_delivered': quantity_delivered,
                    'unit_price': round(unit_price, 2),
                    'line_total': round(line_total, 2),
                    'customization_details': json.dumps(customization_details) if customization_details else None,
                    'estimated_manufacturing_date': estimated_manufacturing,
                    'actual_manufacturing_date': actual_manufacturing,
                    'manufacturing_facility_id': manufacturing_facility,
                    'quality_check_status': qc_status,
                    'quality_check_date': qc_date,
                    'inventory_allocation_time': order['order_date'] + timedelta(hours=random.randint(1, 24)),
                    'line_item_status': line_status,
                    'dispatch_facility_id': dispatch_facility
                }
                
                line_items.append(line_item)
                order_value += line_total
                order_quantity += quantity
                line_counter += 1
        
        # Update orders with calculated values
        line_items_df = pd.DataFrame(line_items)
        
        # Calculate order totals
        for idx, order in self.orders.iterrows():
            order_lines = line_items_df[line_items_df['order_id'] == order['order_id']]
            gross_value = order_lines['line_total'].sum()
            total_qty = order_lines['quantity_ordered'].sum()
            
            # Apply discounts
            discount_rate = 0
            if order['channel'] in ['WEBSITE', 'APP']:
                discount_rate = random.uniform(0.05, 0.15)  # 5-15% discount
            elif order['customer_id'] in self.customers[self.customers['customer_segment'] == 'BULK']['customer_id'].values:
                discount_rate = random.uniform(0.10, 0.25)  # Bulk discounts
            
            discount_amount = gross_value * discount_rate
            net_value = gross_value - discount_amount
            
            self.orders.at[idx, 'total_quantity'] = int(total_qty)
            self.orders.at[idx, 'gross_order_value'] = round(gross_value, 2)
            self.orders.at[idx, 'discount_amount'] = round(discount_amount, 2)
            self.orders.at[idx, 'net_order_value'] = round(net_value, 2)
        
        return line_items_df

    def generate_purchase_orders(self) -> pd.DataFrame:
        """Generate purchase orders for raw materials"""
        purchase_orders = []
        po_counter = 1
        
        # Calculate material requirements based on production needs
        production_volume = len(self.order_line_items) * 1.2  # 20% buffer for safety stock
        
        # Generate POs for each supplier type
        for _, supplier in self.suppliers.iterrows():
            material_type = supplier['supplier_type']
            
            # Number of POs per supplier (based on lead time and demand)
            days_in_period = (self.end_date - self.start_date).days
            po_frequency_days = supplier['standard_lead_time_days'] * 2  # Reorder every 2x lead time
            num_pos = max(1, int(days_in_period / po_frequency_days))
            
            for po_seq in range(num_pos):
                # PO date
                po_date = self.start_date + timedelta(days=po_seq * po_frequency_days + random.randint(0, 7))
                
                # Required delivery date (based on production schedule)
                required_delivery = po_date + timedelta(days=random.randint(20, 40))
                
                # Supplier promise (usually matches or is slightly delayed)
                supplier_reliability = supplier['reliability_rating_5']
                if supplier_reliability >= 4.5:
                    promised_delivery = required_delivery + timedelta(days=random.randint(0, 2))
                elif supplier_reliability >= 3.5:
                    promised_delivery = required_delivery + timedelta(days=random.randint(1, 5))
                else:
                    promised_delivery = required_delivery + timedelta(days=random.randint(3, 10))
                
                # Actual delivery based on supplier reliability
                delivery_delay_probability = (5 - supplier_reliability) / 5  # Higher rating = lower delay probability
                
                if random.random() < delivery_delay_probability:
                    # Delayed delivery
                    delay_days = random.randint(1, 15)
                    actual_delivery = promised_delivery + timedelta(days=delay_days)
                    delay_reason = random.choice([
                        'SUPPLIER_CAPACITY_ISSUE', 'LOGISTICS_DELAY', 'QUALITY_REWORK', 
                        'CUSTOMS_DELAY', 'WEATHER_DISRUPTION'
                    ])
                else:
                    # On-time delivery
                    actual_delivery = promised_delivery + timedelta(days=random.randint(-2, 1))
                    delay_days = 0
                    delay_reason = None
                
                # Quantity and pricing
                moq = supplier['minimum_order_quantity']
                order_quantity = moq * random.randint(1, 5)
                
                # Unit cost based on supplier cost competitiveness
                base_cost_per_unit = {
                    'FOAM': 150, 'FABRIC': 200, 'SPRING': 300, 'WOOD': 500, 
                    'HARDWARE': 25, 'PACKAGING': 10
                }
                base_cost = base_cost_per_unit.get(material_type, 100)
                
                cost_multiplier = {'LOW': 0.8, 'MEDIUM': 1.0, 'HIGH': 1.2}
                unit_cost = base_cost * cost_multiplier[supplier['cost_competitiveness']] * random.uniform(0.8, 1.2)
                
                total_value = order_quantity * unit_cost
                
                # Quality inspection
                quality_rating = supplier['quality_rating_5']
                if quality_rating >= 4.5:
                    quality_status = 'PASSED'
                    rejection_qty = 0
                elif quality_rating >= 3.5:
                    quality_status = random.choice(['PASSED', 'PARTIAL'], p=[0.8, 0.2])
                    rejection_qty = order_quantity * random.uniform(0, 0.1) if quality_status == 'PARTIAL' else 0
                else:
                    quality_status = random.choice(['PASSED', 'PARTIAL', 'FAILED'], p=[0.6, 0.3, 0.1])
                    rejection_qty = order_quantity * random.uniform(0.05, 0.2) if quality_status != 'PASSED' else 0
                
                delivered_quantity = order_quantity - rejection_qty
                
                # Receiving facility (usually manufacturing facilities)
                receiving_facility = random.choice(self.facilities[
                    self.facilities['facility_type'] == 'MANUFACTURING'
                ]['facility_id'].values)
                
                # PO status
                if po_date < self.current_date - timedelta(days=60):
                    po_status = 'CLOSED'
                elif actual_delivery <= self.current_date:
                    po_status = 'RECEIVED'
                elif po_date <= self.current_date:
                    po_status = random.choice(['CONFIRMED', 'SHIPPED'])
                else:
                    po_status = 'PENDING'
                
                purchase_order = {
                    'po_id': f"PO-{supplier['supplier_id'][-3:]}-{po_date.strftime('%Y%m%d')}-{str(po_seq+1).zfill(2)}",
                    'supplier_id': supplier['supplier_id'],
                    'po_date': po_date,
                    'required_delivery_date': required_delivery,
                    'promised_delivery_date': promised_delivery,
                    'actual_delivery_date': actual_delivery if po_status in ['RECEIVED', 'CLOSED'] else None,
                    'receiving_facility_id': receiving_facility,
                    'material_type': material_type,
                    'quantity_ordered': order_quantity,
                    'quantity_delivered': delivered_quantity if po_status in ['RECEIVED', 'CLOSED'] else 0,
                    'unit_cost': round(unit_cost, 2),
                    'total_po_value': round(total_value, 2),
                    'currency': 'USD' if supplier['supplier_country'] != 'India' else 'INR',
                    'payment_terms': f"{supplier['payment_terms_days']} days",
                    'quality_inspection_status': quality_status if po_status in ['RECEIVED', 'CLOSED'] else 'PENDING',
                    'quality_rejection_quantity': round(rejection_qty, 2),
                    'supplier_delay_days': delay_days,
                    'delay_reason': delay_reason,
                    'po_status': po_status
                }
                
                purchase_orders.append(purchase_order)
                po_counter += 1
        
        return pd.DataFrame(purchase_orders)

    def generate_production_batches(self) -> pd.DataFrame:
        """Generate production batch records"""
        production_batches = []
        batch_counter = 1
        
        # Group line items by manufacturing facility and date for realistic batching
        manufacturing_items = self.order_line_items[
            self.order_line_items['estimated_manufacturing_date'].notna()
        ].copy()
        
        # Group by facility and date
        for facility_id in manufacturing_items['manufacturing_facility_id'].unique():
            facility_items = manufacturing_items[
                manufacturing_items['manufacturing_facility_id'] == facility_id
            ]
            
            # Group by SKU and date for batching
            for sku_code in facility_items['sku_code'].unique():
                sku_items = facility_items[facility_items['sku_code'] == sku_code]
                
                # Group by week for batch production
                sku_items['week'] = sku_items['estimated_manufacturing_date'].dt.isocalendar().week
                sku_items['year'] = sku_items['estimated_manufacturing_date'].dt.year
                
                for (year, week), week_items in sku_items.groupby(['year', 'week']):
                    if len(week_items) == 0:
                        continue
                    
                    # Production date (usually Monday of that week)
                    production_date = week_items['estimated_manufacturing_date'].min()
                    
                    # Get product details
                    product = self.products[self.products['sku_code'] == sku_code].iloc[0]
                    
                    # Calculate batch quantities
                    planned_quantity = week_items['quantity_ordered'].sum()
                    
                    # Production efficiency based on complexity and facility
                    complexity_efficiency = {
                        'SIMPLE': random.uniform(0.9, 1.0),
                        'MEDIUM': random.uniform(0.8, 0.95),
                        'COMPLEX': random.uniform(0.7, 0.9)
                    }
                    efficiency = complexity_efficiency[product['manufacturing_complexity']]
                    
                    actual_quantity_produced = int(planned_quantity * efficiency)
                    
                    # Quality pass rate
                    quality_pass_rate = random.uniform(0.92, 0.99)
                    actual_quantity_passed = int(actual_quantity_produced * quality_pass_rate)
                    
                    # Production timing
                    standard_time_hours = product['standard_production_time_hours']
                    total_time_needed = planned_quantity * standard_time_hours
                    actual_time_taken = total_time_needed / efficiency
                    
                    production_start = datetime.combine(production_date, time(8, 0)) + timedelta(hours=random.randint(0, 2))
                    production_end = production_start + timedelta(hours=actual_time_taken)
                    
                    # Shift determination
                    start_hour = production_start.hour
                    if start_hour < 16:
                        shift = 'MORNING'
                    elif start_hour < 24:
                        shift = 'AFTERNOON'
                    else:
                        shift = 'NIGHT'
                    
                    # Materials consumed (simplified)
                    materials_consumed = json.loads(product['raw_materials_list'])
                    material_consumption = {material: planned_quantity * random.uniform(0.8, 1.2) for material in materials_consumed}
                    
                    # Cost calculation
                    material_cost_per_unit = product['cost_inr'] * 0.6  # 60% material cost
                    labor_cost_per_unit = product['cost_inr'] * 0.25   # 25% labor cost
                    overhead_cost_per_unit = product['cost_inr'] * 0.15 # 15% overhead
                    
                    production_cost_per_unit = material_cost_per_unit + labor_cost_per_unit + overhead_cost_per_unit
                    
                    # Quality issues
                    quality_issues = []
                    if quality_pass_rate < 0.95:
                        issues = random.choice([
                            'FOAM_DENSITY_VARIATION', 'FABRIC_DEFECT', 'DIMENSION_TOLERANCE',
                            'STITCHING_ISSUE', 'FINISHING_DEFECT'
                        ])
                        quality_issues.append(issues)
                    
                    batch = {
                        'batch_id': f"BATCH-{facility_id[-3:]}-{production_date.strftime('%Y%m%d')}-{str(batch_counter).zfill(2)}",
                        'sku_code': sku_code,
                        'manufacturing_facility_id': facility_id,
                        'production_date': production_date,
                        'production_shift': shift,
                        'planned_quantity': planned_quantity,
                        'actual_quantity_produced': actual_quantity_produced,
                        'actual_quantity_passed_qc': actual_quantity_passed,
                        'production_start_time': production_start,
                        'production_end_time': production_end,
                        'total_production_time_hours': round(actual_time_taken, 2),
                        'materials_consumed': json.dumps(material_consumption),
                        'labor_hours': round(actual_time_taken * 0.7, 2),  # 70% of time is labor
                        'machine_hours': round(actual_time_taken * 0.3, 2), # 30% is machine time
                        'quality_issues_found': json.dumps(quality_issues),
                        'production_cost_per_unit': round(production_cost_per_unit, 2),
                        'batch_supervisor': fake.name(),
                        'efficiency_percentage': round(efficiency * 100, 2)
                    }
                    
                    production_batches.append(batch)
                    batch_counter += 1
        
        return pd.DataFrame(production_batches)

    def generate_inventory_movements(self) -> pd.DataFrame:
        """Generate inventory movement transactions"""
        movements = []
        movement_counter = 1
        
        # Generate movements for production (IN)
        for _, batch in self.production_batches.iterrows():
            movement = {
                'movement_id': f"INV-{batch['manufacturing_facility_id'][-3:]}-{batch['production_date'].strftime('%Y%m%d%H%M%S')}-{str(movement_counter).zfill(4)}",
                'sku_code': batch['sku_code'],
                'facility_id': batch['manufacturing_facility_id'],
                'movement_date': batch['production_date'],
                'movement_time': batch['production_end_time'].time(),
                'movement_type': 'PRODUCTION_IN',
                'quantity_change': batch['actual_quantity_passed_qc'],
                'previous_stock': 0,  # Will be calculated
                'new_stock': batch['actual_quantity_passed_qc'],  # Will be calculated
                'reference_id': batch['batch_id'],
                'batch_number': batch['batch_id'],
                'expiry_date': None,
                'cost_per_unit': batch['production_cost_per_unit'],
                'movement_reason': f"Production completed - Batch {batch['batch_id']}"
            }
            movements.append(movement)
            movement_counter += 1
        
        # Generate movements for sales (OUT)
        for _, line_item in self.order_line_items.iterrows():
            if line_item['quantity_dispatched'] > 0:
                dispatch_date = self.orders[self.orders['order_id'] == line_item['order_id']]['actual_dispatch_date'].iloc[0]
                if pd.notna(dispatch_date):
                    movement = {
                        'movement_id': f"INV-{line_item['dispatch_facility_id'][-3:]}-{dispatch_date.strftime('%Y%m%d%H%M%S')}-{str(movement_counter).zfill(4)}",
                        'sku_code': line_item['sku_code'],
                        'facility_id': line_item['dispatch_facility_id'],
                        'movement_date': dispatch_date,
                        'movement_time': time(random.randint(9, 17), random.randint(0, 59)),
                        'movement_type': 'SALE_OUT',
                        'quantity_change': -line_item['quantity_dispatched'],
                        'previous_stock': 0,  # Will be calculated
                        'new_stock': 0,  # Will be calculated
                        'reference_id': line_item['order_id'],
                        'batch_number': None,
                        'expiry_date': None,
                        'cost_per_unit': self.products[self.products['sku_code'] == line_item['sku_code']]['cost_inr'].iloc[0],
                        'movement_reason': f"Order dispatch - {line_item['order_id']}"
                    }
                    movements.append(movement)
                    movement_counter += 1
        
        # Generate movements for material purchases (IN)
        for _, po in self.purchase_orders.iterrows():
            if po['quantity_delivered'] > 0 and pd.notna(po['actual_delivery_date']):
                movement = {
                    'movement_id': f"INV-{po['receiving_facility_id'][-3:]}-{po['actual_delivery_date'].strftime('%Y%m%d%H%M%S')}-{str(movement_counter).zfill(4)}",
                    'sku_code': f"RM-{po['material_type']}-STD",  # Raw material SKU
                    'facility_id': po['receiving_facility_id'],
                    'movement_date': po['actual_delivery_date'],
                    'movement_time': time(random.randint(8, 17), random.randint(0, 59)),
                    'movement_type': 'PURCHASE_IN',
                    'quantity_change': po['quantity_delivered'],
                    'previous_stock': 0,  # Will be calculated
                    'new_stock': po['quantity_delivered'],  # Will be calculated
                    'reference_id': po['po_id'],
                    'batch_number': f"PO-{po['po_id'][-6:]}",
                    'expiry_date': None,
                    'cost_per_unit': po['unit_cost'],
                    'movement_reason': f"Purchase order received - {po['po_id']}"
                }
                movements.append(movement)
                movement_counter += 1
        
        # Generate inter-facility transfers
        # Transfer from manufacturing facilities to main DC
        for facility_id in self.facilities[self.facilities['facility_type'] == 'MANUFACTURING']['facility_id'].unique():
            # Simulate weekly transfers to main DC
            transfer_date = self.start_date + timedelta(days=7)
            while transfer_date <= self.end_date:
                # Get products manufactured at this facility in past week
                facility_production = self.production_batches[
                    (self.production_batches['manufacturing_facility_id'] == facility_id) &
                    (self.production_batches['production_date'] >= transfer_date - timedelta(days=7)) &
                    (self.production_batches['production_date'] < transfer_date)
                ]
                
                for _, batch in facility_production.iterrows():
                    transfer_qty = int(batch['actual_quantity_passed_qc'] * 0.8)  # 80% transferred to DC
                    if transfer_qty > 0:
                        # OUT from manufacturing facility
                        movements.append({
                            'movement_id': f"INV-{facility_id[-3:]}-{transfer_date.strftime('%Y%m%d%H%M%S')}-{str(movement_counter).zfill(4)}",
                            'sku_code': batch['sku_code'],
                            'facility_id': facility_id,
                            'movement_date': transfer_date,
                            'movement_time': time(10, 0),
                            'movement_type': 'TRANSFER_OUT',
                            'quantity_change': -transfer_qty,
                            'previous_stock': 0,
                            'new_stock': 0,
                            'reference_id': f"TRANSFER-{transfer_date.strftime('%Y%m%d')}-{movement_counter}",
                            'batch_number': batch['batch_id'],
                            'expiry_date': None,
                            'cost_per_unit': batch['production_cost_per_unit'],
                            'movement_reason': f"Transfer to Main DC"
                        })
                        movement_counter += 1
                        
                        # IN to main DC
                        movements.append({
                            'movement_id': f"INV-HOS-{transfer_date.strftime('%Y%m%d%H%M%S')}-{str(movement_counter).zfill(4)}",
                            'sku_code': batch['sku_code'],
                            'facility_id': 'FAC-HOS-DC',
                            'movement_date': transfer_date,
                            'movement_time': time(14, 0),
                            'movement_type': 'TRANSFER_IN',
                            'quantity_change': transfer_qty,
                            'previous_stock': 0,
                            'new_stock': 0,
                            'reference_id': f"TRANSFER-{transfer_date.strftime('%Y%m%d')}-{movement_counter-1}",
                            'batch_number': batch['batch_id'],
                            'expiry_date': None,
                            'cost_per_unit': batch['production_cost_per_unit'],
                            'movement_reason': f"Received from {facility_id}"
                        })
                        movement_counter += 1
                
                transfer_date += timedelta(days=7)
        
        # Calculate running stock balances
        movements_df = pd.DataFrame(movements)
        movements_df = movements_df.sort_values(['facility_id', 'sku_code', 'movement_date', 'movement_time'])
        
        # Calculate previous_stock and new_stock for each movement
        for facility in movements_df['facility_id'].unique():
            for sku in movements_df['sku_code'].unique():
                facility_sku_movements = movements_df[
                    (movements_df['facility_id'] == facility) & 
                    (movements_df['sku_code'] == sku)
                ].index
                
                running_stock = 0
                for idx in facility_sku_movements:
                    movements_df.at[idx, 'previous_stock'] = running_stock
                    running_stock += movements_df.at[idx, 'quantity_change']
                    movements_df.at[idx, 'new_stock'] = running_stock
        
        return movements_df

    def generate_logistics_shipments(self) -> pd.DataFrame:
        """Generate logistics shipment records"""
        shipments = []
        
        # Logistics carriers used by Wakefit
        carriers = ['BLUEDART', 'DELHIVERY', 'ECOM_EXPRESS', 'DTDC', 'XPRESSBEES']
        carrier_weights = [0.25, 0.25, 0.2, 0.15, 0.15]
        
        for _, order in self.orders.iterrows():
            if pd.notna(order['actual_dispatch_date']):
                # Select carrier
                carrier = np.random.choice(carriers, p=carrier_weights)
                
                # Generate tracking number
                tracking_number = f"{carrier[:3]}{random.randint(100000000, 999999999)}"
                
                # Calculate shipment details
                order_lines = self.order_line_items[self.order_line_items['order_id'] == order['order_id']]
                
                total_weight = 0
                total_volume = 0
                for _, line in order_lines.iterrows():
                    product = self.products[self.products['sku_code'] == line['sku_code']].iloc[0]
                    weight = product['weight_kg'] * line['quantity_dispatched']
                    
                    # Volume calculation (simplified)
                    dims = product['dimensions_lxwxh_cm'].split('x')
                    if len(dims) == 3:
                        volume = float(dims[0]) * float(dims[1]) * float(dims[2]) * line['quantity_dispatched']
                    else:
                        volume = 50000 * line['quantity_dispatched']  # Default volume
                    
                    total_weight += weight
                    total_volume += volume
                
                # Distance calculation (simplified based on pincode)
                distance = random.randint(50, 2000)  # km
                
                # Transportation cost calculation
                base_cost_per_kg = 15
                base_cost_per_km = 2
                if order['is_bulky_item'] if 'is_bulky_item' in order else False:
                    base_cost_per_kg *= 1.5
                
                transportation_cost = (total_weight * base_cost_per_kg) + (distance * base_cost_per_km * 0.1)
                transportation_cost *= random.uniform(0.8, 1.2)  # Add variation
                
                # Delivery attempts and success
                max_attempts = 3
                delivery_attempts = []
                successful_delivery_date = None
                successful_delivery_time = None
                
                if order['delivery_status'] == 'DELIVERED':
                    # Successful delivery
                    num_attempts = random.choices([1, 2, 3], weights=[0.7, 0.2, 0.1])[0]
                    
                    for attempt in range(num_attempts):
                        attempt_date = order['actual_dispatch_date'] + timedelta(days=attempt + 1)
                        attempt_time = time(random.randint(9, 18), random.randint(0, 59))
                        
                        delivery_attempts.append({
                            'attempt_number': attempt + 1,
                            'attempt_date': attempt_date.strftime('%Y-%m-%d'),
                            'attempt_time': attempt_time.strftime('%H:%M'),
                            'status': 'DELIVERED' if attempt == num_attempts - 1 else 'FAILED',
                            'reason': None if attempt == num_attempts - 1 else random.choice([
                                'CUSTOMER_NOT_AVAILABLE', 'ADDRESS_ISSUE', 'PAYMENT_PENDING'
                            ])
                        })
                    
                    successful_delivery_date = order['actual_delivery_date']
                    successful_delivery_time = time(random.randint(9, 18), random.randint(0, 59))
                
                elif order['delivery_status'] == 'FAILED':
                    # Failed delivery after all attempts
                    for attempt in range(max_attempts):
                        attempt_date = order['actual_dispatch_date'] + timedelta(days=attempt + 1)
                        attempt_time = time(random.randint(9, 18), random.randint(0, 59))
                        
                        delivery_attempts.append({
                            'attempt_number': attempt + 1,
                            'attempt_date': attempt_date.strftime('%Y-%m-%d'),
                            'attempt_time': attempt_time.strftime('%H:%M'),
                            'status': 'FAILED',
                            'reason': random.choice([
                                'CUSTOMER_NOT_AVAILABLE', 'ADDRESS_ISSUE', 'PAYMENT_PENDING', 'CUSTOMER_REFUSED'
                            ])
                        })
                
                # Customer rating based on delivery performance
                if order['delivery_status'] == 'DELIVERED':
                    if order['delay_days'] <= 0:
                        customer_rating = random.choices([4, 5], weights=[0.3, 0.7])[0]
                    elif order['delay_days'] <= 2:
                        customer_rating = random.choices([3, 4, 5], weights=[0.2, 0.5, 0.3])[0]
                    else:
                        customer_rating = random.choices([1, 2, 3], weights=[0.3, 0.4, 0.3])[0]
                else:
                    customer_rating = random.choices([1, 2], weights=[0.7, 0.3])[0]
                
                # Delivery issues
                delivery_issues = None
                if order['delay_days'] > 0 or order['delivery_status'] == 'FAILED':
                    issues = random.choice([
                        'TRAFFIC_DELAY', 'VEHICLE_BREAKDOWN', 'WEATHER_CONDITIONS', 
                        'CUSTOMER_UNAVAILABLE', 'ADDRESS_NOT_FOUND', 'PAYMENT_ISSUE'
                    ])
                    delivery_issues = issues
                
                # Return initiated (for trial orders)
                return_initiated = False
                if order['is_trial_order'] and order['delivery_status'] == 'DELIVERED':
                    return_initiated = random.random() < 0.08  # 8% return rate for trial orders
                
                shipment = {
                    'shipment_id': f"SHIP-{carrier[:3]}-{order['actual_dispatch_date'].strftime('%Y%m%d')}-{str(len(shipments)+1).zfill(6)}",
                    'order_id': order['order_id'],
                    'carrier_name': carrier,
                    'tracking_number': tracking_number,
                    'dispatch_facility_id': order_lines.iloc[0]['dispatch_facility_id'],
                    'dispatch_date': order['actual_dispatch_date'],
                    'dispatch_time': time(random.randint(8, 17), random.randint(0, 59)),
                    'delivery_address_verified': order['delivery_address_full'],
                    'delivery_pincode': order['delivery_pincode'],
                    'estimated_delivery_date': order['estimated_delivery_date'],
                    'attempted_delivery_dates': json.dumps(delivery_attempts),
                    'successful_delivery_date': successful_delivery_date,
                    'successful_delivery_time': successful_delivery_time,
                    'delivery_person_name': fake.name() if successful_delivery_date else None,
                    'delivery_otp': str(random.randint(100000, 999999)) if successful_delivery_date else None,
                    'customer_signature_received': successful_delivery_date is not None,
                    'delivery_photos': json.dumps([f"photo_{i}.jpg" for i in range(random.randint(1, 3))]) if successful_delivery_date else None,
                    'total_weight_kg': round(total_weight, 2),
                    'total_volume_cubic_cm': round(total_volume, 2),
                    'transportation_cost': round(transportation_cost, 2),
                    'distance_km': distance,
                    'delivery_rating_by_customer': customer_rating if successful_delivery_date else None,
                    'delivery_issues': delivery_issues,
                    'return_initiated': return_initiated
                }
                
                shipments.append(shipment)
        
        return pd.DataFrame(shipments)

    def generate_demand_forecasts(self) -> pd.DataFrame:
        """Generate demand forecast records with accuracy tracking"""
        forecasts = []
        forecast_counter = 1
        
        # Generate forecasts for each SKU for different time horizons
        forecast_methods = ['ARIMA', 'LINEAR_REGRESSION', 'SEASONAL_NAIVE', 'EXPONENTIAL_SMOOTHING', 'ENSEMBLE']
        
        # Create monthly forecasts for the past year to show accuracy
        for sku_code in self.products['sku_code'].unique()[:50]:  # Limit to 50 SKUs for demo
            product = self.products[self.products['sku_code'] == sku_code].iloc[0]
            
            # Get actual demand from order line items
            actual_demand = self.order_line_items[
                self.order_line_items['sku_code'] == sku_code
            ].groupby(
                self.order_line_items[self.order_line_items['sku_code'] == sku_code]['order_id'].map(
                    self.orders.set_index('order_id')['order_date'].to_dict()
                ).dt.to_period('M')
            )['quantity_ordered'].sum().to_dict()
            
            # Generate forecasts for each month
            forecast_date = self.start_date.replace(day=1)
            while forecast_date <= self.end_date.replace(day=1):
                forecast_period = forecast_date.replace(day=1)
                
                # Get historical data for this point in time
                historical_demand = []
                for i in range(6):  # Use 6 months of history
                    hist_period = (forecast_period - timedelta(days=30*i)).replace(day=1)
                    hist_period_key = pd.Period(hist_period, freq='M')
                    demand = actual_demand.get(hist_period_key, 0)
                    historical_demand.append(demand)
                
                avg_historical_demand = np.mean(historical_demand) if historical_demand else 10
                
                # Base forecast using different methods
                method = random.choice(forecast_methods)
                
                # Method-specific forecasting logic
                if method == 'SEASONAL_NAIVE':
                    seasonal_factor = product['seasonal_demand_factor'].get(forecast_period.month, 1.0)
                    base_forecast = avg_historical_demand * seasonal_factor
                elif method == 'LINEAR_REGRESSION':
                    trend = random.uniform(-0.1, 0.1)  # -10% to +10% trend
                    base_forecast = avg_historical_demand * (1 + trend)
                elif method == 'EXPONENTIAL_SMOOTHING':
                    alpha = 0.3  # Smoothing parameter
                    base_forecast = avg_historical_demand * (1 + random.uniform(-0.2, 0.2))
                else:
                    base_forecast = avg_historical_demand * random.uniform(0.8, 1.2)
                
                # Adjustments
                promotional_adjustment = 0
                if random.random() < 0.1:  # 10% chance of promotion
                    promotional_adjustment = int(base_forecast * random.uniform(0.2, 0.5))
                
                seasonal_adjustment = product['seasonal_demand_factor'].get(forecast_period.month, 1.0)
                
                # External factors
                external_factors = []
                if forecast_period.month in [10, 11, 12]:  # Festival season
                    external_factors.append('FESTIVAL_SEASON')
                    seasonal_adjustment *= 1.2
                if random.random() < 0.05:  # Random external events
                    external_factors.append(random.choice(['COMPETITOR_LAUNCH', 'ECONOMIC_SLOWDOWN', 'SUPPLY_SHORTAGE']))
                
                final_forecast = int(base_forecast * seasonal_adjustment) + promotional_adjustment
                final_forecast = max(0, final_forecast)
                
                # Get actual demand for this period
                actual_period_key = pd.Period(forecast_period, freq='M')
                actual_demand_value = actual_demand.get(actual_period_key, 0)
                
                # Calculate forecast error
                forecast_error = final_forecast - actual_demand_value
                forecast_error_percentage = (abs(forecast_error) / max(actual_demand_value, 1)) * 100
                
                # Forecast accuracy rating
                if forecast_error_percentage <= 10:
                    accuracy_rating = 'EXCELLENT'
                elif forecast_error_percentage <= 25:
                    accuracy_rating = 'GOOD'
                elif forecast_error_percentage <= 50:
                    accuracy_rating = 'AVERAGE'
                else:
                    accuracy_rating = 'POOR'
                
                # Facility (assume forecasting is done for main DC)
                facility_id = 'FAC-HOS-DC'
                
                forecast = {
                    'forecast_id': f"FC-{sku_code[-6:]}-{forecast_date.strftime('%Y%m%d')}",
                    'sku_code': sku_code,
                    'facility_id': facility_id,
                    'forecast_date': forecast_date,
                    'forecast_for_date': forecast_period,
                    'forecast_horizon_days': (forecast_period - forecast_date).days,
                    'forecasting_method': method,
                    'base_forecast': int(base_forecast),
                    'promotional_adjustment': promotional_adjustment,
                    'seasonal_adjustment': round(seasonal_adjustment, 2),
                    'external_factors': json.dumps(external_factors) if external_factors else None,
                    'final_forecast': final_forecast,
                    'actual_demand': actual_demand_value,
                    'forecast_error': forecast_error,
                    'forecast_error_percentage': round(forecast_error_percentage, 2),
                    'forecast_accuracy_rating': accuracy_rating
                }
                
                forecasts.append(forecast)
                forecast_counter += 1
                
                forecast_date += timedelta(days=30)  # Move to next month
        
        return pd.DataFrame(forecasts)

    def generate_supply_chain_events(self) -> pd.DataFrame:
        """Generate supply chain events for root cause analysis"""
        events = []
        event_counter = 1
        
        # Define event types and their typical sequence
        event_types = [
            'ORDER_RECEIVED', 'INVENTORY_ALLOCATED', 'PRODUCTION_STARTED', 
            'PRODUCTION_COMPLETED', 'QC_STARTED', 'QC_COMPLETED', 
            'DISPATCHED', 'IN_TRANSIT', 'OUT_FOR_DELIVERY', 'DELIVERED'
        ]
        
        # Generate events for each order
        for _, order in self.orders.iterrows():
            order_lines = self.order_line_items[self.order_line_items['order_id'] == order['order_id']]
            
            # Order received event
            events.append({
                'event_id': f"EVENT-ORD-{order['order_date'].strftime('%Y%m%d%H%M%S')}-{str(event_counter).zfill(6)}",
                'related_order_id': order['order_id'],
                'related_sku_code': None,  # Order level event
                'facility_id': None,
                'event_type': 'ORDER_RECEIVED',
                'event_timestamp': datetime.combine(order['order_date'], order['order_time']),
                'expected_completion_time': datetime.combine(order['order_date'], order['order_time']) + timedelta(hours=1),
                'actual_completion_time': datetime.combine(order['order_date'], order['order_time']) + timedelta(minutes=random.randint(5, 60)),
                'duration_minutes': random.randint(5, 60),
                'delay_minutes': max(0, random.randint(-10, 30)),
                'delay_category': 'NO_DELAY',
                'delay_root_cause': None,
                'responsible_team': 'SALES',
                'resolution_action': 'Order processed successfully',
                'impact_on_customer': 'NONE',
                'cost_of_delay': 0
            })
            event_counter += 1
            
            # Generate events for each line item
            for _, line_item in order_lines.iterrows():
                current_timestamp = datetime.combine(order['order_date'], order['order_time']) + timedelta(hours=2)
                
                # Inventory allocation event
                inventory_delay = 0
                inventory_delay_reason = None
                if random.random() < 0.1:  # 10% chance of inventory delay
                    inventory_delay = random.randint(60, 480)  # 1-8 hours delay
                    inventory_delay_reason = 'INVENTORY_SHORTAGE'
                
                events.append({
                    'event_id': f"EVENT-INV-{current_timestamp.strftime('%Y%m%d%H%M%S')}-{str(event_counter).zfill(6)}",
                    'related_order_id': order['order_id'],
                    'related_sku_code': line_item['sku_code'],
                    'facility_id': line_item['manufacturing_facility_id'],
                    'event_type': 'INVENTORY_ALLOCATED',
                    'event_timestamp': current_timestamp,
                    'expected_completion_time': current_timestamp + timedelta(hours=1),
                    'actual_completion_time': current_timestamp + timedelta(minutes=60 + inventory_delay),
                    'duration_minutes': 60 + inventory_delay,
                    'delay_minutes': inventory_delay,
                    'delay_category': 'INVENTORY_SHORTAGE' if inventory_delay > 0 else 'NO_DELAY',
                    'delay_root_cause': inventory_delay_reason,
                    'responsible_team': 'PRODUCTION',
                    'resolution_action': 'Stock allocated from available inventory' if inventory_delay == 0 else 'Expedited production to fulfill order',
                    'impact_on_customer': 'NONE' if inventory_delay == 0 else 'MINOR',
                    'cost_of_delay': inventory_delay * 0.5  # Cost per minute delay
                })
                event_counter += 1
                current_timestamp += timedelta(minutes=60 + inventory_delay)
                
                # Production events (if item needs to be manufactured)
                if pd.notna(line_item['estimated_manufacturing_date']):
                    # Production start
                    prod_start_delay = 0
                    prod_delay_reason = None
                    if random.random() < 0.15:  # 15% chance of production delay
                        delay_reasons = ['SUPPLIER_DELAY', 'EQUIPMENT_BREAKDOWN', 'LABOR_SHORTAGE', 'QUALITY_REWORK']
                        prod_delay_reason = random.choice(delay_reasons)
                        prod_start_delay = random.randint(120, 1440)  # 2-24 hours delay
                    
                    prod_start_time = datetime.combine(line_item['estimated_manufacturing_date'], time(8, 0)) + timedelta(minutes=prod_start_delay)
                    
                    events.append({
                        'event_id': f"EVENT-PRD-{prod_start_time.strftime('%Y%m%d%H%M%S')}-{str(event_counter).zfill(6)}",
                        'related_order_id': order['order_id'],
                        'related_sku_code': line_item['sku_code'],
                        'facility_id': line_item['manufacturing_facility_id'],
                        'event_type': 'PRODUCTION_STARTED',
                        'event_timestamp': prod_start_time,
                        'expected_completion_time': datetime.combine(line_item['estimated_manufacturing_date'], time(8, 0)),
                        'actual_completion_time': prod_start_time,
                        'duration_minutes': 0,
                        'delay_minutes': prod_start_delay,
                        'delay_category': prod_delay_reason if prod_start_delay > 0 else 'NO_DELAY',
                        'delay_root_cause': prod_delay_reason,
                        'responsible_team': 'PRODUCTION',
                        'resolution_action': 'Started production as scheduled' if prod_start_delay == 0 else f'Resolved {prod_delay_reason.lower()} and started production',
                        'impact_on_customer': 'NONE' if prod_start_delay <= 240 else 'MODERATE',
                        'cost_of_delay': prod_start_delay * 1.2
                    })
                    event_counter += 1
                    
                    # Production completion
                    product = self.products[self.products['sku_code'] == line_item['sku_code']].iloc[0]
                    production_duration = product['standard_production_time_hours'] * 60 * line_item['quantity_ordered']  # minutes
                    prod_end_time = prod_start_time + timedelta(minutes=production_duration)
                    
                    events.append({
                        'event_id': f"EVENT-PRD-{prod_end_time.strftime('%Y%m%d%H%M%S')}-{str(event_counter).zfill(6)}",
                        'related_order_id': order['order_id'],
                        'related_sku_code': line_item['sku_code'],
                        'facility_id': line_item['manufacturing_facility_id'],
                        'event_type': 'PRODUCTION_COMPLETED',
                        'event_timestamp': prod_end_time,
                        'expected_completion_time': prod_end_time,
                        'actual_completion_time': prod_end_time,
                        'duration_minutes': production_duration,
                        'delay_minutes': 0,
                        'delay_category': 'NO_DELAY',
                        'delay_root_cause': None,
                        'responsible_team': 'PRODUCTION',
                        'resolution_action': 'Production completed successfully',
                        'impact_on_customer': 'NONE',
                        'cost_of_delay': 0
                    })
                    event_counter += 1
                    current_timestamp = prod_end_time
                
                # Quality check events
                if pd.notna(line_item['quality_check_date']):
                    qc_delay = 0
                    qc_delay_reason = None
                    if line_item['quality_check_status'] == 'REWORK':
                        qc_delay = random.randint(240, 960)  # 4-16 hours for rework
                        qc_delay_reason = 'QUALITY_ISSUE'
                    
                    qc_start_time = current_timestamp + timedelta(hours=1)
                    qc_end_time = qc_start_time + timedelta(minutes=60 + qc_delay)
                    
                    events.append({
                        'event_id': f"EVENT-QC-{qc_end_time.strftime('%Y%m%d%H%M%S')}-{str(event_counter).zfill(6)}",
                        'related_order_id': order['order_id'],
                        'related_sku_code': line_item['sku_code'],
                        'facility_id': line_item['manufacturing_facility_id'],
                        'event_type': 'QC_COMPLETED',
                        'event_timestamp': qc_end_time,
                        'expected_completion_time': qc_start_time + timedelta(hours=1),
                        'actual_completion_time': qc_end_time,
                        'duration_minutes': 60 + qc_delay,
                        'delay_minutes': qc_delay,
                        'delay_category': 'QUALITY_ISSUE' if qc_delay > 0 else 'NO_DELAY',
                        'delay_root_cause': qc_delay_reason,
                        'responsible_team': 'QC',
                        'resolution_action': 'Quality check passed' if qc_delay == 0 else 'Quality issues resolved through rework',
                        'impact_on_customer': 'NONE' if qc_delay == 0 else 'MODERATE',
                        'cost_of_delay': qc_delay * 0.8
                    })
                    event_counter += 1
                    current_timestamp = qc_end_time
                
                # Dispatch event
                if pd.notna(order['actual_dispatch_date']):
                    dispatch_delay = 0
                    dispatch_delay_reason = None
                    if order['otif_status'] in ['LATE', 'FAILED']:
                        dispatch_delay = random.randint(60, 480)  # 1-8 hours delay
                        dispatch_delay_reason = random.choice(['LOGISTICS_ISSUE', 'PACKAGING_DELAY', 'CARRIER_UNAVAILABLE'])
                    
                    dispatch_time = datetime.combine(order['actual_dispatch_date'], time(10, 0)) + timedelta(minutes=dispatch_delay)
                    
                    events.append({
                        'event_id': f"EVENT-DIS-{dispatch_time.strftime('%Y%m%d%H%M%S')}-{str(event_counter).zfill(6)}",
                        'related_order_id': order['order_id'],
                        'related_sku_code': line_item['sku_code'],
                        'facility_id': line_item['dispatch_facility_id'],
                        'event_type': 'DISPATCHED',
                        'event_timestamp': dispatch_time,
                        'expected_completion_time': datetime.combine(order['actual_dispatch_date'], time(10, 0)),
                        'actual_completion_time': dispatch_time,
                        'duration_minutes': 60 + dispatch_delay,
                        'delay_minutes': dispatch_delay,
                        'delay_category': dispatch_delay_reason if dispatch_delay > 0 else 'NO_DELAY',
                        'delay_root_cause': dispatch_delay_reason,
                        'responsible_team': 'LOGISTICS',
                        'resolution_action': 'Dispatched on schedule' if dispatch_delay == 0 else f'Resolved {dispatch_delay_reason.lower()} and dispatched',
                        'impact_on_customer': 'NONE' if dispatch_delay <= 120 else 'MAJOR',
                        'cost_of_delay': dispatch_delay * 1.5
                    })
                    event_counter += 1
                
                # Delivery event
                if pd.notna(order['actual_delivery_date']):
                    delivery_delay = max(0, order['delay_days'] * 1440)  # Convert days to minutes
                    delivery_delay_reason = None
                    
                    if order['otif_status'] == 'LATE':
                        delivery_delay_reasons = ['TRAFFIC_DELAY', 'CUSTOMER_UNAVAILABLE', 'ADDRESS_ISSUE', 'VEHICLE_BREAKDOWN']
                        delivery_delay_reason = random.choice(delivery_delay_reasons)
                    elif order['otif_status'] == 'FAILED':
                        delivery_delay_reason = 'DELIVERY_FAILED'
                    
                    delivery_time = datetime.combine(order['actual_delivery_date'], time(14, 0))
                    
                    events.append({
                        'event_id': f"EVENT-DEL-{delivery_time.strftime('%Y%m%d%H%M%S')}-{str(event_counter).zfill(6)}",
                        'related_order_id': order['order_id'],
                        'related_sku_code': line_item['sku_code'],
                        'facility_id': None,
                        'event_type': 'DELIVERED' if order['delivery_status'] == 'DELIVERED' else 'DELIVERY_FAILED',
                        'event_timestamp': delivery_time,
                        'expected_completion_time': datetime.combine(order['promised_delivery_date'], time(14, 0)),
                        'actual_completion_time': delivery_time,
                        'duration_minutes': 30,  # Standard delivery time
                        'delay_minutes': delivery_delay,
                        'delay_category': delivery_delay_reason if delivery_delay > 0 else 'NO_DELAY',
                        'delay_root_cause': delivery_delay_reason,
                        'responsible_team': 'LOGISTICS',
                        'resolution_action': 'Delivered successfully' if order['delivery_status'] == 'DELIVERED' else 'Delivery failed - customer follow up required',
                        'impact_on_customer': 'NONE' if delivery_delay == 0 else 'MAJOR',
                        'cost_of_delay': delivery_delay * 2.0  # Higher cost for customer impact
                    })
                    event_counter += 1
        
        return pd.DataFrame(events)

    def save_datasets_to_files(self, datasets: Dict[str, pd.DataFrame], output_dir: str = "wakefit_data"):
        """Save all datasets to CSV files"""
        import os
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        print(f"\n💾 Saving datasets to {output_dir}/")
        
        for name, df in datasets.items():
            filename = f"{output_dir}/{name}.csv"
            df.to_csv(filename, index=False)
            print(f"✅ Saved {name}: {len(df):,} records → {filename}")
        
        # Create a summary file
        summary = []
        total_records = 0
        for name, df in datasets.items():
            summary.append({
                'Dataset': name,
                'Records': len(df),
                'Columns': len(df.columns),
                'Size_MB': round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2)
            })
            total_records += len(df)
        
        summary_df = pd.DataFrame(summary)
        summary_df.to_csv(f"{output_dir}/dataset_summary.csv", index=False)
        
        print(f"\n📊 Dataset Summary:")
        print(summary_df.to_string(index=False))
        print(f"\n🎯 Total Records Generated: {total_records:,}")
        print(f"💿 Data saved to: {os.path.abspath(output_dir)}")
        
        return output_dir

# Example usage
if __name__ == "__main__":
    # Initialize the generator
    generator = WakefitDataGenerator(
        start_date='2023-01-01', 
        end_date='2024-12-31'
    )
    
    # Generate all datasets
    datasets = generator.generate_all_data()
    
    # Save to files
    output_directory = generator.save_datasets_to_files(datasets)
    
    print(f"\n🚀 Wakefit Data Generation Complete!")
    print(f"📁 All files saved to: {output_directory}")
    print(f"\n🔍 You can now analyze:")
    print(f"   • Orders at risk of missing delivery commitments")
    print(f"   • OTIF failure root causes")
    print(f"   • Customer segments sensitive to delays")
    print(f"   • Problematic SKUs causing delays") 
    print(f"   • Demand forecast accuracy by category")
    
    # Show sample data quality checks
    print(f"\n✅ Data Quality Checks:")
    print(f"   • OTIF Rate: {(datasets['orders']['otif_status'] == 'ON_TIME_IN_FULL').mean():.1%}")
    print(f"   • Average Order Value: ₹{datasets['orders']['net_order_value'].mean():,.0f}")
    print(f"   • Total Revenue: ₹{datasets['orders']['net_order_value'].sum():,.0f}")
    print(f"   • Customer Segments: {datasets['customers']['customer_segment'].value_counts().to_dict()}")
    print(f"   • Top Categories: {datasets['products']['category'].value_counts().head(3).to_dict()}")