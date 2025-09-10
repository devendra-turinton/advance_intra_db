# """
# Optimized Wakefit Supply Chain Data Generator
# =============================================

# Features:
# - Async processing with concurrent execution
# - Batch processing with incremental saves
# - Memory-efficient streaming generators
# - Progress tracking and resume capability
# - Parallel processing for independent datasets
# - Real-time progress monitoring
# """

# import pandas as pd
# import numpy as np
# import random
# from datetime import datetime, timedelta, date, time
# import json
# from faker import Faker
# import asyncio
# import aiofiles
# from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
# from multiprocessing import Manager
# import os
# import pickle
# import gc
# from tqdm.asyncio import tqdm_asyncio
# from tqdm import tqdm
# import psutil
# import threading
# from typing import Dict, List, Tuple, Any, Generator, Optional
# import warnings
# warnings.filterwarnings('ignore')

# # Initialize Faker
# fake = Faker('en_IN')

# class OptimizedWakefitDataGenerator:
#     """High-performance data generator with async processing and batch saves"""
    
#     def __init__(self, start_date='2023-01-01', end_date='2024-12-31', output_dir='wakefit_data'):
#         self.start_date = datetime.strptime(start_date, '%Y-%m-%d')
#         self.end_date = datetime.strptime(end_date, '%Y-%m-%d')
#         self.current_date = self.end_date
#         self.output_dir = output_dir
        
#         # Create output directory
#         os.makedirs(self.output_dir, exist_ok=True)
#         os.makedirs(f"{self.output_dir}/checkpoints", exist_ok=True)
#         os.makedirs(f"{self.output_dir}/batches", exist_ok=True)
        
#         # Business constants
#         self.MONTHLY_ORDERS = 8000
#         self.TOTAL_SKUS = 500
#         self.BATCH_SIZE = 10000  # Process in batches
#         self.MAX_WORKERS = min(4, os.cpu_count())  # Limit workers
        
#         # Batch processing parameters
#         self.ORDER_BATCH_SIZE = 5000
#         self.LINE_ITEM_BATCH_SIZE = 10000
#         self.EVENT_BATCH_SIZE = 15000
        
#         # Progress tracking
#         self.progress_data = {}
#         self.completed_stages = set()
        
#         # Business logic parameters
#         self.otif_target = 0.92
#         self.seasonal_factors = {
#             'MATTRESS': {1: 0.8, 2: 0.9, 3: 1.1, 4: 1.2, 5: 1.1, 6: 0.9, 
#                         7: 0.8, 8: 0.9, 9: 1.1, 10: 1.3, 11: 1.4, 12: 1.2},
#             'FURNITURE': {1: 0.9, 2: 1.0, 3: 1.2, 4: 1.1, 5: 1.0, 6: 0.8,
#                          7: 0.7, 8: 0.8, 9: 1.0, 10: 1.2, 11: 1.3, 12: 1.1}
#         }
        
#         # Thread-safe locks
#         self.file_locks = {}
        
#         print("🚀 Optimized Wakefit Data Generator Initialized")
#         print(f"📅 Date Range: {start_date} to {end_date}")
#         print(f"💾 Output Directory: {self.output_dir}")
#         print(f"⚡ Batch Size: {self.BATCH_SIZE:,}")
#         print(f"🔧 Max Workers: {self.MAX_WORKERS}")

#     async def generate_all_data_async(self) -> Dict[str, str]:
#         """Main async orchestrator for data generation"""
#         print("\n🚀 Starting Optimized Data Generation Pipeline...")
        
#         # Step 1: Generate Master Data in Parallel
#         print("\n📋 Phase 1: Generating Master Data (Parallel)...")
#         master_data_tasks = [
#             self.generate_and_save_customers(),
#             self.generate_and_save_products(),
#             self.generate_and_save_facilities(),
#             self.generate_and_save_suppliers()
#         ]
        
#         master_results = await asyncio.gather(*master_data_tasks, return_exceptions=True)
        
#         # Check for errors
#         for i, result in enumerate(master_results):
#             if isinstance(result, Exception):
#                 print(f"❌ Error in master data generation: {result}")
#                 return {}
        
#         print("✅ Master data generation completed!")
        
#         # Step 2: Load master data for transactional generation
#         await self.load_master_data_async()
        
#         # Step 3: Generate Transactional Data (Sequential with dependencies)
#         print("\n📦 Phase 2: Generating Transactional Data (Batch Processing)...")
        
#         # Generate orders in batches
#         await self.generate_orders_batch_async()
        
#         # Generate order line items in batches
#         await self.generate_order_line_items_batch_async()
        
#         # Generate other transactional data
#         transactional_tasks = [
#             self.generate_and_save_purchase_orders(),
#             self.generate_and_save_production_batches(),
#         ]
        
#         transactional_results = await asyncio.gather(*transactional_tasks, return_exceptions=True)
        
#         # Step 4: Generate dependent datasets
#         print("\n🔗 Phase 3: Generating Dependent Data...")
#         dependent_tasks = [
#             self.generate_inventory_movements_batch_async(),
#             self.generate_logistics_shipments_batch_async(),
#             self.generate_demand_forecasts_batch_async(),
#             self.generate_supply_chain_events_batch_async()
#         ]
        
#         dependent_results = await asyncio.gather(*dependent_tasks, return_exceptions=True)
        
#         # Step 5: Consolidate batch files
#         print("\n🔧 Phase 4: Consolidating Batch Files...")
#         await self.consolidate_batch_files()
        
#         # Step 6: Generate summary
#         summary = await self.generate_summary_report()
        
#         print("\n🎉 Optimized Data Generation Complete!")
#         return summary

#     # MASTER DATA GENERATORS (Parallel)
    
#     async def generate_and_save_customers(self) -> str:
#         """Generate customers with progress tracking"""
#         if 'customers' in self.completed_stages:
#             print("⏭️ Customers already generated, skipping...")
#             return f"{self.output_dir}/customers.csv"
        
#         print("👥 Generating customers...")
#         num_customers = 50000
        
#         # Use thread pool for CPU-intensive task
#         loop = asyncio.get_event_loop()
#         with ThreadPoolExecutor(max_workers=2) as executor:
#             customers_data = await loop.run_in_executor(
#                 executor, self._generate_customers_sync, num_customers
#             )
        
#         # Convert to DataFrame and save
#         customers_df = pd.DataFrame(customers_data)
#         filename = f"{self.output_dir}/customers.csv"
        
#         await self.save_dataframe_async(customers_df, filename)
        
#         # Save checkpoint
#         await self.save_checkpoint_async('customers', len(customers_df))
#         self.completed_stages.add('customers')
        
#         print(f"✅ Generated {len(customers_df):,} customers → {filename}")
#         return filename

#     def _generate_customers_sync(self, num_customers: int) -> List[Dict]:
#         """Synchronous customer generation for thread pool"""
#         customers = []
#         major_cities = ['Bangalore', 'Mumbai', 'Delhi', 'Hyderabad', 'Chennai', 'Pune', 'Kolkata', 'Ahmedabad',
#                        'Jaipur', 'Lucknow', 'Kanpur', 'Nagpur', 'Indore', 'Bhopal', 'Visakhapatnam', 'Kochi']
        
#         states = ['Karnataka', 'Maharashtra', 'Delhi', 'Telangana', 'Tamil Nadu', 'West Bengal', 'Gujarat',
#                  'Rajasthan', 'Uttar Pradesh', 'Madhya Pradesh', 'Andhra Pradesh', 'Kerala']
        
#         with tqdm(total=num_customers, desc="Customers", leave=False) as pbar:
#             for i in range(num_customers):
#                 reg_date = fake.date_between(start_date=date(2018, 1, 1), end_date=date(2024, 11, 30))
#                 city = random.choice(major_cities)
#                 state = random.choice(states)
                
#                 # Customer segmentation logic
#                 segment_weights = {'REGULAR': 0.6, 'PREMIUM': 0.25, 'BULK': 0.1, 'PRICE_SENSITIVE': 0.05}
#                 segment = np.random.choice(list(segment_weights.keys()), p=list(segment_weights.values()))
                
#                 # Delivery sensitivity based on segment
#                 sensitivity_mapping = {'PREMIUM': (8, 10), 'REGULAR': (5, 7), 'BULK': (3, 5), 'PRICE_SENSITIVE': (2, 4)}
#                 sensitivity_range = sensitivity_mapping[segment]
#                 delivery_sensitivity = random.randint(*sensitivity_range)
                
#                 # Lifetime metrics
#                 days_active = (date(2024, 12, 31) - reg_date).days
#                 if segment == 'PREMIUM':
#                     order_frequency = random.randint(60, 120)
#                     avg_order_value = random.uniform(25000, 75000)
#                 elif segment == 'BULK':
#                     order_frequency = random.randint(180, 365)
#                     avg_order_value = random.uniform(100000, 500000)
#                 else:
#                     order_frequency = random.randint(180, 720)
#                     avg_order_value = random.uniform(8000, 35000)
                
#                 estimated_orders = max(1, days_active // order_frequency)
#                 lifetime_value = estimated_orders * avg_order_value * random.uniform(0.7, 1.3)
                
#                 customer = {
#                     'customer_id': f"CUS-{reg_date.strftime('%Y%m%d')}-{str(i+1).zfill(4)}",
#                     'customer_type': 'B2B_HOSPITALITY' if segment == 'BULK' else 'B2C',
#                     'registration_date': reg_date,
#                     'primary_channel': np.random.choice(['WEBSITE', 'APP', 'AMAZON', 'FLIPKART', 'STORE'], 
#                                                        p=[0.35, 0.25, 0.2, 0.15, 0.05]),
#                     'delivery_city': city,
#                     'delivery_state': state,
#                     'pincode': fake.postcode(),
#                     'customer_segment': segment,
#                     'delivery_sensitivity_score': delivery_sensitivity,
#                     'lifetime_orders': estimated_orders,
#                     'lifetime_value': round(lifetime_value, 2),
#                     'avg_order_frequency_days': order_frequency,
#                     'preferred_delivery_window': np.random.choice(['MORNING', 'AFTERNOON', 'EVENING', 'ANYTIME'],
#                                                                 p=[0.3, 0.25, 0.2, 0.25]),
#                     'last_order_date': fake.date_between(start_date=reg_date, end_date=date(2024, 12, 31))
#                 }
#                 customers.append(customer)
                
#                 if i % 5000 == 0:
#                     pbar.update(5000)
            
#             pbar.update(num_customers % 5000)
        
#         return customers

#     async def generate_and_save_products(self) -> str:
#         """Generate products asynchronously"""
#         if 'products' in self.completed_stages:
#             print("⏭️ Products already generated, skipping...")
#             return f"{self.output_dir}/products.csv"
        
#         print("🛏️ Generating products...")
        
#         loop = asyncio.get_event_loop()
#         with ThreadPoolExecutor(max_workers=1) as executor:
#             products_data = await loop.run_in_executor(executor, self._generate_products_sync)
        
#         products_df = pd.DataFrame(products_data)
#         filename = f"{self.output_dir}/products.csv"
        
#         await self.save_dataframe_async(products_df, filename)
#         await self.save_checkpoint_async('products', len(products_df))
#         self.completed_stages.add('products')
        
#         print(f"✅ Generated {len(products_df):,} products → {filename}")
#         return filename

#     def _generate_products_sync(self) -> List[Dict]:
#         """Generate products synchronously"""
#         products = []
        
#         categories = {
#             'MATTRESS': {
#                 'sub_categories': ['MEMORY_FOAM', 'ORTHOPEDIC', 'LATEX', 'DUAL_COMFORT', 'POCKET_SPRING'],
#                 'sizes': ['SINGLE', 'DOUBLE', 'QUEEN', 'KING'],
#                 'thickness': [4, 5, 6, 8, 10],
#                 'base_price': 8000,
#                 'complexity': 'MEDIUM'
#             },
#             'BED': {
#                 'sub_categories': ['PLATFORM', 'STORAGE', 'UPHOLSTERED', 'WOODEN'],
#                 'sizes': ['SINGLE', 'DOUBLE', 'QUEEN', 'KING'],
#                 'base_price': 15000,
#                 'complexity': 'COMPLEX'
#             },
#             'SOFA': {
#                 'sub_categories': ['2_SEATER', '3_SEATER', 'L_SHAPED', 'RECLINER', 'SOFA_CUM_BED'],
#                 'sizes': ['COMPACT', 'REGULAR', 'LARGE'],
#                 'base_price': 25000,
#                 'complexity': 'COMPLEX'
#             },
#             'CHAIR': {
#                 'sub_categories': ['OFFICE', 'DINING', 'ACCENT', 'RECLINER'],
#                 'base_price': 5000,
#                 'complexity': 'SIMPLE'
#             },
#             'TABLE': {
#                 'sub_categories': ['DINING', 'COFFEE', 'STUDY', 'CONSOLE'],
#                 'sizes': ['2_SEATER', '4_SEATER', '6_SEATER', 'STANDARD'],
#                 'base_price': 8000,
#                 'complexity': 'MEDIUM'
#             },
#             'STORAGE': {
#                 'sub_categories': ['WARDROBE', 'CHEST', 'BOOKSHELF', 'SHOE_RACK'],
#                 'base_price': 12000,
#                 'complexity': 'COMPLEX'
#             },
#             'PILLOW': {
#                 'sub_categories': ['MEMORY_FOAM', 'FIBER', 'LATEX', 'CERVICAL'],
#                 'base_price': 800,
#                 'complexity': 'SIMPLE'
#             },
#             'BEDDING': {
#                 'sub_categories': ['COMFORTER', 'BEDSHEET', 'MATTRESS_PROTECTOR'],
#                 'sizes': ['SINGLE', 'DOUBLE', 'QUEEN', 'KING'],
#                 'base_price': 2000,
#                 'complexity': 'SIMPLE'
#             }
#         }
        
#         sku_counter = 1
#         for category, cat_info in categories.items():
#             for sub_category in cat_info['sub_categories']:
#                 sizes = cat_info.get('sizes', ['STANDARD'])
                
#                 for size in sizes:
#                     if category == 'MATTRESS':
#                         for thickness in cat_info['thickness']:
#                             sku_code = f"{category[:3]}-{sub_category[:6]}-{size[:3]}-{thickness}IN"
#                             product_name = f"Wakefit {sub_category.replace('_', ' ')} {thickness}inch {size} Mattress"
                            
#                             dimensions, weight = self._get_mattress_dimensions_weight(size, thickness)
                            
#                             products.append(self._create_product_record(
#                                 sku_code, product_name, category, sub_category, size,
#                                 cat_info, weight, dimensions, thickness
#                             ))
#                             sku_counter += 1
#                     else:
#                         sku_code = f"{category[:3]}-{sub_category[:6]}-{size[:3]}-STD"
#                         product_name = f"Wakefit {sub_category.replace('_', ' ')} {size}"
                        
#                         dimensions, weight = self._get_product_dimensions_weight(category, sub_category, size)
                        
#                         products.append(self._create_product_record(
#                             sku_code, product_name, category, sub_category, size,
#                             cat_info, weight, dimensions
#                         ))
#                         sku_counter += 1
                    
#                     if sku_counter > self.TOTAL_SKUS:
#                         break
#                 if sku_counter > self.TOTAL_SKUS:
#                     break
#             if sku_counter > self.TOTAL_SKUS:
#                 break
        
#         return products

#     def _create_product_record(self, sku_code, product_name, category, sub_category, size, 
#                               cat_info, weight, dimensions, thickness=None):
#         """Create individual product record"""
#         base_price = cat_info['base_price']
#         complexity = cat_info['complexity']
        
#         # Price calculation
#         size_multiplier = {'SINGLE': 0.7, 'DOUBLE': 1.0, 'QUEEN': 1.3, 'KING': 1.6, 
#                           'COMPACT': 0.8, 'REGULAR': 1.0, 'LARGE': 1.4, 'STANDARD': 1.0}
        
#         price = base_price * size_multiplier.get(size, 1.0)
#         if thickness:
#             price *= (1 + (thickness - 4) * 0.15)
#         price *= random.uniform(0.8, 1.2)
        
#         # Manufacturing time
#         complexity_hours = {'SIMPLE': random.uniform(0.5, 2), 'MEDIUM': random.uniform(2, 6), 
#                            'COMPLEX': random.uniform(6, 24)}
        
#         # Materials
#         materials = self._get_material_requirements(category, sub_category)
        
#         return {
#             'sku_code': sku_code,
#             'product_name': product_name,
#             'category': category,
#             'sub_category': sub_category,
#             'size_variant': size,
#             'manufacturing_complexity': complexity,
#             'standard_production_time_hours': round(complexity_hours[complexity], 2),
#             'is_customizable': category in ['MATTRESS', 'BED', 'SOFA', 'STORAGE'],
#             'weight_kg': round(weight, 2),
#             'dimensions_lxwxh_cm': dimensions,
#             'is_bulky_item': category in ['BED', 'SOFA', 'STORAGE'],
#             'raw_materials_list': json.dumps(materials),
#             'minimum_inventory_days': random.randint(7, 21),
#             'maximum_inventory_days': random.randint(60, 180),
#             'supplier_lead_time_days': random.randint(15, 45),
#             'seasonal_demand_factor': json.dumps(self.seasonal_factors.get(category, 
#                                     {str(i): random.uniform(0.8, 1.2) for i in range(1, 13)})),
#             'price_inr': round(price, 2),
#             'cost_inr': round(price * random.uniform(0.4, 0.7), 2),
#             'launch_date': fake.date_between(start_date=date(2018, 1, 1), end_date=date(2024, 6, 30)),
#             'discontinuation_date': None if random.random() > 0.05 else fake.date_between(
#                 start_date=date(2024, 1, 1), end_date=date(2025, 12, 31))
#         }

#     def _get_mattress_dimensions_weight(self, size, thickness):
#         """Get mattress dimensions and weight"""
#         size_dims = {
#             'SINGLE': ('190x90', 15),
#             'DOUBLE': ('190x120', 25), 
#             'QUEEN': ('190x150', 35),
#             'KING': ('190x180', 45)
#         }
#         base_dim, base_weight = size_dims[size]
#         dimensions = f"{base_dim}x{thickness*2.54:.0f}"
#         weight = base_weight * (thickness / 6)
#         return dimensions, weight

#     def _get_product_dimensions_weight(self, category, sub_category, size):
#         """Get dimensions and weight for non-mattress products"""
#         category_weights = {
#             'BED': (25, 80), 'SOFA': (40, 120), 'CHAIR': (8, 25),
#             'TABLE': (15, 50), 'STORAGE': (30, 100), 'PILLOW': (0.5, 2),
#             'BEDDING': (1, 3)
#         }
        
#         weight_range = category_weights.get(category, (5, 20))
#         weight = random.uniform(*weight_range)
        
#         if size in ['SINGLE', 'COMPACT']:
#             dims = f"{random.randint(80,120)}x{random.randint(60,100)}x{random.randint(20,80)}"
#         elif size in ['LARGE', 'KING']:
#             dims = f"{random.randint(180,250)}x{random.randint(80,120)}x{random.randint(40,100)}"
#         else:
#             dims = f"{random.randint(120,180)}x{random.randint(70,100)}x{random.randint(30,90)}"
        
#         return dims, weight

#     def _get_material_requirements(self, category, sub_category):
#         """Get material requirements"""
#         base_materials = {
#             'MATTRESS': ['foam', 'fabric', 'zipper'],
#             'BED': ['wood', 'hardware', 'finish'],
#             'SOFA': ['wood_frame', 'foam', 'fabric', 'springs'],
#             'CHAIR': ['wood', 'fabric', 'foam', 'hardware'],
#             'TABLE': ['wood', 'finish', 'hardware'],
#             'STORAGE': ['wood', 'hardware', 'finish'],
#             'PILLOW': ['foam', 'fabric'],
#             'BEDDING': ['fabric', 'filling']
#         }
#         return base_materials.get(category, ['basic_materials'])

#     async def generate_and_save_facilities(self) -> str:
#         """Generate facilities data"""
#         if 'facilities' in self.completed_stages:
#             print("⏭️ Facilities already generated, skipping...")
#             return f"{self.output_dir}/facilities.csv"
        
#         print("🏭 Generating facilities...")
        
#         facilities = []
        
#         # Manufacturing facilities (5 locations)
#         manufacturing_locations = [
#             ('Hosur', 'Tamil Nadu', '635109'),
#             ('Bangalore', 'Karnataka', '560045'), 
#             ('Chennai', 'Tamil Nadu', '600001'),
#             ('Gurgaon', 'Haryana', '122001'),
#             ('Pune', 'Maharashtra', '411001')
#         ]
        
#         for i, (city, state, pincode) in enumerate(manufacturing_locations):
#             facilities.append({
#                 'facility_id': f"FAC-{city.upper()[:3]}-MFG",
#                 'facility_name': f"Wakefit Manufacturing - {city}",
#                 'facility_type': 'MANUFACTURING',
#                 'location_city': city,
#                 'location_state': state,
#                 'pincode': pincode,
#                 'capacity_units_per_day': random.randint(200, 500),
#                 'product_capabilities': json.dumps(['MATTRESS', 'BED', 'PILLOW'] if i < 3 else ['SOFA', 'CHAIR', 'TABLE', 'STORAGE']),
#                 'serving_regions': json.dumps([state] + random.sample(['Karnataka', 'Tamil Nadu', 'Maharashtra', 'Haryana'], 2)),
#                 'operational_status': 'ACTIVE',
#                 'setup_date': fake.date_between(start_date=date(2018, 1, 1), end_date=date(2022, 12, 31))
#             })
        
#         # Main Distribution Center
#         facilities.append({
#             'facility_id': 'FAC-HOS-DC',
#             'facility_name': 'Wakefit Main Distribution Center - Hosur',
#             'facility_type': 'DC',
#             'location_city': 'Hosur',
#             'location_state': 'Tamil Nadu',
#             'pincode': '635109',
#             'capacity_units_per_day': 2000,
#             'product_capabilities': json.dumps(['ALL_PRODUCTS']),
#             'serving_regions': json.dumps(['ALL_INDIA']),
#             'operational_status': 'ACTIVE',
#             'setup_date': date(2020, 6, 1)
#         })
        
#         # Regional Distribution Centers
#         dc_locations = [
#             ('Mumbai', 'Maharashtra'), ('Delhi', 'Delhi'), ('Bangalore', 'Karnataka'),
#             ('Chennai', 'Tamil Nadu'), ('Hyderabad', 'Telangana'), ('Pune', 'Maharashtra'),
#             ('Kolkata', 'West Bengal'), ('Ahmedabad', 'Gujarat')
#         ]
        
#         for city, state in dc_locations:
#             facilities.append({
#                 'facility_id': f"FAC-{city.upper()[:3]}-DC",
#                 'facility_name': f"Wakefit Distribution Center - {city}",
#                 'facility_type': 'WAREHOUSE',
#                 'location_city': city,
#                 'location_state': state,
#                 'pincode': fake.postcode(),
#                 'capacity_units_per_day': random.randint(300, 800),
#                 'product_capabilities': json.dumps(['ALL_PRODUCTS']),
#                 'serving_regions': json.dumps([state]),
#                 'operational_status': 'ACTIVE',
#                 'setup_date': fake.date_between(start_date=date(2019, 1, 1), end_date=date(2023, 12, 31))
#             })
        
#         # Sample retail stores
#         store_cities = ['Mumbai', 'Delhi', 'Bangalore', 'Chennai', 'Hyderabad', 'Pune', 
#                        'Kolkata', 'Ahmedabad', 'Jaipur', 'Lucknow'] * 3
        
#         for i, city in enumerate(store_cities[:30]):
#             facilities.append({
#                 'facility_id': f"FAC-{city.upper()[:3]}-ST{str(i+1).zfill(2)}",
#                 'facility_name': f"Wakefit Store - {city} {i+1}",
#                 'facility_type': 'STORE',
#                 'location_city': city,
#                 'location_state': random.choice(['Maharashtra', 'Delhi', 'Karnataka', 'Tamil Nadu', 'Telangana']),
#                 'pincode': fake.postcode(),
#                 'capacity_units_per_day': random.randint(10, 50),
#                 'product_capabilities': json.dumps(['MATTRESS', 'PILLOW', 'BEDDING']),
#                 'serving_regions': json.dumps([f"{city}_LOCAL"]),
#                 'operational_status': 'ACTIVE',
#                 'setup_date': fake.date_between(start_date=date(2022, 1, 1), end_date=date(2024, 6, 30))
#             })
        
#         facilities_df = pd.DataFrame(facilities)
#         filename = f"{self.output_dir}/facilities.csv"
        
#         await self.save_dataframe_async(facilities_df, filename)
#         await self.save_checkpoint_async('facilities', len(facilities_df))
#         self.completed_stages.add('facilities')
        
#         print(f"✅ Generated {len(facilities_df):,} facilities → {filename}")
#         return filename

#     async def generate_and_save_suppliers(self) -> str:
#         """Generate suppliers data"""
#         if 'suppliers' in self.completed_stages:
#             print("⏭️ Suppliers already generated, skipping...")
#             return f"{self.output_dir}/suppliers.csv"
        
#         print("🏭 Generating suppliers...")
        
#         suppliers = []
#         supplier_types = {
#             'FOAM': {
#                 'countries': ['India', 'Germany', 'Belgium'],
#                 'lead_times': (15, 30),
#                 'moq_range': (100, 1000)
#             },
#             'FABRIC': {
#                 'countries': ['India', 'Turkey', 'China'],
#                 'lead_times': (20, 35),
#                 'moq_range': (500, 2000)
#             },
#             'SPRING': {
#                 'countries': ['India', 'Germany', 'Belgium'],
#                 'lead_times': (25, 40),
#                 'moq_range': (200, 800)
#             },
#             'WOOD': {
#                 'countries': ['India', 'Malaysia', 'Myanmar'],
#                 'lead_times': (30, 45),
#                 'moq_range': (50, 300)
#             },
#             'HARDWARE': {
#                 'countries': ['India', 'China', 'Taiwan'],
#                 'lead_times': (15, 25),
#                 'moq_range': (1000, 5000)
#             },
#             'PACKAGING': {
#                 'countries': ['India'],
#                 'lead_times': (10, 20),
#                 'moq_range': (500, 2000)
#             }
#         }
        
#         supplier_id = 1
#         for supplier_type, info in supplier_types.items():
#             num_suppliers = random.randint(3, 5)
            
#             for i in range(num_suppliers):
#                 country = random.choice(info['countries'])
                
#                 # Quality and reliability based on country
#                 if country in ['Germany', 'Belgium']:
#                     quality_rating = random.uniform(4.2, 5.0)
#                     reliability_rating = random.uniform(4.0, 4.8)
#                     cost_competitiveness = 'HIGH'
#                 elif country == 'India':
#                     quality_rating = random.uniform(3.5, 4.5)
#                     reliability_rating = random.uniform(3.8, 4.6)
#                     cost_competitiveness = 'LOW'
#                 else:
#                     quality_rating = random.uniform(3.0, 4.2)
#                     reliability_rating = random.uniform(3.2, 4.2)
#                     cost_competitiveness = 'MEDIUM'
                
#                 suppliers.append({
#                     'supplier_id': f"SUP-{country[:3].upper()}-{str(supplier_id).zfill(3)}",
#                     'supplier_name': f"{fake.company()} {supplier_type.title()} Co.",
#                     'supplier_country': country,
#                     'supplier_type': supplier_type,
#                     'materials_supplied': json.dumps([supplier_type.lower()]),
#                     'standard_lead_time_days': random.randint(*info['lead_times']),
#                     'minimum_order_quantity': random.randint(*info['moq_range']),
#                     'quality_rating_5': round(quality_rating, 2),
#                     'reliability_rating_5': round(reliability_rating, 2),
#                     'cost_competitiveness': cost_competitiveness,
#                     'contract_start_date': fake.date_between(start_date=date(2020, 1, 1), end_date=date(2023, 12, 31)),
#                     'contract_end_date': fake.date_between(start_date=date(2024, 1, 1), end_date=date(2026, 12, 31)),
#                     'payment_terms_days': random.choice([15, 30, 45, 60])
#                 })
#                 supplier_id += 1
        
#         suppliers_df = pd.DataFrame(suppliers)
#         filename = f"{self.output_dir}/suppliers.csv"
        
#         await self.save_dataframe_async(suppliers_df, filename)
#         await self.save_checkpoint_async('suppliers', len(suppliers_df))
#         self.completed_stages.add('suppliers')
        
#         print(f"✅ Generated {len(suppliers_df):,} suppliers → {filename}")
#         return filename

#     # BATCH PROCESSING FOR LARGE DATASETS
    
#     async def generate_orders_batch_async(self):
#         """Generate orders in batches with async processing"""
#         if 'orders' in self.completed_stages:
#             print("⏭️ Orders already generated, skipping...")
#             return
        
#         print("📦 Generating orders in batches...")
        
#         total_days = (self.end_date - self.start_date).days
#         total_orders = int((self.MONTHLY_ORDERS * (total_days / 30)) * random.uniform(0.9, 1.1))
        
#         # Process orders in daily batches
#         current_date = self.start_date
#         batch_num = 0
#         total_generated = 0
        
#         with tqdm(total=total_days, desc="Order Days") as pbar:
#             while current_date <= self.end_date:
#                 # Generate orders for this day
#                 daily_orders = await self.generate_daily_orders_async(current_date)
                
#                 if daily_orders:
#                     # Save daily batch
#                     batch_filename = f"{self.output_dir}/batches/orders_batch_{batch_num:04d}.csv"
#                     daily_df = pd.DataFrame(daily_orders)
#                     await self.save_dataframe_async(daily_df, batch_filename)
                    
#                     total_generated += len(daily_orders)
#                     batch_num += 1
                
#                 current_date += timedelta(days=1)
#                 pbar.update(1)
                
#                 # Memory cleanup every 30 days
#                 if batch_num % 30 == 0:
#                     gc.collect()
        
#         print(f"✅ Generated {total_generated:,} orders in {batch_num} batches")
#         await self.save_checkpoint_async('orders', total_generated)
#         self.completed_stages.add('orders')

#     async def generate_daily_orders_async(self, date_obj: date) -> List[Dict]:
#         """Generate orders for a specific day"""
#         # Daily order calculation with seasonality
#         month = date_obj.month
#         day_of_week = date_obj.weekday()
        
#         # Base daily orders
#         daily_base = self.MONTHLY_ORDERS / 30
        
#         # Seasonal and day-of-week adjustments
#         seasonal_factor = random.choice([0.9, 1.0, 1.1, 1.2])
#         dow_factors = {0: 1.0, 1: 1.0, 2: 1.0, 3: 1.0, 4: 1.1, 5: 1.3, 6: 1.4}
#         dow_factor = dow_factors[day_of_week]
        
#         daily_orders = int(daily_base * seasonal_factor * dow_factor * random.uniform(0.7, 1.3))
        
#         # Use thread pool for CPU-intensive order generation
#         loop = asyncio.get_event_loop()
#         with ThreadPoolExecutor(max_workers=2) as executor:
#             orders = await loop.run_in_executor(
#                 executor, self._generate_orders_for_date_sync, date_obj, daily_orders
#             )
        
#         return orders

#     def _generate_orders_for_date_sync(self, date_obj: date, num_orders: int) -> List[Dict]:
#         """Synchronously generate orders for a specific date"""
#         orders = []
        
#         # Load customer data (assume it's available)
#         if not hasattr(self, 'customers_sample'):
#             # Create a small sample for reference
#             self.customers_sample = [
#                 {
#                     'customer_id': f"CUS-{date_obj.strftime('%Y%m%d')}-{str(i).zfill(4)}",
#                     'customer_segment': random.choice(['REGULAR', 'PREMIUM', 'BULK', 'PRICE_SENSITIVE']),
#                     'primary_channel': random.choice(['WEBSITE', 'APP', 'AMAZON', 'FLIPKART', 'STORE']),
#                     'delivery_city': random.choice(['Mumbai', 'Delhi', 'Bangalore', 'Chennai']),
#                     'delivery_state': random.choice(['Maharashtra', 'Delhi', 'Karnataka', 'Tamil Nadu']),
#                     'pincode': fake.postcode(),
#                     'delivery_sensitivity_score': random.randint(1, 10)
#                 }
#                 for i in range(min(1000, num_orders))
#             ]
        
#         for i in range(num_orders):
#             customer = random.choice(self.customers_sample)
            
#             # Order timing
#             order_time = time(
#                 hour=random.randint(6, 23),
#                 minute=random.randint(0, 59),
#                 second=random.randint(0, 59)
#             )
            
#             # Channel and delivery expectations
#             channel = customer['primary_channel']
            
#             if customer['customer_segment'] == 'PREMIUM':
#                 delivery_days = random.randint(2, 5)
#             elif channel in ['AMAZON', 'FLIPKART']:
#                 delivery_days = random.randint(3, 7)
#             else:
#                 delivery_days = random.randint(5, 12)
            
#             customer_expectation = date_obj + timedelta(days=delivery_days)
#             promised_delivery = customer_expectation + timedelta(days=random.randint(1, 3))
            
#             # Order characteristics
#             if customer['customer_segment'] == 'BULK':
#                 total_items = random.randint(5, 20)
#                 total_quantity = random.randint(50, 200)
#                 gross_value = random.uniform(100000, 500000)
#             elif customer['customer_segment'] == 'PREMIUM':
#                 total_items = random.randint(2, 8)
#                 total_quantity = random.randint(5, 25)
#                 gross_value = random.uniform(25000, 75000)
#             else:
#                 total_items = random.randint(1, 4)
#                 total_quantity = random.randint(1, 10)
#                 gross_value = random.uniform(8000, 35000)
            
#             # Payment method
#             if channel in ['AMAZON', 'FLIPKART']:
#                 payment_method = np.random.choice(['PREPAID', 'COD'], p=[0.6, 0.4])
#             else:
#                 payment_method = np.random.choice(['COD', 'PREPAID'], p=[0.6, 0.4])
            
#             # OTIF simulation
#             will_be_otif = random.random() < self.otif_target
            
#             if will_be_otif:
#                 actual_delivery = promised_delivery - timedelta(days=random.randint(0, 2))
#                 delay_days = (actual_delivery - promised_delivery).days
#                 otif_status = 'ON_TIME_IN_FULL'
#                 delivery_status = 'DELIVERED'
#             else:
#                 delay_days = random.randint(1, 8)
#                 actual_delivery = promised_delivery + timedelta(days=delay_days)
                
#                 if delay_days <= 2:
#                     otif_status = 'LATE'
#                 else:
#                     otif_status = np.random.choice(['LATE', 'INCOMPLETE', 'FAILED'], p=[0.5, 0.3, 0.2])
                
#                 delivery_status = 'DELIVERED' if otif_status != 'FAILED' else 'FAILED'
            
#             # Satisfaction
#             if otif_status == 'ON_TIME_IN_FULL':
#                 satisfaction = np.random.choice([4, 5], p=[0.3, 0.7])
#                 nps = random.randint(7, 10)
#             elif delay_days <= 2:
#                 satisfaction = np.random.choice([3, 4, 5], p=[0.2, 0.5, 0.3])
#                 nps = random.randint(5, 8)
#             else:
#                 satisfaction = np.random.choice([1, 2, 3], p=[0.3, 0.4, 0.3])
#                 nps = random.randint(-10, 5)
            
#             # Discounts
#             discount_rate = random.uniform(0.05, 0.15) if channel in ['WEBSITE', 'APP'] else 0
#             discount_amount = gross_value * discount_rate
#             net_value = gross_value - discount_amount
            
#             order = {
#                 'order_id': f"ORD-{date_obj.strftime('%Y%m%d')}-{str(i+1).zfill(6)}",
#                 'customer_id': customer['customer_id'],
#                 'order_date': date_obj,
#                 'order_time': order_time,
#                 'channel': channel,
#                 'store_id': None,
#                 'total_items': total_items,
#                 'total_quantity': total_quantity,
#                 'gross_order_value': round(gross_value, 2),
#                 'discount_amount': round(discount_amount, 2),
#                 'net_order_value': round(net_value, 2),
#                 'payment_method': payment_method,
#                 'payment_status': 'PAID' if payment_method != 'COD' else 'PENDING',
#                 'customer_delivery_expectation': customer_expectation,
#                 'promised_delivery_date': promised_delivery,
#                 'delivery_address_full': f"{fake.street_address()}, {customer['delivery_city']}, {customer['delivery_state']}",
#                 'delivery_pincode': customer['pincode'],
#                 'delivery_instructions': fake.sentence() if random.random() < 0.3 else None,
#                 'order_priority': 'BULK' if customer['customer_segment'] == 'BULK' else 'STANDARD',
#                 'is_trial_order': random.random() < 0.15,
#                 'estimated_dispatch_date': promised_delivery - timedelta(days=2),
#                 'actual_dispatch_date': actual_delivery - timedelta(days=1) if date_obj < self.current_date else None,
#                 'estimated_delivery_date': promised_delivery,
#                 'actual_delivery_date': actual_delivery if date_obj < self.current_date else None,
#                 'delivery_status': delivery_status if date_obj < self.current_date else 'PENDING',
#                 'delivery_attempts': random.randint(1, 3) if delivery_status == 'DELIVERED' else 1,
#                 'otif_status': otif_status if date_obj < self.current_date else 'PENDING',
#                 'delay_days': delay_days if date_obj < self.current_date else 0,
#                 'customer_satisfaction_rating': satisfaction if date_obj < self.current_date else None,
#                 'nps_score': nps if date_obj < self.current_date else None
#             }
            
#             orders.append(order)
        
#         return orders

#     async def generate_order_line_items_batch_async(self):
#         """Generate order line items in batches"""
#         if 'order_line_items' in self.completed_stages:
#             print("⏭️ Order line items already generated, skipping...")
#             return
        
#         print("📋 Generating order line items in batches...")
        
#         # Get list of order batch files
#         batch_files = [f for f in os.listdir(f"{self.output_dir}/batches") if f.startswith('orders_batch_')]
#         batch_files.sort()
        
#         line_item_counter = 0
#         batch_num = 0
        
#         with tqdm(total=len(batch_files), desc="Processing Order Batches") as pbar:
#             for batch_file in batch_files:
#                 # Load order batch
#                 orders_df = pd.read_csv(f"{self.output_dir}/batches/{batch_file}")
                
#                 # Generate line items for this batch
#                 line_items = await self.generate_line_items_for_order_batch(orders_df)
                
#                 if line_items:
#                     # Save line items batch
#                     line_items_df = pd.DataFrame(line_items)
#                     batch_filename = f"{self.output_dir}/batches/line_items_batch_{batch_num:04d}.csv"
#                     await self.save_dataframe_async(line_items_df, batch_filename)
                    
#                     line_item_counter += len(line_items)
#                     batch_num += 1
                
#                 pbar.update(1)
                
#                 # Memory cleanup
#                 del orders_df, line_items
#                 if batch_num % 10 == 0:
#                     gc.collect()
        
#         print(f"✅ Generated {line_item_counter:,} order line items in {batch_num} batches")
#         await self.save_checkpoint_async('order_line_items', line_item_counter)
#         self.completed_stages.add('order_line_items')

#     async def generate_line_items_for_order_batch(self, orders_df: pd.DataFrame) -> List[Dict]:
#         """Generate line items for a batch of orders"""
#         loop = asyncio.get_event_loop()
#         with ThreadPoolExecutor(max_workers=2) as executor:
#             line_items = await loop.run_in_executor(
#                 executor, self._generate_line_items_sync, orders_df
#             )
#         return line_items

#     def _generate_line_items_sync(self, orders_df: pd.DataFrame) -> List[Dict]:
#         """Synchronously generate line items"""
#         line_items = []
        
#         # Sample product data for line items
#         sample_products = [
#             {'sku_code': 'MAT-MEMORY-QUE-6IN', 'category': 'MATTRESS', 'price_inr': 15000, 'is_customizable': True},
#             {'sku_code': 'BED-PLATFO-KIN-STD', 'category': 'BED', 'price_inr': 25000, 'is_customizable': True},
#             {'sku_code': 'SOF-3_SEAT-REG-STD', 'category': 'SOFA', 'price_inr': 35000, 'is_customizable': False},
#             {'sku_code': 'PIL-MEMORY-STD-STD', 'category': 'PILLOW', 'price_inr': 1200, 'is_customizable': False},
#             {'sku_code': 'BED-COMFOR-QUE-STD', 'category': 'BEDDING', 'price_inr': 2500, 'is_customizable': False},
#         ] * 20  # Multiply for more variety
        
#         for _, order in orders_df.iterrows():
#             total_items = order['total_items']
            
#             for item_seq in range(total_items):
#                 product = random.choice(sample_products)
                
#                 # Quantity based on product and order type
#                 if order['order_priority'] == 'BULK':
#                     quantity = random.randint(5, 20)
#                 elif product['category'] == 'MATTRESS':
#                     quantity = 1
#                 elif product['category'] in ['PILLOW', 'BEDDING']:
#                     quantity = random.randint(1, 4)
#                 else:
#                     quantity = random.randint(1, 2)
                
#                 unit_price = product['price_inr'] * random.uniform(0.95, 1.05)
#                 line_total = unit_price * quantity
                
#                 # Manufacturing timeline
#                 estimated_manufacturing = pd.to_datetime(order['order_date']) + timedelta(days=random.randint(1, 5))
#                 manufacturing_facility = random.choice(['FAC-HOS-MFG', 'FAC-BAN-MFG', 'FAC-CHE-MFG'])
                
#                 # Quality and delivery status
#                 if pd.notna(order['actual_delivery_date']):
#                     if order['otif_status'] == 'ON_TIME_IN_FULL':
#                         qc_status = 'PASSED'
#                         quantity_delivered = quantity
#                     else:
#                         qc_status = random.choice(['PASSED', 'REWORK'])
#                         quantity_delivered = int(quantity * random.uniform(0.8, 1.0))
                    
#                     actual_manufacturing = estimated_manufacturing + timedelta(days=random.randint(-1, 2))
#                     qc_date = actual_manufacturing + timedelta(days=1)
#                     line_status = 'DELIVERED'
#                 else:
#                     qc_status = 'PENDING'
#                     actual_manufacturing = None
#                     qc_date = None
#                     quantity_delivered = 0
#                     line_status = 'PENDING'
                
#                 line_item = {
#                     'line_item_id': f"LI-{order['order_id']}-{str(item_seq+1).zfill(3)}",
#                     'order_id': order['order_id'],
#                     'sku_code': product['sku_code'],
#                     'quantity_ordered': quantity,
#                     'quantity_confirmed': quantity,
#                     'quantity_dispatched': quantity_delivered,
#                     'quantity_delivered': quantity_delivered,
#                     'unit_price': round(unit_price, 2),
#                     'line_total': round(line_total, 2),
#                     'customization_details': json.dumps({'custom': True}) if product['is_customizable'] and random.random() < 0.2 else None,
#                     'estimated_manufacturing_date': estimated_manufacturing.date(),
#                     'actual_manufacturing_date': actual_manufacturing.date() if actual_manufacturing else None,
#                     'manufacturing_facility_id': manufacturing_facility,
#                     'quality_check_status': qc_status,
#                     'quality_check_date': qc_date.date() if qc_date else None,
#                     'inventory_allocation_time': pd.to_datetime(order['order_date']) + timedelta(hours=random.randint(1, 24)),
#                     'line_item_status': line_status,
#                     'dispatch_facility_id': manufacturing_facility
#                 }
                
#                 line_items.append(line_item)
        
#         return line_items

#     # UTILITY FUNCTIONS
    
#     async def save_dataframe_async(self, df: pd.DataFrame, filename: str):
#         """Asynchronously save DataFrame to CSV"""
#         loop = asyncio.get_event_loop()
#         with ThreadPoolExecutor(max_workers=1) as executor:
#             await loop.run_in_executor(
#                 executor, 
#                 lambda: df.to_csv(filename, index=False)
#             )

#     async def save_checkpoint_async(self, stage: str, count: int):
#         """Save progress checkpoint"""
#         checkpoint_data = {
#             'stage': stage,
#             'count': count,
#             'timestamp': datetime.now().isoformat(),
#             'completed_stages': list(self.completed_stages)
#         }
        
#         checkpoint_file = f"{self.output_dir}/checkpoints/{stage}_checkpoint.json"
#         async with aiofiles.open(checkpoint_file, 'w') as f:
#             await f.write(json.dumps(checkpoint_data, indent=2))

#     async def load_master_data_async(self):
#         """Load master data for transactional generation"""
#         print("📥 Loading master data for transactional processing...")
        
#         # Load essential data
#         try:
#             self.customers_df = pd.read_csv(f"{self.output_dir}/customers.csv")
#             self.products_df = pd.read_csv(f"{self.output_dir}/products.csv")
#             self.facilities_df = pd.read_csv(f"{self.output_dir}/facilities.csv")
#             self.suppliers_df = pd.read_csv(f"{self.output_dir}/suppliers.csv")
#             print("✅ Master data loaded successfully")
#         except Exception as e:
#             print(f"❌ Error loading master data: {e}")
#             raise

#     async def consolidate_batch_files(self):
#         """Consolidate all batch files into final datasets"""
#         print("🔧 Consolidating batch files...")
        
#         datasets_to_consolidate = [
#             ('orders_batch_', 'orders.csv'),
#             ('line_items_batch_', 'order_line_items.csv'),
#             ('inventory_batch_', 'inventory_movements.csv'),
#             ('shipments_batch_', 'logistics_shipments.csv'),
#             ('forecasts_batch_', 'demand_forecasts.csv'),
#             ('events_batch_', 'supply_chain_events.csv')
#         ]
        
#         for batch_prefix, final_filename in datasets_to_consolidate:
#             batch_files = [f for f in os.listdir(f"{self.output_dir}/batches") if f.startswith(batch_prefix)]
            
#             if batch_files:
#                 batch_files.sort()
#                 consolidated_df = pd.DataFrame()
                
#                 print(f"🔗 Consolidating {len(batch_files)} {batch_prefix} files...")
                
#                 for batch_file in batch_files:
#                     batch_df = pd.read_csv(f"{self.output_dir}/batches/{batch_file}")
#                     consolidated_df = pd.concat([consolidated_df, batch_df], ignore_index=True)
                
#                 # Save consolidated file
#                 final_path = f"{self.output_dir}/{final_filename}"
#                 await self.save_dataframe_async(consolidated_df, final_path)
                
#                 print(f"✅ Consolidated {final_filename}: {len(consolidated_df):,} records")
                
#                 # Clean up batch files
#                 for batch_file in batch_files:
#                     os.remove(f"{self.output_dir}/batches/{batch_file}")

#     async def generate_summary_report(self) -> Dict[str, Any]:
#         """Generate final summary report"""
#         print("📊 Generating summary report...")
        
#         summary = {
#             'generation_completed_at': datetime.now().isoformat(),
#             'output_directory': self.output_dir,
#             'datasets': {},
#             'total_records': 0,
#             'total_size_mb': 0
#         }
        
#         # Analyze each dataset
#         for filename in os.listdir(self.output_dir):
#             if filename.endswith('.csv') and not filename.startswith('dataset_summary'):
#                 filepath = f"{self.output_dir}/{filename}"
#                 try:
#                     df = pd.read_csv(filepath)
#                     file_size = os.path.getsize(filepath) / 1024 / 1024  # MB
                    
#                     dataset_name = filename.replace('.csv', '')
#                     summary['datasets'][dataset_name] = {
#                         'records': len(df),
#                         'columns': len(df.columns),
#                         'size_mb': round(file_size, 2),
#                         'filename': filename
#                     }
                    
#                     summary['total_records'] += len(df)
#                     summary['total_size_mb'] += file_size
                    
#                 except Exception as e:
#                     print(f"⚠️ Error analyzing {filename}: {e}")
        
#         summary['total_size_mb'] = round(summary['total_size_mb'], 2)
        
#         # Save summary
#         summary_file = f"{self.output_dir}/generation_summary.json"
#         async with aiofiles.open(summary_file, 'w') as f:
#             await f.write(json.dumps(summary, indent=2))
        
#         # Create CSV summary
#         datasets_summary = []
#         for name, info in summary['datasets'].items():
#             datasets_summary.append({
#                 'Dataset': name,
#                 'Records': info['records'],
#                 'Columns': info['columns'],
#                 'Size_MB': info['size_mb'],
#                 'Filename': info['filename']
#             })
        
#         summary_df = pd.DataFrame(datasets_summary)
#         await self.save_dataframe_async(summary_df, f"{self.output_dir}/dataset_summary.csv")
        
#         print(f"\n📊 Generation Summary:")
#         print(f"   📁 Output Directory: {self.output_dir}")
#         print(f"   📊 Total Datasets: {len(summary['datasets'])}")
#         print(f"   📝 Total Records: {summary['total_records']:,}")
#         print(f"   💾 Total Size: {summary['total_size_mb']:.1f} MB")
        
#         return summary

#     # PLACEHOLDER METHODS FOR REMAINING DATASETS
#     # These would implement similar batch processing patterns

#     async def generate_and_save_purchase_orders(self) -> str:
#         """Generate purchase orders (placeholder for brevity)"""
#         if 'purchase_orders' in self.completed_stages:
#             return f"{self.output_dir}/purchase_orders.csv"
        
#         print("🛒 Generating purchase orders...")
#         # Implementation would follow similar pattern with batch processing
#         # For brevity, creating minimal data
        
#         po_data = [
#             {
#                 'po_id': f"PO-SUP-{datetime.now().strftime('%Y%m%d')}-{str(i).zfill(3)}",
#                 'supplier_id': f"SUP-IND-{str(random.randint(1,25)).zfill(3)}",
#                 'po_date': fake.date_between(start_date=self.start_date.date(), end_date=self.end_date.date()),
#                 'total_po_value': random.uniform(50000, 500000),
#                 'po_status': random.choice(['PENDING', 'CONFIRMED', 'RECEIVED', 'CLOSED'])
#             }
#             for i in range(500)  # Reduced for demo
#         ]
        
#         po_df = pd.DataFrame(po_data)
#         filename = f"{self.output_dir}/purchase_orders.csv"
#         await self.save_dataframe_async(po_df, filename)
#         self.completed_stages.add('purchase_orders')
        
#         print(f"✅ Generated {len(po_df):,} purchase orders → {filename}")
#         return filename

#     async def generate_and_save_production_batches(self) -> str:
#         """Generate production batches (placeholder)"""
#         if 'production_batches' in self.completed_stages:
#             return f"{self.output_dir}/production_batches.csv"
        
#         print("🏭 Generating production batches...")
        
#         batch_data = [
#             {
#                 'batch_id': f"BATCH-MFG-{datetime.now().strftime('%Y%m%d')}-{str(i).zfill(3)}",
#                 'sku_code': f"MAT-MEMORY-QUE-6IN",
#                 'production_date': fake.date_between(start_date=self.start_date.date(), end_date=self.end_date.date()),
#                 'planned_quantity': random.randint(50, 200),
#                 'actual_quantity_produced': random.randint(45, 195),
#                 'efficiency_percentage': random.uniform(85, 98)
#             }
#             for i in range(1000)  # Reduced for demo
#         ]
        
#         batch_df = pd.DataFrame(batch_data)
#         filename = f"{self.output_dir}/production_batches.csv"
#         await self.save_dataframe_async(batch_df, filename)
#         self.completed_stages.add('production_batches')
        
#         print(f"✅ Generated {len(batch_df):,} production batches → {filename}")
#         return filename

#     async def generate_inventory_movements_batch_async(self):
#         """Generate inventory movements in batches"""
#         if 'inventory_movements' in self.completed_stages:
#             print("⏭️ Inventory movements already generated, skipping...")
#             return
        
#         print("📦 Generating inventory movements...")
        
#         try:
#             # Load required data
#             orders_df = pd.read_csv(f"{self.output_dir}/orders.csv")
#             line_items_df = pd.read_csv(f"{self.output_dir}/order_line_items.csv")
#             production_df = pd.read_csv(f"{self.output_dir}/production_batches.csv")
            
#             movements = []
#             movement_counter = 1
            
#             # Generate movements for production (IN)
#             for _, batch in production_df.iterrows():
#                 movements.append({
#                     'movement_id': f"INV-{batch.get('sku_code', 'UNKNOWN')[:3]}-{movement_counter:06d}",
#                     'sku_code': batch.get('sku_code', 'MAT-MEMORY-QUE-6IN'),
#                     'facility_id': 'FAC-HOS-MFG',
#                     'movement_date': batch.get('production_date', fake.date_between(start_date=self.start_date.date(), end_date=self.end_date.date())),
#                     'movement_time': fake.time(),
#                     'movement_type': 'PRODUCTION_IN',
#                     'quantity_change': batch.get('actual_quantity_produced', random.randint(50, 200)),
#                     'previous_stock': 0,
#                     'new_stock': batch.get('actual_quantity_produced', random.randint(50, 200)),
#                     'reference_id': batch.get('batch_id', f"BATCH-{movement_counter}"),
#                     'batch_number': batch.get('batch_id', f"BATCH-{movement_counter}"),
#                     'expiry_date': None,
#                     'cost_per_unit': random.uniform(1000, 5000),
#                     'movement_reason': f"Production completed - {batch.get('batch_id', 'BATCH')}"
#                 })
#                 movement_counter += 1
            
#             # Generate movements for sales (OUT)
#             for _, line_item in line_items_df.iterrows():
#                 if line_item.get('quantity_dispatched', 0) > 0:
#                     movements.append({
#                         'movement_id': f"INV-SALE-{movement_counter:06d}",
#                         'sku_code': line_item['sku_code'],
#                         'facility_id': line_item.get('dispatch_facility_id', 'FAC-HOS-MFG'),
#                         'movement_date': fake.date_between(start_date=self.start_date.date(), end_date=self.end_date.date()),
#                         'movement_time': fake.time(),
#                         'movement_type': 'SALE_OUT',
#                         'quantity_change': -int(line_item['quantity_dispatched']),
#                         'previous_stock': random.randint(50, 500),
#                         'new_stock': random.randint(10, 450),
#                         'reference_id': line_item['order_id'],
#                         'batch_number': None,
#                         'expiry_date': None,
#                         'cost_per_unit': line_item.get('unit_price', 1000) * 0.7,
#                         'movement_reason': f"Order dispatch - {line_item['order_id']}"
#                     })
#                     movement_counter += 1
            
#             # Generate inter-facility transfers
#             for i in range(500):  # Generate some transfers
#                 movements.append({
#                     'movement_id': f"INV-TRANSFER-{movement_counter:06d}",
#                     'sku_code': random.choice(['MAT-MEMORY-QUE-6IN', 'BED-PLATFO-KIN-STD', 'SOF-3_SEAT-REG-STD']),
#                     'facility_id': random.choice(['FAC-HOS-MFG', 'FAC-BAN-MFG', 'FAC-HOS-DC']),
#                     'movement_date': fake.date_between(start_date=self.start_date.date(), end_date=self.end_date.date()),
#                     'movement_time': fake.time(),
#                     'movement_type': random.choice(['TRANSFER_IN', 'TRANSFER_OUT']),
#                     'quantity_change': random.randint(-100, 100),
#                     'previous_stock': random.randint(100, 1000),
#                     'new_stock': random.randint(50, 950),
#                     'reference_id': f"TRANSFER-{i+1:04d}",
#                     'batch_number': None,
#                     'expiry_date': None,
#                     'cost_per_unit': random.uniform(500, 3000),
#                     'movement_reason': 'Inter-facility stock transfer'
#                 })
#                 movement_counter += 1
            
#             movements_df = pd.DataFrame(movements)
#             await self.save_dataframe_async(movements_df, f"{self.output_dir}/inventory_movements.csv")
            
#             print(f"✅ Generated {len(movements_df):,} inventory movements")
#             await self.save_checkpoint_async('inventory_movements', len(movements_df))
#             self.completed_stages.add('inventory_movements')
            
#         except Exception as e:
#             print(f"❌ Error generating inventory movements: {e}")

#     async def generate_logistics_shipments_batch_async(self):
#         """Generate logistics shipments in batches"""
#         if 'logistics_shipments' in self.completed_stages:
#             print("⏭️ Logistics shipments already generated, skipping...")
#             return
        
#         print("🚚 Generating logistics shipments...")
        
#         try:
#             # Load orders data
#             orders_df = pd.read_csv(f"{self.output_dir}/orders.csv")
#             line_items_df = pd.read_csv(f"{self.output_dir}/order_line_items.csv")
            
#             carriers = ['BLUEDART', 'DELHIVERY', 'ECOM_EXPRESS', 'DTDC', 'XPRESSBEES']
#             shipments = []
            
#             for _, order in orders_df.iterrows():
#                 # Check if dispatch date exists and is not null/empty
#                 dispatch_date_str = order.get('actual_dispatch_date')
#                 if pd.isna(dispatch_date_str) or dispatch_date_str == '' or str(dispatch_date_str).lower() == 'nan':
#                     continue
                
#                 try:
#                     # Try to parse the dispatch date
#                     if isinstance(dispatch_date_str, str):
#                         dispatch_date = pd.to_datetime(dispatch_date_str)
#                     else:
#                         dispatch_date = pd.to_datetime(str(dispatch_date_str))
                    
#                     if pd.isna(dispatch_date):
#                         continue
                        
#                 except:
#                     # If date parsing fails, skip this order
#                     continue
                
#                 # Select carrier
#                 carrier = random.choice(carriers)
                
#                 # Calculate shipment details
#                 order_lines = line_items_df[line_items_df['order_id'] == order['order_id']]
#                 total_weight = len(order_lines) * random.uniform(5, 50)  # Simplified weight calculation
#                 total_volume = total_weight * random.uniform(1000, 5000)
                
#                 # Distance and cost
#                 distance = random.randint(50, 2000)
#                 transportation_cost = (total_weight * 15) + (distance * 0.2) + random.uniform(100, 500)
                
#                 # Delivery attempts
#                 delivery_attempts = []
#                 num_attempts = 1 if order.get('delivery_status') == 'DELIVERED' else random.randint(1, 3)
                
#                 for attempt in range(num_attempts):
#                     try:
#                         attempt_date = dispatch_date + timedelta(days=attempt + 1)
#                         delivery_attempts.append({
#                             'attempt_number': attempt + 1,
#                             'attempt_date': attempt_date.strftime('%Y-%m-%d'),
#                             'attempt_time': f"{random.randint(9,18):02d}:{random.randint(0,59):02d}",
#                             'status': 'DELIVERED' if (attempt == num_attempts - 1 and order.get('delivery_status') == 'DELIVERED') else 'FAILED',
#                             'reason': None if order.get('delivery_status') == 'DELIVERED' else random.choice(['CUSTOMER_NOT_AVAILABLE', 'ADDRESS_ISSUE', 'PAYMENT_PENDING'])
#                         })
#                     except:
#                         # If date calculation fails, create basic attempt
#                         delivery_attempts.append({
#                             'attempt_number': attempt + 1,
#                             'attempt_date': fake.date().strftime('%Y-%m-%d'),
#                             'attempt_time': f"{random.randint(9,18):02d}:{random.randint(0,59):02d}",
#                             'status': 'DELIVERED' if (attempt == num_attempts - 1 and order.get('delivery_status') == 'DELIVERED') else 'FAILED',
#                             'reason': None if order.get('delivery_status') == 'DELIVERED' else 'DELIVERY_ISSUE'
#                         })
                
#                 # Customer rating
#                 delivery_rating = order.get('customer_satisfaction_rating', 4)
#                 if pd.isna(delivery_rating):
#                     delay_days = order.get('delay_days', 0)
#                     if delay_days <= 0:
#                         delivery_rating = random.randint(4, 5)
#                     elif delay_days <= 2:
#                         delivery_rating = random.randint(3, 4)
#                     else:
#                         delivery_rating = random.randint(1, 3)
                
#                 # Safe date handling for output
#                 try:
#                     dispatch_date_output = dispatch_date.date()
#                 except:
#                     dispatch_date_output = fake.date()
                
#                 # Handle delivery date safely
#                 delivery_date_output = None
#                 if order.get('delivery_status') == 'DELIVERED':
#                     try:
#                         actual_delivery = pd.to_datetime(order.get('actual_delivery_date'))
#                         if pd.notna(actual_delivery):
#                             delivery_date_output = actual_delivery.date()
#                         else:
#                             delivery_date_output = fake.date()
#                     except:
#                         delivery_date_output = fake.date()
                
#                 shipments.append({
#                     'shipment_id': f"SHIP-{carrier[:3]}-{len(shipments)+1:06d}",
#                     'order_id': order['order_id'],
#                     'carrier_name': carrier,
#                     'tracking_number': f"{carrier[:3]}{random.randint(100000000, 999999999)}",
#                     'dispatch_facility_id': random.choice(['FAC-HOS-MFG', 'FAC-BAN-MFG', 'FAC-HOS-DC']),
#                     'dispatch_date': dispatch_date_output,
#                     'dispatch_time': f"{random.randint(8,17):02d}:{random.randint(0,59):02d}",
#                     'delivery_address_verified': order.get('delivery_address_full', fake.address()),
#                     'delivery_pincode': order.get('delivery_pincode', fake.postcode()),
#                     'estimated_delivery_date': fake.date(),
#                     'attempted_delivery_dates': json.dumps(delivery_attempts),
#                     'successful_delivery_date': delivery_date_output,
#                     'successful_delivery_time': f"{random.randint(9,18):02d}:{random.randint(0,59):02d}" if delivery_date_output else None,
#                     'delivery_person_name': fake.name() if delivery_date_output else None,
#                     'delivery_otp': str(random.randint(100000, 999999)) if delivery_date_output else None,
#                     'customer_signature_received': delivery_date_output is not None,
#                     'delivery_photos': json.dumps([f"photo_{i}.jpg" for i in range(random.randint(1, 3))]) if delivery_date_output else None,
#                     'total_weight_kg': round(total_weight, 2),
#                     'total_volume_cubic_cm': round(total_volume, 2),
#                     'transportation_cost': round(transportation_cost, 2),
#                     'distance_km': distance,
#                     'delivery_rating_by_customer': int(delivery_rating) if pd.notna(delivery_rating) else None,
#                     'delivery_issues': random.choice(['TRAFFIC_DELAY', 'VEHICLE_BREAKDOWN', 'WEATHER_CONDITIONS']) if order.get('delay_days', 0) > 0 else None,
#                     'return_initiated': random.random() < 0.08 if order.get('is_trial_order') else False
#                 })
            
#             shipments_df = pd.DataFrame(shipments)
#             await self.save_dataframe_async(shipments_df, f"{self.output_dir}/logistics_shipments.csv")
            
#             print(f"✅ Generated {len(shipments_df):,} logistics shipments")
#             await self.save_checkpoint_async('logistics_shipments', len(shipments_df))
#             self.completed_stages.add('logistics_shipments')
            
#         except Exception as e:
#             print(f"❌ Error generating logistics shipments: {e}")
#             # Continue execution instead of failing
#             self.completed_stages.add('logistics_shipments')

#     async def generate_demand_forecasts_batch_async(self):
#         """Generate demand forecasts in batches"""
#         if 'demand_forecasts' in self.completed_stages:
#             print("⏭️ Demand forecasts already generated, skipping...")
#             return
        
#         print("📈 Generating demand forecasts...")
        
#         try:
#             # Load required data
#             products_df = pd.read_csv(f"{self.output_dir}/products.csv")
#             line_items_df = pd.read_csv(f"{self.output_dir}/order_line_items.csv")
#             orders_df = pd.read_csv(f"{self.output_dir}/orders.csv")
            
#             # Create order_date mapping
#             order_date_map = dict(zip(orders_df['order_id'], pd.to_datetime(orders_df['order_date'])))
            
#             forecasts = []
#             forecast_methods = ['ARIMA', 'LINEAR_REGRESSION', 'SEASONAL_NAIVE', 'EXPONENTIAL_SMOOTHING', 'ENSEMBLE']
            
#             # Generate forecasts for top products
#             top_products = products_df.head(50)  # Top 50 products
            
#             for _, product in top_products.iterrows():
#                 sku_code = product['sku_code']
                
#                 # Get actual demand data
#                 sku_line_items = line_items_df[line_items_df['sku_code'] == sku_code].copy()
#                 sku_line_items['order_date'] = sku_line_items['order_id'].map(order_date_map)
#                 sku_line_items = sku_line_items.dropna(subset=['order_date'])
                
#                 if len(sku_line_items) == 0:
#                     continue
                
#                 # Group by month
#                 monthly_demand = sku_line_items.groupby(
#                     sku_line_items['order_date'].dt.to_period('M')
#                 )['quantity_ordered'].sum().to_dict()
                
#                 # Generate forecasts for each month in range
#                 current_month = self.start_date.replace(day=1)
#                 end_month = self.end_date.replace(day=1)
                
#                 while current_month <= end_month:
#                     month_period = pd.Period(current_month, freq='M')
#                     actual_demand = monthly_demand.get(month_period, 0)
                    
#                     # Generate forecast
#                     method = random.choice(forecast_methods)
#                     base_forecast = max(1, actual_demand + random.randint(-10, 10))
                    
#                     # Seasonal adjustment
#                     try:
#                         seasonal_factors = json.loads(product.get('seasonal_demand_factor', '{}'))
#                         seasonal_factor = float(seasonal_factors.get(str(current_month.month), 1.0))
#                     except:
#                         seasonal_factor = random.uniform(0.8, 1.2)
                    
#                     # Promotional adjustment
#                     promotional_adjustment = 0
#                     if random.random() < 0.1:  # 10% chance of promotion
#                         promotional_adjustment = int(base_forecast * random.uniform(0.2, 0.5))
                    
#                     final_forecast = int(base_forecast * seasonal_factor) + promotional_adjustment
#                     final_forecast = max(0, final_forecast)
                    
#                     # Calculate error
#                     forecast_error = final_forecast - actual_demand
#                     forecast_error_percentage = (abs(forecast_error) / max(actual_demand, 1)) * 100
                    
#                     # Accuracy rating
#                     if forecast_error_percentage <= 10:
#                         accuracy_rating = 'EXCELLENT'
#                     elif forecast_error_percentage <= 25:
#                         accuracy_rating = 'GOOD'
#                     elif forecast_error_percentage <= 50:
#                         accuracy_rating = 'AVERAGE'
#                     else:
#                         accuracy_rating = 'POOR'
                    
#                     # External factors
#                     external_factors = []
#                     if current_month.month in [10, 11, 12]:
#                         external_factors.append('FESTIVAL_SEASON')
#                     if random.random() < 0.05:
#                         external_factors.append(random.choice(['COMPETITOR_LAUNCH', 'ECONOMIC_SLOWDOWN', 'SUPPLY_SHORTAGE']))
                    
#                     forecasts.append({
#                         'forecast_id': f"FC-{sku_code[-6:]}-{current_month.strftime('%Y%m')}",
#                         'sku_code': sku_code,
#                         'facility_id': 'FAC-HOS-DC',
#                         'forecast_date': current_month - timedelta(days=30),  # Forecast made 1 month prior
#                         'forecast_for_date': current_month,
#                         'forecast_horizon_days': 30,
#                         'forecasting_method': method,
#                         'base_forecast': base_forecast,
#                         'promotional_adjustment': promotional_adjustment,
#                         'seasonal_adjustment': round(seasonal_factor, 2),
#                         'external_factors': json.dumps(external_factors) if external_factors else None,
#                         'final_forecast': final_forecast,
#                         'actual_demand': actual_demand,
#                         'forecast_error': forecast_error,
#                         'forecast_error_percentage': round(forecast_error_percentage, 2),
#                         'forecast_accuracy_rating': accuracy_rating
#                     })
                    
#                     current_month += timedelta(days=32)
#                     current_month = current_month.replace(day=1)
            
#             forecasts_df = pd.DataFrame(forecasts)
#             await self.save_dataframe_async(forecasts_df, f"{self.output_dir}/demand_forecasts.csv")
            
#             print(f"✅ Generated {len(forecasts_df):,} demand forecasts")
#             await self.save_checkpoint_async('demand_forecasts', len(forecasts_df))
#             self.completed_stages.add('demand_forecasts')
            
#         except Exception as e:
#             print(f"❌ Error generating demand forecasts: {e}")

#     async def generate_supply_chain_events_batch_async(self):
#         """Generate supply chain events in batches"""
#         if 'supply_chain_events' in self.completed_stages:
#             print("⏭️ Supply chain events already generated, skipping...")
#             return
        
#         print("🔄 Generating supply chain events...")
        
#         try:
#             # Load required data
#             orders_df = pd.read_csv(f"{self.output_dir}/orders.csv")
#             line_items_df = pd.read_csv(f"{self.output_dir}/order_line_items.csv")
            
#             events = []
#             event_counter = 1
            
#             # Event types and their typical sequence
#             event_sequence = [
#                 'ORDER_RECEIVED', 'INVENTORY_ALLOCATED', 'PRODUCTION_STARTED', 
#                 'PRODUCTION_COMPLETED', 'QC_STARTED', 'QC_COMPLETED', 
#                 'DISPATCHED', 'IN_TRANSIT', 'OUT_FOR_DELIVERY', 'DELIVERED'
#             ]
            
#             for _, order in orders_df.iterrows():
#                 order_date = pd.to_datetime(order['order_date'])
#                 order_lines = line_items_df[line_items_df['order_id'] == order['order_id']]
                
#                 # Order received event
#                 events.append({
#                     'event_id': f"EVENT-ORD-{order_date.strftime('%Y%m%d')}-{event_counter:06d}",
#                     'related_order_id': order['order_id'],
#                     'related_sku_code': None,
#                     'facility_id': None,
#                     'event_type': 'ORDER_RECEIVED',
#                     'event_timestamp': order_date + timedelta(hours=random.randint(0, 2)),
#                     'expected_completion_time': order_date + timedelta(hours=1),
#                     'actual_completion_time': order_date + timedelta(minutes=random.randint(5, 60)),
#                     'duration_minutes': random.randint(5, 60),
#                     'delay_minutes': max(0, random.randint(-10, 30)),
#                     'delay_category': 'NO_DELAY',
#                     'delay_root_cause': None,
#                     'responsible_team': 'SALES',
#                     'resolution_action': 'Order processed successfully',
#                     'impact_on_customer': 'NONE',
#                     'cost_of_delay': 0
#                 })
#                 event_counter += 1
                
#                 # Generate events for each line item
#                 for _, line_item in order_lines.iterrows():
#                     current_time = order_date + timedelta(hours=2)
                    
#                     # Inventory allocation
#                     inventory_delay = random.randint(0, 120) if random.random() < 0.1 else 0
#                     events.append({
#                         'event_id': f"EVENT-INV-{current_time.strftime('%Y%m%d')}-{event_counter:06d}",
#                         'related_order_id': order['order_id'],
#                         'related_sku_code': line_item['sku_code'],
#                         'facility_id': line_item.get('manufacturing_facility_id', 'FAC-HOS-MFG'),
#                         'event_type': 'INVENTORY_ALLOCATED',
#                         'event_timestamp': current_time,
#                         'expected_completion_time': current_time + timedelta(hours=1),
#                         'actual_completion_time': current_time + timedelta(minutes=60 + inventory_delay),
#                         'duration_minutes': 60 + inventory_delay,
#                         'delay_minutes': inventory_delay,
#                         'delay_category': 'INVENTORY_SHORTAGE' if inventory_delay > 0 else 'NO_DELAY',
#                         'delay_root_cause': 'Stock shortage' if inventory_delay > 0 else None,
#                         'responsible_team': 'PRODUCTION',
#                         'resolution_action': 'Stock allocated' if inventory_delay == 0 else 'Expedited production',
#                         'impact_on_customer': 'NONE' if inventory_delay == 0 else 'MINOR',
#                         'cost_of_delay': inventory_delay * 0.5
#                     })
#                     event_counter += 1
#                     current_time += timedelta(minutes=60 + inventory_delay)
                    
#                     # Production events
#                     if pd.notna(line_item.get('estimated_manufacturing_date')):
#                         prod_delay = random.randint(0, 480) if random.random() < 0.15 else 0
#                         delay_reasons = ['SUPPLIER_DELAY', 'EQUIPMENT_BREAKDOWN', 'LABOR_SHORTAGE', 'QUALITY_REWORK']
                        
#                         events.append({
#                             'event_id': f"EVENT-PRD-{current_time.strftime('%Y%m%d')}-{event_counter:06d}",
#                             'related_order_id': order['order_id'],
#                             'related_sku_code': line_item['sku_code'],
#                             'facility_id': line_item.get('manufacturing_facility_id', 'FAC-HOS-MFG'),
#                             'event_type': 'PRODUCTION_STARTED',
#                             'event_timestamp': current_time + timedelta(minutes=prod_delay),
#                             'expected_completion_time': current_time,
#                             'actual_completion_time': current_time + timedelta(minutes=prod_delay),
#                             'duration_minutes': 0,
#                             'delay_minutes': prod_delay,
#                             'delay_category': random.choice(delay_reasons) if prod_delay > 0 else 'NO_DELAY',
#                             'delay_root_cause': random.choice(delay_reasons) if prod_delay > 0 else None,
#                             'responsible_team': 'PRODUCTION',
#                             'resolution_action': 'Production started' if prod_delay == 0 else f'Resolved delay and started production',
#                             'impact_on_customer': 'NONE' if prod_delay <= 240 else 'MODERATE',
#                             'cost_of_delay': prod_delay * 1.2
#                         })
#                         event_counter += 1
                    
#                     # Quality check events
#                     qc_delay = random.randint(60, 480) if line_item.get('quality_check_status') == 'REWORK' else 0
#                     events.append({
#                         'event_id': f"EVENT-QC-{current_time.strftime('%Y%m%d')}-{event_counter:06d}",
#                         'related_order_id': order['order_id'],
#                         'related_sku_code': line_item['sku_code'],
#                         'facility_id': line_item.get('manufacturing_facility_id', 'FAC-HOS-MFG'),
#                         'event_type': 'QC_COMPLETED',
#                         'event_timestamp': current_time + timedelta(minutes=60 + qc_delay),
#                         'expected_completion_time': current_time + timedelta(hours=1),
#                         'actual_completion_time': current_time + timedelta(minutes=60 + qc_delay),
#                         'duration_minutes': 60 + qc_delay,
#                         'delay_minutes': qc_delay,
#                         'delay_category': 'QUALITY_ISSUE' if qc_delay > 0 else 'NO_DELAY',
#                         'delay_root_cause': 'Quality rework required' if qc_delay > 0 else None,
#                         'responsible_team': 'QC',
#                         'resolution_action': 'Quality check passed' if qc_delay == 0 else 'Issues resolved through rework',
#                         'impact_on_customer': 'NONE' if qc_delay == 0 else 'MODERATE',
#                         'cost_of_delay': qc_delay * 0.8
#                     })
#                     event_counter += 1
                    
#                     # Dispatch event
#                     if pd.notna(order.get('actual_dispatch_date')):
#                         dispatch_delay = random.randint(0, 240) if order.get('otif_status') in ['LATE', 'FAILED'] else 0
#                         dispatch_time = pd.to_datetime(order['actual_dispatch_date']) + timedelta(minutes=dispatch_delay)
                        
#                         events.append({
#                             'event_id': f"EVENT-DIS-{dispatch_time.strftime('%Y%m%d')}-{event_counter:06d}",
#                             'related_order_id': order['order_id'],
#                             'related_sku_code': line_item['sku_code'],
#                             'facility_id': line_item.get('dispatch_facility_id', 'FAC-HOS-MFG'),
#                             'event_type': 'DISPATCHED',
#                             'event_timestamp': dispatch_time,
#                             'expected_completion_time': pd.to_datetime(order['actual_dispatch_date']),
#                             'actual_completion_time': dispatch_time,
#                             'duration_minutes': 60 + dispatch_delay,
#                             'delay_minutes': dispatch_delay,
#                             'delay_category': random.choice(['LOGISTICS_ISSUE', 'PACKAGING_DELAY', 'CARRIER_UNAVAILABLE']) if dispatch_delay > 0 else 'NO_DELAY',
#                             'delay_root_cause': 'Logistics coordination' if dispatch_delay > 0 else None,
#                             'responsible_team': 'LOGISTICS',
#                             'resolution_action': 'Dispatched on schedule' if dispatch_delay == 0 else 'Resolved logistics issue',
#                             'impact_on_customer': 'NONE' if dispatch_delay <= 120 else 'MAJOR',
#                             'cost_of_delay': dispatch_delay * 1.5
#                         })
#                         event_counter += 1
                    
#                     # Delivery event
#                     if pd.notna(order.get('actual_delivery_date')):
#                         delivery_delay = max(0, order.get('delay_days', 0) * 1440)  # Convert days to minutes
#                         delivery_time = pd.to_datetime(order['actual_delivery_date'])
                        
#                         events.append({
#                             'event_id': f"EVENT-DEL-{delivery_time.strftime('%Y%m%d')}-{event_counter:06d}",
#                             'related_order_id': order['order_id'],
#                             'related_sku_code': line_item['sku_code'],
#                             'facility_id': None,
#                             'event_type': 'DELIVERED' if order['delivery_status'] == 'DELIVERED' else 'DELIVERY_FAILED',
#                             'event_timestamp': delivery_time,
#                             'expected_completion_time': pd.to_datetime(order['promised_delivery_date']),
#                             'actual_completion_time': delivery_time,
#                             'duration_minutes': 30,
#                             'delay_minutes': delivery_delay,
#                             'delay_category': random.choice(['TRAFFIC_DELAY', 'CUSTOMER_UNAVAILABLE', 'ADDRESS_ISSUE']) if delivery_delay > 0 else 'NO_DELAY',
#                             'delay_root_cause': 'Last mile delivery issues' if delivery_delay > 0 else None,
#                             'responsible_team': 'LOGISTICS',
#                             'resolution_action': 'Delivered successfully' if order['delivery_status'] == 'DELIVERED' else 'Delivery failed',
#                             'impact_on_customer': 'NONE' if delivery_delay == 0 else 'MAJOR',
#                             'cost_of_delay': delivery_delay * 2.0
#                         })
#                         event_counter += 1
            
#             events_df = pd.DataFrame(events)
#             await self.save_dataframe_async(events_df, f"{self.output_dir}/supply_chain_events.csv")
            
#             print(f"✅ Generated {len(events_df):,} supply chain events")
#             await self.save_checkpoint_async('supply_chain_events', len(events_df))
#             self.completed_stages.add('supply_chain_events')
            
#         except Exception as e:
#             print(f"❌ Error generating supply chain events: {e}")



# # ASYNC RUNNER AND MAIN EXECUTION

# async def run_optimized_generation():
#     """Main async runner"""
#     generator = OptimizedWakefitDataGenerator(
#         start_date='2023-01-01',
#         end_date='2024-12-31',
#         output_dir='wakefit_data_optimized'
#     )
    
#     try:
#         summary = await generator.generate_all_data_async()
        
#         print(f"\n🎉 Optimized Generation Complete!")
#         print(f"📁 Data Location: {generator.output_dir}")
#         print(f"📊 Total Records: {summary.get('total_records', 0):,}")
#         print(f"💾 Total Size: {summary.get('total_size_mb', 0):.1f} MB")
        
#         return summary
        
#     except Exception as e:
#         print(f"❌ Generation failed: {e}")
#         import traceback
#         traceback.print_exc()
#         return None

# def main():
#     """Main execution function"""
#     print("🚀 Starting Optimized Wakefit Data Generation")
#     print("⚡ Features: Async Processing, Batch Saves, Memory Optimization")
    
#     # Monitor system resources
#     print(f"💻 System Info:")
#     print(f"   CPU Cores: {os.cpu_count()}")
#     print(f"   Available RAM: {psutil.virtual_memory().available / 1024**3:.1f} GB")
    
#     # Run async generation
#     summary = asyncio.run(run_optimized_generation())
    
#     if summary:
#         print(f"\n✅ SUCCESS: All data generated successfully!")
#         print(f"🔍 Ready for Analytics:")
#         print(f"   • Orders at risk analysis")
#         print(f"   • OTIF failure root cause analysis")
#         print(f"   • Customer delivery sensitivity analysis")
#         print(f"   • SKU delay pattern analysis")
#         print(f"   • Demand forecast accuracy analysis")
#     else:
#         print(f"\n❌ FAILED: Data generation encountered errors")
#         return 1
    
#     return 0

# if __name__ == "__main__":
#     import sys
#     sys.exit(main())

"""
Final Wakefit 3-Month Supply Chain Data Generator
===============================================

Complete solution with all ID generation issues fixed:
- UUID-based unique identifiers where needed
- Collision detection and prevention
- Comprehensive validation
- Database-ready with guaranteed referential integrity
"""

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta, date, time
import json
from faker import Faker
import os
import uuid
from typing import Dict, List, Tuple, Any, Set
import warnings
warnings.filterwarnings('ignore')

# Initialize Faker for Indian data
fake = Faker('en_IN')
Faker.seed(42)  # For reproducible results
np.random.seed(42)
random.seed(42)

class WakefitFinalDataGenerator:
    """Final data generator with all ID collision issues resolved"""
    
    def __init__(self, output_dir='wakefit_final_data'):
        # Date range: Jan 1 - Mar 31, 2024 (90 days)
        self.start_date = datetime(2024, 1, 1)
        self.end_date = datetime(2024, 3, 31)
        self.output_dir = output_dir
        
        # Create output directory
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Business parameters
        self.daily_orders = 100  # 100 orders per day = 9,000 total
        self.otif_target = 0.92
        
        # Data containers
        self.products_data = []
        self.customers_data = []
        self.facilities_data = []
        self.suppliers_data = []
        self.orders_data = []
        self.order_line_items_data = []
        self.purchase_orders_data = []
        self.production_batches_data = []
        self.inventory_movements_data = []
        self.logistics_shipments_data = []
        self.supply_chain_events_data = []
        self.demand_forecasts_data = []
        
        # Validation sets for foreign key checking
        self.valid_customer_ids = set()
        self.valid_order_ids = set()
        self.valid_sku_codes = set()
        self.valid_facility_ids = set()
        self.valid_supplier_ids = set()
        
        # ID collision prevention sets
        self.used_forecast_ids = set()
        self.used_event_ids = set()
        self.used_batch_ids = set()
        self.used_movement_ids = set()
        self.used_shipment_ids = set()
        self.used_po_ids = set()
        
        # Global counters
        self.global_order_counter = 1
        self.global_line_item_counter = 1
        self.global_event_counter = 1
        self.global_movement_counter = 1
        self.global_forecast_counter = 1
        
        # Session UUID for unique identification
        self.session_id = str(uuid.uuid4())[:8]
        
        print(f"Wakefit Final Data Generator Initialized")
        print(f"Date Range: {self.start_date.date()} to {self.end_date.date()}")
        print(f"Session ID: {self.session_id}")
        print(f"All ID collision issues resolved")

    def generate_unique_forecast_id(self, sku_code: str, month: int, year: int) -> str:
        """Generate guaranteed unique forecast ID"""
        # Use first 3 chars of category + last 3 chars for uniqueness
        sku_parts = sku_code.split('-')
        sku_identifier = f"{sku_parts[0][:3]}{sku_parts[-1][:3]}" if len(sku_parts) >= 2 else sku_code[:6]
        
        base_id = f"FC-{sku_identifier}-{year}{month:02d}"
        
        # If collision detected, add session ID and counter
        if base_id in self.used_forecast_ids:
            unique_id = f"FC-{self.global_forecast_counter:04d}-{year}{month:02d}-{self.session_id[:4]}"
            self.global_forecast_counter += 1
        else:
            unique_id = base_id
        
        self.used_forecast_ids.add(unique_id)
        return unique_id

    def generate_unique_event_id(self) -> str:
        """Generate guaranteed unique event ID"""
        unique_id = f"EVT-{self.session_id[:4]}-{self.global_event_counter:08d}"
        self.global_event_counter += 1
        self.used_event_ids.add(unique_id)
        return unique_id

    def generate_unique_movement_id(self, movement_type: str) -> str:
        """Generate guaranteed unique inventory movement ID"""
        type_code = movement_type[:4].upper()
        unique_id = f"INV-{type_code}-{self.session_id[:4]}-{self.global_movement_counter:06d}"
        self.global_movement_counter += 1
        self.used_movement_ids.add(unique_id)
        return unique_id

    def generate_all_data(self):
        """Main orchestrator with complete validation"""
        print("\nStarting Complete Data Generation...")
        
        # Phase 1: Master Data
        print("\nPhase 1: Generating Master Data...")
        self.generate_products()
        self.validate_products()
        
        self.generate_customers()
        self.validate_customers()
        
        self.generate_facilities()
        self.validate_facilities()
        
        self.generate_suppliers()
        self.validate_suppliers()
        
        # Phase 2: Orders (Critical Path)
        print("\nPhase 2: Generating Orders...")
        self.generate_orders()
        self.validate_orders()
        
        # Phase 3: Order Line Items (Depends on Orders)
        print("\nPhase 3: Generating Order Line Items...")
        self.generate_order_line_items()
        self.validate_order_line_items()
        
        # Phase 4: Other Transactional Data
        print("\nPhase 4: Generating Production Data...")
        self.generate_purchase_orders()
        self.validate_purchase_orders()
        
        self.generate_production_batches()
        self.validate_production_batches()
        
        # Phase 5: Operational Data
        print("\nPhase 5: Generating Operational Data...")
        self.generate_inventory_movements()
        self.validate_inventory_movements()
        
        self.generate_logistics_shipments()
        self.validate_logistics_shipments()
        
        self.generate_supply_chain_events()
        self.validate_supply_chain_events()
        
        self.generate_demand_forecasts()
        self.validate_demand_forecasts()
        
        # Phase 6: Final Validation and Save
        print("\nPhase 6: Final Validation and Save...")
        self.perform_comprehensive_validation()
        self.save_all_datasets()
        self.generate_summary_report()
        
        print(f"\nComplete! All datasets generated in: {self.output_dir}")

    def generate_products(self):
        """Generate 10 products with guaranteed unique SKU codes"""
        print("Generating 10 products...")
        
        # Predefined unique SKU codes to prevent any duplicates
        products_config = [
            # Mattresses (3)
            {'sku': 'MAT001-MEMORY-QUE-6IN', 'category': 'MATTRESS', 'sub_category': 'MEMORY_FOAM', 'size': 'QUEEN', 'thickness': 6, 
             'name': 'Wakefit Memory Foam 6inch Queen Mattress', 'price': 15000, 'cost': 9000, 'weight': 35},
            {'sku': 'MAT002-ORTHOP-KIN-8IN', 'category': 'MATTRESS', 'sub_category': 'ORTHOPEDIC', 'size': 'KING', 'thickness': 8,
             'name': 'Wakefit Orthopedic 8inch King Mattress', 'price': 22000, 'cost': 13500, 'weight': 50},
            {'sku': 'MAT003-DUALCO-DOU-5IN', 'category': 'MATTRESS', 'sub_category': 'DUAL_COMFORT', 'size': 'DOUBLE', 'thickness': 5,
             'name': 'Wakefit Dual Comfort 5inch Double Mattress', 'price': 12000, 'cost': 7500, 'weight': 25},
            
            # Furniture (3)
            {'sku': 'BED001-STORAG-QUE-STD', 'category': 'BED', 'sub_category': 'STORAGE', 'size': 'QUEEN', 'thickness': None,
             'name': 'Wakefit Storage Queen Bed', 'price': 25000, 'cost': 15000, 'weight': 60},
            {'sku': 'SOF001-3SEATE-REG-STD', 'category': 'SOFA', 'sub_category': '3_SEATER', 'size': 'REGULAR', 'thickness': None,
             'name': 'Wakefit 3 Seater Regular Sofa', 'price': 35000, 'cost': 21000, 'weight': 80},
            {'sku': 'CHA001-OFFICE-STD-STD', 'category': 'CHAIR', 'sub_category': 'OFFICE', 'size': 'STANDARD', 'thickness': None,
             'name': 'Wakefit Office Standard Chair', 'price': 8000, 'cost': 4800, 'weight': 15},
            
            # Storage (2)
            {'sku': 'STO001-WARDRO-LAR-STD', 'category': 'STORAGE', 'sub_category': 'WARDROBE', 'size': 'LARGE', 'thickness': None,
             'name': 'Wakefit Large Wardrobe', 'price': 28000, 'cost': 17000, 'weight': 90},
            {'sku': 'STO002-BOOKSH-STD-STD', 'category': 'STORAGE', 'sub_category': 'BOOKSHELF', 'size': 'STANDARD', 'thickness': None,
             'name': 'Wakefit Standard Bookshelf', 'price': 12000, 'cost': 7200, 'weight': 25},
            
            # Accessories (2)
            {'sku': 'PIL001-MEMORY-STD-STD', 'category': 'PILLOW', 'sub_category': 'MEMORY_FOAM', 'size': 'STANDARD', 'thickness': None,
             'name': 'Wakefit Memory Foam Standard Pillow', 'price': 1200, 'cost': 600, 'weight': 1.5},
            {'sku': 'BDG001-COMFOR-QUE-STD', 'category': 'BEDDING', 'sub_category': 'COMFORTER', 'size': 'QUEEN', 'thickness': None,
             'name': 'Wakefit Queen Comforter', 'price': 2500, 'cost': 1200, 'weight': 2.5}
        ]
        
        for config in products_config:
            sku_code = config['sku']
            
            # Dimensions based on category and size
            dimensions = self._get_product_dimensions(config['category'], config['size'], config.get('thickness'))
            
            # Manufacturing complexity
            complexity_map = {
                'MATTRESS': 'MEDIUM', 'BED': 'COMPLEX', 'SOFA': 'COMPLEX', 
                'CHAIR': 'SIMPLE', 'STORAGE': 'COMPLEX', 'PILLOW': 'SIMPLE', 'BEDDING': 'SIMPLE'
            }
            complexity = complexity_map[config['category']]
            
            # Production time
            time_map = {'SIMPLE': 2, 'MEDIUM': 6, 'COMPLEX': 18}
            production_time = time_map[complexity] + random.uniform(-1, 1)
            
            # Materials
            materials = self._get_materials_for_product(config['category'])
            
            # Seasonal demand
            seasonal_factor = self._get_seasonal_factors(config['category'])
            
            product = {
                'sku_code': sku_code,
                'product_name': config['name'],
                'category': config['category'],
                'sub_category': config['sub_category'],
                'size_variant': config['size'],
                'manufacturing_complexity': complexity,
                'standard_production_time_hours': round(production_time, 2),
                'is_customizable': config['category'] in ['MATTRESS', 'BED', 'SOFA', 'STORAGE'],
                'weight_kg': config['weight'],
                'dimensions_lxwxh_cm': dimensions,
                'is_bulky_item': config['category'] in ['BED', 'SOFA', 'STORAGE'],
                'raw_materials_list': json.dumps(materials),
                'minimum_inventory_days': random.randint(7, 21),
                'maximum_inventory_days': random.randint(60, 120),
                'supplier_lead_time_days': random.randint(15, 30),
                'seasonal_demand_factor': json.dumps(seasonal_factor),
                'price_inr': config['price'],
                'cost_inr': config['cost'],
                'launch_date': fake.date_between(start_date=date(2020, 1, 1), end_date=date(2023, 12, 31)),
                'discontinuation_date': None
            }
            self.products_data.append(product)
        
        print(f"Generated {len(self.products_data)} products")

    def validate_products(self):
        """Validate products and build SKU lookup set"""
        print("Validating products...")
        
        # Check for duplicate SKU codes
        sku_codes = [p['sku_code'] for p in self.products_data]
        duplicate_skus = set([sku for sku in sku_codes if sku_codes.count(sku) > 1])
        
        if duplicate_skus:
            raise ValueError(f"Duplicate SKU codes found: {duplicate_skus}")
        
        # Build valid SKU codes set
        self.valid_sku_codes = set(sku_codes)
        print(f"Products validated. {len(self.valid_sku_codes)} unique SKU codes registered")

    def generate_customers(self):
        """Generate 2,000 customers with guaranteed unique IDs"""
        print("Generating 2,000 customers...")
        
        # Indian cities and states
        cities = ['Bangalore', 'Mumbai', 'Delhi', 'Hyderabad', 'Chennai', 'Pune', 'Kolkata', 'Ahmedabad']
        states = ['Karnataka', 'Maharashtra', 'Delhi', 'Telangana', 'Tamil Nadu', 'West Bengal', 'Gujarat']
        
        # Customer segments
        segments = ['REGULAR', 'PREMIUM', 'BULK', 'PRICE_SENSITIVE']
        segment_weights = [0.6, 0.25, 0.1, 0.05]
        
        used_customer_ids = set()
        
        for i in range(2000):
            # Generate unique customer ID with session prefix to prevent collisions
            base_customer_id = f"CUS-{self.session_id[:4]}-{str(i+1).zfill(4)}"
            customer_id = base_customer_id
            
            # Ensure absolute uniqueness
            counter = 1
            while customer_id in used_customer_ids:
                customer_id = f"CUS-{self.session_id[:4]}-{str(i+1).zfill(4)}-{counter}"
                counter += 1
            
            used_customer_ids.add(customer_id)
            
            segment = np.random.choice(segments, p=segment_weights)
            city = random.choice(cities)
            state = random.choice(states)
            reg_date = fake.date_between(start_date=date(2020, 1, 1), end_date=date(2023, 12, 31))
            
            # Segment-specific characteristics
            if segment == 'PREMIUM':
                avg_order_value = random.uniform(25000, 60000)
                delivery_sensitivity = random.randint(8, 10)
                frequency_days = random.randint(60, 120)
            elif segment == 'BULK':
                avg_order_value = random.uniform(100000, 300000)
                delivery_sensitivity = random.randint(3, 5)
                frequency_days = random.randint(180, 365)
            elif segment == 'PRICE_SENSITIVE':
                avg_order_value = random.uniform(5000, 15000)
                delivery_sensitivity = random.randint(2, 4)
                frequency_days = random.randint(300, 720)
            else:  # REGULAR
                avg_order_value = random.uniform(10000, 30000)
                delivery_sensitivity = random.randint(5, 7)
                frequency_days = random.randint(180, 365)
            
            customer = {
                'customer_id': customer_id,
                'customer_type': 'B2B_HOSPITALITY' if segment == 'BULK' else 'B2C',
                'registration_date': reg_date,
                'primary_channel': np.random.choice(['WEBSITE', 'APP', 'AMAZON', 'FLIPKART', 'STORE'], 
                                                   p=[0.35, 0.25, 0.2, 0.15, 0.05]),
                'delivery_city': city,
                'delivery_state': state,
                'pincode': fake.postcode(),
                'customer_segment': segment,
                'delivery_sensitivity_score': delivery_sensitivity,
                'lifetime_orders': max(1, random.randint(1, 8)),
                'lifetime_value': round(avg_order_value * random.uniform(0.5, 2.0), 2),
                'avg_order_frequency_days': frequency_days,
                'preferred_delivery_window': np.random.choice(['MORNING', 'AFTERNOON', 'EVENING', 'ANYTIME'],
                                                            p=[0.3, 0.25, 0.2, 0.25]),
                'last_order_date': fake.date_between(start_date=reg_date, end_date=date(2023, 12, 31))
            }
            self.customers_data.append(customer)
        
        print(f"Generated {len(self.customers_data)} customers")

    def validate_customers(self):
        """Validate customers and build customer lookup set"""
        print("Validating customers...")
        
        customer_ids = [c['customer_id'] for c in self.customers_data]
        duplicate_customers = set([cid for cid in customer_ids if customer_ids.count(cid) > 1])
        
        if duplicate_customers:
            raise ValueError(f"Duplicate customer IDs found: {duplicate_customers}")
        
        self.valid_customer_ids = set(customer_ids)
        print(f"Customers validated. {len(self.valid_customer_ids)} unique customer IDs registered")

    def generate_facilities(self):
        """Generate 5 facilities"""
        print("Generating 5 facilities...")
        
        facilities_config = [
            {
                'id': 'FAC-HOS-MFG', 'name': 'Wakefit Manufacturing - Hosur', 'type': 'MANUFACTURING',
                'city': 'Hosur', 'state': 'Tamil Nadu', 'pincode': '635109',
                'capacity': 300, 'capabilities': ['MATTRESS', 'BED', 'PILLOW', 'BEDDING']
            },
            {
                'id': 'FAC-HOS-DC', 'name': 'Wakefit Main Distribution Center - Hosur', 'type': 'DC',
                'city': 'Hosur', 'state': 'Tamil Nadu', 'pincode': '635109',
                'capacity': 1000, 'capabilities': ['ALL_PRODUCTS']
            },
            {
                'id': 'FAC-BAN-WH', 'name': 'Wakefit Warehouse - Bangalore', 'type': 'WAREHOUSE',
                'city': 'Bangalore', 'state': 'Karnataka', 'pincode': '560045',
                'capacity': 400, 'capabilities': ['ALL_PRODUCTS']
            },
            {
                'id': 'FAC-MUM-WH', 'name': 'Wakefit Warehouse - Mumbai', 'type': 'WAREHOUSE',
                'city': 'Mumbai', 'state': 'Maharashtra', 'pincode': '400001',
                'capacity': 350, 'capabilities': ['ALL_PRODUCTS']
            },
            {
                'id': 'FAC-DEL-WH', 'name': 'Wakefit Warehouse - Delhi', 'type': 'WAREHOUSE',
                'city': 'Delhi', 'state': 'Delhi', 'pincode': '110001',
                'capacity': 300, 'capabilities': ['ALL_PRODUCTS']
            }
        ]
        
        for config in facilities_config:
            facility = {
                'facility_id': config['id'],
                'facility_name': config['name'],
                'facility_type': config['type'],
                'location_city': config['city'],
                'location_state': config['state'],
                'pincode': config['pincode'],
                'capacity_units_per_day': config['capacity'],
                'product_capabilities': json.dumps(config['capabilities']),
                'serving_regions': json.dumps([config['state']]),
                'operational_status': 'ACTIVE',
                'setup_date': fake.date_between(start_date=date(2018, 1, 1), end_date=date(2022, 12, 31))
            }
            self.facilities_data.append(facility)
        
        print(f"Generated {len(self.facilities_data)} facilities")

    def validate_facilities(self):
        """Validate facilities"""
        facility_ids = [f['facility_id'] for f in self.facilities_data]
        self.valid_facility_ids = set(facility_ids)
        print(f"Facilities validated. {len(self.valid_facility_ids)} facility IDs registered")

    def generate_suppliers(self):
        """Generate 5 suppliers"""
        print("Generating 5 suppliers...")
        
        suppliers_config = [
            {
                'id': 'SUP-FOAM-001', 'name': 'Premium Foam Industries', 'country': 'India',
                'type': 'FOAM', 'lead_time': 20, 'moq': 500, 'quality': 4.2, 'reliability': 4.0
            },
            {
                'id': 'SUP-FABRIC-001', 'name': 'Textile Masters Pvt Ltd', 'country': 'India',
                'type': 'FABRIC', 'lead_time': 25, 'moq': 1000, 'quality': 4.0, 'reliability': 4.2
            },
            {
                'id': 'SUP-WOOD-001', 'name': 'Timber Works Corporation', 'country': 'India',
                'type': 'WOOD', 'lead_time': 35, 'moq': 200, 'quality': 4.3, 'reliability': 4.1
            },
            {
                'id': 'SUP-HARDWARE-001', 'name': 'Metal Components Ltd', 'country': 'India',
                'type': 'HARDWARE', 'lead_time': 18, 'moq': 2000, 'quality': 4.1, 'reliability': 4.4
            },
            {
                'id': 'SUP-PACKAGING-001', 'name': 'Pack Solutions India', 'country': 'India',
                'type': 'PACKAGING', 'lead_time': 15, 'moq': 1000, 'quality': 3.8, 'reliability': 4.0
            }
        ]
        
        for config in suppliers_config:
            supplier = {
                'supplier_id': config['id'],
                'supplier_name': config['name'],
                'supplier_country': config['country'],
                'supplier_type': config['type'],
                'materials_supplied': json.dumps([config['type'].lower()]),
                'standard_lead_time_days': config['lead_time'],
                'minimum_order_quantity': config['moq'],
                'quality_rating_5': config['quality'],
                'reliability_rating_5': config['reliability'],
                'cost_competitiveness': 'LOW',
                'contract_start_date': fake.date_between(start_date=date(2020, 1, 1), end_date=date(2023, 12, 31)),
                'contract_end_date': fake.date_between(start_date=date(2025, 1, 1), end_date=date(2026, 12, 31)),
                'payment_terms_days': random.choice([30, 45, 60])
            }
            self.suppliers_data.append(supplier)
        
        print(f"Generated {len(self.suppliers_data)} suppliers")

    def validate_suppliers(self):
        """Validate suppliers"""
        supplier_ids = [s['supplier_id'] for s in self.suppliers_data]
        self.valid_supplier_ids = set(supplier_ids)
        print(f"Suppliers validated. {len(self.valid_supplier_ids)} supplier IDs registered")

    def generate_orders(self):
        """Generate orders with guaranteed unique IDs"""
        print("Generating 9,000 orders over 90 days...")
        
        current_date = self.start_date
        
        while current_date <= self.end_date:
            daily_orders = int(self.daily_orders * random.uniform(0.7, 1.3))
            
            if current_date.weekday() in [5, 6]:
                daily_orders = int(daily_orders * 1.2)
            
            for i in range(daily_orders):
                customer = random.choice(self.customers_data)
                
                # Generate GUARANTEED unique order ID
                order_id = f"ORD-{self.session_id[:4]}-{current_date.strftime('%Y%m%d')}-{str(self.global_order_counter).zfill(6)}"
                
                if customer['customer_id'] not in self.valid_customer_ids:
                    raise ValueError(f"Invalid customer_id: {customer['customer_id']}")
                
                order_time = time(
                    hour=random.randint(6, 23),
                    minute=random.randint(0, 59)
                )
                
                channel = customer['primary_channel']
                segment = customer['customer_segment']
                
                # Delivery expectations
                if segment == 'PREMIUM':
                    delivery_days = random.randint(2, 4)
                elif channel in ['AMAZON', 'FLIPKART']:
                    delivery_days = random.randint(3, 6)
                else:
                    delivery_days = random.randint(4, 8)
                
                customer_expectation = current_date.date() + timedelta(days=delivery_days)
                promised_delivery = customer_expectation + timedelta(days=random.randint(0, 2))
                
                # Order size based on segment
                if segment == 'BULK':
                    total_items = random.randint(5, 15)
                    total_quantity = random.randint(20, 100)
                    gross_value = random.uniform(80000, 250000)
                elif segment == 'PREMIUM':
                    total_items = random.randint(2, 6)
                    total_quantity = random.randint(3, 12)
                    gross_value = random.uniform(20000, 60000)
                elif segment == 'PRICE_SENSITIVE':
                    total_items = random.randint(1, 3)
                    total_quantity = random.randint(1, 5)
                    gross_value = random.uniform(5000, 18000)
                else:  # REGULAR
                    total_items = random.randint(1, 4)
                    total_quantity = random.randint(1, 8)
                    gross_value = random.uniform(10000, 35000)
                
                # Payment method
                if channel in ['AMAZON', 'FLIPKART']:
                    payment_method = np.random.choice(['PREPAID', 'COD'], p=[0.7, 0.3])
                else:
                    payment_method = np.random.choice(['COD', 'PREPAID'], p=[0.6, 0.4])
                
                # OTIF simulation
                will_be_otif = random.random() < self.otif_target
                
                if will_be_otif:
                    actual_delivery = promised_delivery - timedelta(days=random.randint(0, 1))
                    delay_days = max(0, (actual_delivery - promised_delivery).days)
                    otif_status = 'ON_TIME_IN_FULL'
                    delivery_status = 'DELIVERED'
                    satisfaction = random.choice([4, 5])
                    nps = random.randint(7, 10)
                else:
                    delay_days = random.randint(1, 6)
                    actual_delivery = promised_delivery + timedelta(days=delay_days)
                    
                    if delay_days <= 2:
                        otif_status = 'LATE'
                        satisfaction = random.choice([3, 4])
                        nps = random.randint(5, 7)
                    else:
                        otif_status = np.random.choice(['LATE', 'INCOMPLETE'], p=[0.7, 0.3])
                        satisfaction = random.choice([1, 2, 3])
                        nps = random.randint(-5, 4)
                    
                    delivery_status = 'DELIVERED' if otif_status != 'INCOMPLETE' else 'PARTIAL'
                
                # Discounts
                discount_rate = random.uniform(0.05, 0.12) if channel in ['WEBSITE', 'APP'] else 0
                discount_amount = gross_value * discount_rate
                net_value = gross_value - discount_amount
                
                order = {
                    'order_id': order_id,
                    'customer_id': customer['customer_id'],
                    'order_date': current_date.date(),
                    'order_time': order_time,
                    'channel': channel,
                    'store_id': None,
                    'total_items': total_items,
                    'total_quantity': total_quantity,
                    'gross_order_value': round(gross_value, 2),
                    'discount_amount': round(discount_amount, 2),
                    'net_order_value': round(net_value, 2),
                    'payment_method': payment_method,
                    'payment_status': 'PAID' if payment_method == 'PREPAID' else 'PENDING',
                    'customer_delivery_expectation': customer_expectation,
                    'promised_delivery_date': promised_delivery,
                    'delivery_address_full': f"{fake.street_address()}, {customer['delivery_city']}, {customer['delivery_state']}",
                    'delivery_pincode': customer['pincode'],
                    'delivery_instructions': fake.sentence() if random.random() < 0.2 else None,
                    'order_priority': 'BULK' if segment == 'BULK' else 'STANDARD',
                    'is_trial_order': random.random() < 0.1,
                    'estimated_dispatch_date': promised_delivery - timedelta(days=2),
                    'actual_dispatch_date': actual_delivery - timedelta(days=1),
                    'estimated_delivery_date': promised_delivery,
                    'actual_delivery_date': actual_delivery,
                    'delivery_status': delivery_status,
                    'delivery_attempts': random.randint(1, 2) if delivery_status == 'DELIVERED' else 1,
                    'otif_status': otif_status,
                    'delay_days': delay_days,
                    'customer_satisfaction_rating': satisfaction,
                    'nps_score': nps
                }
                
                self.orders_data.append(order)
                self.global_order_counter += 1
            
            current_date += timedelta(days=1)
        
        print(f"Generated {len(self.orders_data)} orders with unique IDs")

    def validate_orders(self):
        """Validate orders"""
        print("Validating orders...")
        
        order_ids = [o['order_id'] for o in self.orders_data]
        duplicate_orders = set([oid for oid in order_ids if order_ids.count(oid) > 1])
        
        if duplicate_orders:
            raise ValueError(f"Duplicate order IDs found: {duplicate_orders}")
        
        # Validate customer references
        invalid_customers = []
        for order in self.orders_data:
            if order['customer_id'] not in self.valid_customer_ids:
                invalid_customers.append(order['customer_id'])
        
        if invalid_customers:
            raise ValueError(f"Orders reference invalid customer_ids: {set(invalid_customers)}")
        
        self.valid_order_ids = set(order_ids)
        print(f"Orders validated. {len(self.valid_order_ids)} unique order IDs registered")

    def generate_order_line_items(self):
        """Generate line items with validated foreign keys"""
        print("Generating order line items...")
        
        for order in self.orders_data:
            if order['order_id'] not in self.valid_order_ids:
                raise ValueError(f"Order ID not found: {order['order_id']}")
            
            total_items = order['total_items']
            order_value = order['gross_order_value']
            
            remaining_value = order_value
            remaining_items = total_items
            
            for item_seq in range(total_items):
                product = random.choice(self.products_data)
                
                if product['sku_code'] not in self.valid_sku_codes:
                    raise ValueError(f"SKU code not found: {product['sku_code']}")
                
                line_item_id = f"LI-{order['order_id']}-{str(item_seq+1).zfill(3)}"
                
                # Quantity logic
                if order['order_priority'] == 'BULK':
                    quantity = random.randint(3, 15)
                elif product['category'] == 'MATTRESS':
                    quantity = 1
                elif product['category'] in ['PILLOW', 'BEDDING']:
                    quantity = random.randint(1, 4)
                else:
                    quantity = random.randint(1, 2)
                
                # Price calculation
                if remaining_items == 1:
                    line_total = remaining_value
                    unit_price = line_total / quantity if quantity > 0 else 0
                else:
                    target_value = remaining_value / remaining_items
                    unit_price = product['price_inr'] * random.uniform(0.95, 1.05)
                    line_total = unit_price * quantity
                    
                    if line_total > remaining_value * 0.8:
                        line_total = remaining_value * random.uniform(0.3, 0.7)
                        unit_price = line_total / quantity if quantity > 0 else 0
                
                manufacturing_facility = 'FAC-HOS-MFG'
                if manufacturing_facility not in self.valid_facility_ids:
                    raise ValueError(f"Manufacturing facility not found: {manufacturing_facility}")
                
                estimated_manufacturing = pd.to_datetime(order['order_date']) + timedelta(days=random.randint(1, 3))
                
                # Quality status
                if order['otif_status'] == 'ON_TIME_IN_FULL':
                    qc_status = 'PASSED'
                    quantity_delivered = quantity
                    actual_manufacturing = estimated_manufacturing
                elif order['otif_status'] == 'INCOMPLETE':
                    qc_status = random.choice(['PASSED', 'REWORK'])
                    quantity_delivered = int(quantity * random.uniform(0.5, 0.9))
                    actual_manufacturing = estimated_manufacturing + timedelta(days=random.randint(0, 2))
                else:  # LATE
                    qc_status = 'PASSED'
                    quantity_delivered = quantity
                    actual_manufacturing = estimated_manufacturing + timedelta(days=random.randint(1, 3))
                
                line_item = {
                    'line_item_id': line_item_id,
                    'order_id': order['order_id'],
                    'sku_code': product['sku_code'],
                    'quantity_ordered': quantity,
                    'quantity_confirmed': quantity,
                    'quantity_dispatched': quantity_delivered,
                    'quantity_delivered': quantity_delivered,
                    'unit_price': round(unit_price, 2),
                    'line_total': round(line_total, 2),
                    'customization_details': json.dumps({'color': 'custom'}) if product['is_customizable'] and random.random() < 0.15 else None,
                    'estimated_manufacturing_date': estimated_manufacturing.date(),
                    'actual_manufacturing_date': actual_manufacturing.date(),
                    'manufacturing_facility_id': manufacturing_facility,
                    'quality_check_status': qc_status,
                    'quality_check_date': actual_manufacturing.date() + timedelta(days=1),
                    'inventory_allocation_time': pd.to_datetime(order['order_date']) + timedelta(hours=random.randint(1, 12)),
                    'line_item_status': order['delivery_status'],
                    'dispatch_facility_id': manufacturing_facility
                }
                
                self.order_line_items_data.append(line_item)
                self.global_line_item_counter += 1
                
                remaining_value -= line_total
                remaining_items -= 1
                
                if remaining_value < 0:
                    remaining_value = 0
        
        print(f"Generated {len(self.order_line_items_data)} order line items")

    def validate_order_line_items(self):
        """Validate order line items"""
        print("Validating order line items...")
        
        invalid_orders = []
        invalid_skus = []
        invalid_facilities = []
        
        for line_item in self.order_line_items_data:
            if line_item['order_id'] not in self.valid_order_ids:
                invalid_orders.append(line_item['order_id'])
            
            if line_item['sku_code'] not in self.valid_sku_codes:
                invalid_skus.append(line_item['sku_code'])
            
            if line_item['manufacturing_facility_id'] not in self.valid_facility_ids:
                invalid_facilities.append(line_item['manufacturing_facility_id'])
        
        if invalid_orders:
            raise ValueError(f"Line items reference invalid order_ids: {set(invalid_orders)}")
        
        if invalid_skus:
            raise ValueError(f"Line items reference invalid sku_codes: {set(invalid_skus)}")
        
        if invalid_facilities:
            raise ValueError(f"Line items reference invalid facility_ids: {set(invalid_facilities)}")
        
        print(f"Order line items validated. All foreign keys exist.")

    def generate_purchase_orders(self):
        """Generate purchase orders with unique IDs"""
        print("Generating 45 purchase orders...")
        
        current_date = self.start_date
        po_counter = 1
        
        while current_date <= self.end_date and len(self.purchase_orders_data) < 45:
            if random.random() < 0.5:
                supplier = random.choice(self.suppliers_data)
                
                if supplier['supplier_id'] not in self.valid_supplier_ids:
                    raise ValueError(f"Invalid supplier_id: {supplier['supplier_id']}")
                
                # Generate unique PO ID
                po_id = f"PO-{self.session_id[:4]}-{current_date.strftime('%Y%m%d')}-{str(po_counter).zfill(3)}"
                
                # Ensure no collision
                while po_id in self.used_po_ids:
                    po_counter += 1
                    po_id = f"PO-{self.session_id[:4]}-{current_date.strftime('%Y%m%d')}-{str(po_counter).zfill(3)}"
                
                self.used_po_ids.add(po_id)
                
                if supplier['supplier_type'] == 'WOOD':
                    po_value = random.uniform(100000, 300000)
                elif supplier['supplier_type'] == 'FOAM':
                    po_value = random.uniform(80000, 200000)
                else:
                    po_value = random.uniform(30000, 120000)
                
                expected_delivery = current_date + timedelta(days=supplier['standard_lead_time_days'])
                
                if expected_delivery.date() < self.end_date.date():
                    po_status = random.choice(['RECEIVED', 'CLOSED'])
                    actual_delivery = expected_delivery + timedelta(days=random.randint(-2, 5))
                else:
                    po_status = 'CONFIRMED'
                    actual_delivery = None
                
                po = {
                    'po_id': po_id,
                    'supplier_id': supplier['supplier_id'],
                    'po_date': current_date.date(),
                    'expected_delivery_date': expected_delivery.date(),
                    'actual_delivery_date': actual_delivery.date() if actual_delivery else None,
                    'total_po_value': round(po_value, 2),
                    'po_status': po_status,
                    'materials_ordered': json.dumps([supplier['supplier_type'].lower()]),
                    'payment_terms': supplier['payment_terms_days'],
                    'quality_rating': supplier['quality_rating_5'] + random.uniform(-0.2, 0.2)
                }
                
                self.purchase_orders_data.append(po)
                po_counter += 1
            
            current_date += timedelta(days=1)
        
        print(f"Generated {len(self.purchase_orders_data)} purchase orders")

    def validate_purchase_orders(self):
        """Validate purchase orders"""
        print("Validating purchase orders...")
        
        invalid_suppliers = []
        for po in self.purchase_orders_data:
            if po['supplier_id'] not in self.valid_supplier_ids:
                invalid_suppliers.append(po['supplier_id'])
        
        if invalid_suppliers:
            raise ValueError(f"Purchase orders reference invalid supplier_ids: {set(invalid_suppliers)}")
        
        print(f"Purchase orders validated.")

    def generate_production_batches(self):
        """Generate production batches with unique IDs"""
        print("Generating 90 production batches...")
        
        current_date = self.start_date
        batch_counter = 1
        
        while current_date <= self.end_date:
            valid_products = [p for p in self.products_data if p['category'] in ['MATTRESS', 'BED', 'SOFA', 'CHAIR', 'STORAGE']]
            product = random.choice(valid_products)
            
            if product['sku_code'] not in self.valid_sku_codes:
                raise ValueError(f"Invalid sku_code: {product['sku_code']}")
            
            facility_id = 'FAC-HOS-MFG'
            if facility_id not in self.valid_facility_ids:
                raise ValueError(f"Invalid facility_id: {facility_id}")
            
            # Generate unique batch ID
            batch_id = f"BATCH-{self.session_id[:4]}-{current_date.strftime('%Y%m%d')}-{str(batch_counter).zfill(3)}"
            
            while batch_id in self.used_batch_ids:
                batch_counter += 1
                batch_id = f"BATCH-{self.session_id[:4]}-{current_date.strftime('%Y%m%d')}-{str(batch_counter).zfill(3)}"
            
            self.used_batch_ids.add(batch_id)
            
            if product['category'] == 'MATTRESS':
                planned_qty = random.randint(20, 80)
            elif product['category'] in ['BED', 'SOFA']:
                planned_qty = random.randint(10, 40)
            else:
                planned_qty = random.randint(15, 60)
            
            efficiency = random.uniform(85, 98)
            actual_qty = int(planned_qty * efficiency / 100)
            
            batch = {
                'batch_id': batch_id,
                'sku_code': product['sku_code'],
                'facility_id': facility_id,
                'production_date': current_date.date(),
                'production_start_time': time(hour=random.randint(8, 10)),
                'production_end_time': time(hour=random.randint(16, 20)),
                'planned_quantity': planned_qty,
                'actual_quantity_produced': actual_qty,
                'efficiency_percentage': round(efficiency, 2),
                'quality_passed': int(actual_qty * random.uniform(0.95, 1.0)),
                'raw_materials_consumed': json.dumps({mat: random.randint(50, 200) for mat in json.loads(product['raw_materials_list'])}),
                'production_cost_per_unit': product['cost_inr'] * random.uniform(0.8, 1.0)
            }
            
            self.production_batches_data.append(batch)
            batch_counter += 1
            current_date += timedelta(days=1)
        
        print(f"Generated {len(self.production_batches_data)} production batches")

    def validate_production_batches(self):
        """Validate production batches"""
        print("Validating production batches...")
        
        invalid_skus = []
        invalid_facilities = []
        
        for batch in self.production_batches_data:
            if batch['sku_code'] not in self.valid_sku_codes:
                invalid_skus.append(batch['sku_code'])
            
            if batch['facility_id'] not in self.valid_facility_ids:
                invalid_facilities.append(batch['facility_id'])
        
        if invalid_skus:
            raise ValueError(f"Production batches reference invalid sku_codes: {set(invalid_skus)}")
        
        if invalid_facilities:
            raise ValueError(f"Production batches reference invalid facility_ids: {set(invalid_facilities)}")
        
        print(f"Production batches validated.")

    def generate_inventory_movements(self):
        """Generate inventory movements with unique IDs"""
        print("Generating inventory movements...")
        
        # Production IN movements
        for batch in self.production_batches_data:
            if batch['sku_code'] not in self.valid_sku_codes:
                raise ValueError(f"Invalid sku_code: {batch['sku_code']}")
            
            if batch['facility_id'] not in self.valid_facility_ids:
                raise ValueError(f"Invalid facility_id: {batch['facility_id']}")
            
            movement_id = self.generate_unique_movement_id('PRODUCTION_IN')
            
            movement = {
                'movement_id': movement_id,
                'sku_code': batch['sku_code'],
                'facility_id': batch['facility_id'],
                'movement_date': batch['production_date'],
                'movement_time': batch['production_end_time'],
                'movement_type': 'PRODUCTION_IN',
                'quantity_change': batch['actual_quantity_produced'],
                'previous_stock': random.randint(50, 300),
                'new_stock': random.randint(100, 400),
                'reference_id': batch['batch_id'],
                'batch_number': batch['batch_id'],
                'expiry_date': None,
                'cost_per_unit': batch['production_cost_per_unit'],
                'movement_reason': f"Production completed - {batch['batch_id']}"
            }
            self.inventory_movements_data.append(movement)
        
        # Sales OUT movements
        for line_item in self.order_line_items_data:
            if line_item['quantity_dispatched'] > 0:
                if line_item['sku_code'] not in self.valid_sku_codes:
                    raise ValueError(f"Invalid sku_code: {line_item['sku_code']}")
                
                if line_item['dispatch_facility_id'] not in self.valid_facility_ids:
                    raise ValueError(f"Invalid facility_id: {line_item['dispatch_facility_id']}")
                
                if line_item['order_id'] not in self.valid_order_ids:
                    raise ValueError(f"Invalid order_id: {line_item['order_id']}")
                
                movement_id = self.generate_unique_movement_id('SALE_OUT')
                
                movement = {
                    'movement_id': movement_id,
                    'sku_code': line_item['sku_code'],
                    'facility_id': line_item['dispatch_facility_id'],
                    'movement_date': line_item['actual_manufacturing_date'],
                    'movement_time': time(hour=random.randint(14, 18)),
                    'movement_type': 'SALE_OUT',
                    'quantity_change': -line_item['quantity_dispatched'],
                    'previous_stock': random.randint(100, 500),
                    'new_stock': random.randint(50, 450),
                    'reference_id': line_item['order_id'],
                    'batch_number': None,
                    'expiry_date': None,
                    'cost_per_unit': line_item['unit_price'] * 0.6,
                    'movement_reason': f"Order dispatch - {line_item['order_id']}"
                }
                self.inventory_movements_data.append(movement)
        
        # Transfer movements
        for i in range(500):
            sku = random.choice(self.products_data)
            from_facility = random.choice(['FAC-HOS-MFG', 'FAC-HOS-DC'])
            to_facility = random.choice(['FAC-BAN-WH', 'FAC-MUM-WH', 'FAC-DEL-WH'])
            transfer_date = fake.date_between(start_date=self.start_date.date(), end_date=self.end_date.date())
            quantity = random.randint(5, 50)
            
            if from_facility not in self.valid_facility_ids:
                raise ValueError(f"Invalid from_facility: {from_facility}")
            if to_facility not in self.valid_facility_ids:
                raise ValueError(f"Invalid to_facility: {to_facility}")
            if sku['sku_code'] not in self.valid_sku_codes:
                raise ValueError(f"Invalid sku_code: {sku['sku_code']}")
            
            # OUT movement
            movement_out_id = self.generate_unique_movement_id('TRANSFER_OUT')
            movement_out = {
                'movement_id': movement_out_id,
                'sku_code': sku['sku_code'],
                'facility_id': from_facility,
                'movement_date': transfer_date,
                'movement_time': time(hour=random.randint(10, 14)),
                'movement_type': 'TRANSFER_OUT',
                'quantity_change': -quantity,
                'previous_stock': random.randint(200, 800),
                'new_stock': random.randint(150, 750),
                'reference_id': f"TRANSFER-{self.session_id[:4]}-{i+1:04d}",
                'batch_number': None,
                'expiry_date': None,
                'cost_per_unit': sku['cost_inr'],
                'movement_reason': f"Transfer to {to_facility}"
            }
            self.inventory_movements_data.append(movement_out)
            
            # IN movement
            movement_in_id = self.generate_unique_movement_id('TRANSFER_IN')
            movement_in = {
                'movement_id': movement_in_id,
                'sku_code': sku['sku_code'],
                'facility_id': to_facility,
                'movement_date': transfer_date + timedelta(days=1),
                'movement_time': time(hour=random.randint(9, 12)),
                'movement_type': 'TRANSFER_IN',
                'quantity_change': quantity,
                'previous_stock': random.randint(50, 300),
                'new_stock': random.randint(100, 350),
                'reference_id': f"TRANSFER-{self.session_id[:4]}-{i+1:04d}",
                'batch_number': None,
                'expiry_date': None,
                'cost_per_unit': sku['cost_inr'],
                'movement_reason': f"Transfer from {from_facility}"
            }
            self.inventory_movements_data.append(movement_in)
        
        print(f"Generated {len(self.inventory_movements_data)} inventory movements")

    def validate_inventory_movements(self):
        """Validate inventory movements"""
        print("Validating inventory movements...")
        
        invalid_skus = []
        invalid_facilities = []
        invalid_orders = []
        
        for movement in self.inventory_movements_data:
            if movement['sku_code'] not in self.valid_sku_codes:
                invalid_skus.append(movement['sku_code'])
            
            if movement['facility_id'] not in self.valid_facility_ids:
                invalid_facilities.append(movement['facility_id'])
            
            if movement['movement_type'] == 'SALE_OUT' and movement['reference_id'] not in self.valid_order_ids:
                invalid_orders.append(movement['reference_id'])
        
        if invalid_skus:
            raise ValueError(f"Inventory movements reference invalid sku_codes: {set(invalid_skus)}")
        
        if invalid_facilities:
            raise ValueError(f"Inventory movements reference invalid facility_ids: {set(invalid_facilities)}")
        
        if invalid_orders:
            raise ValueError(f"Inventory movements reference invalid order_ids: {set(invalid_orders)}")
        
        print(f"Inventory movements validated.")

    def generate_logistics_shipments(self):
        """Generate logistics shipments with unique IDs"""
        print("Generating logistics shipments...")
        
        carriers = ['BLUEDART', 'DELHIVERY', 'ECOM_EXPRESS', 'DTDC', 'XPRESSBEES']
        
        for order in self.orders_data:
            if order['order_id'] not in self.valid_order_ids:
                raise ValueError(f"Invalid order_id: {order['order_id']}")
            
            carrier = random.choice(carriers)
            
            # Generate unique shipment ID
            shipment_id = f"SHIP-{carrier[:3]}-{self.session_id[:4]}-{len(self.logistics_shipments_data)+1:06d}"
            
            while shipment_id in self.used_shipment_ids:
                shipment_id = f"SHIP-{carrier[:3]}-{self.session_id[:4]}-{str(uuid.uuid4())[:6]}"
            
            self.used_shipment_ids.add(shipment_id)
            
            order_lines = [li for li in self.order_line_items_data if li['order_id'] == order['order_id']]
            total_weight = sum([random.uniform(5, 100) for _ in order_lines])
            total_volume = total_weight * random.uniform(1000, 3000)
            
            distance = random.randint(100, 1500)
            transportation_cost = (total_weight * 20) + (distance * 0.3) + random.uniform(200, 800)
            
            num_attempts = 1 if order['delivery_status'] == 'DELIVERED' else random.randint(1, 3)
            delivery_attempts = []
            
            attempt_date = pd.to_datetime(order['actual_dispatch_date'])
            for attempt in range(num_attempts):
                attempt_date += timedelta(days=attempt)
                delivery_attempts.append({
                    'attempt_number': attempt + 1,
                    'attempt_date': attempt_date.strftime('%Y-%m-%d'),
                    'attempt_time': f"{random.randint(9, 18):02d}:{random.randint(0, 59):02d}",
                    'status': 'DELIVERED' if (attempt == num_attempts - 1 and order['delivery_status'] == 'DELIVERED') else 'FAILED',
                    'reason': None if order['delivery_status'] == 'DELIVERED' else random.choice(['CUSTOMER_NOT_AVAILABLE', 'ADDRESS_ISSUE'])
                })
            
            dispatch_facility = 'FAC-HOS-MFG'
            if dispatch_facility not in self.valid_facility_ids:
                raise ValueError(f"Invalid dispatch_facility_id: {dispatch_facility}")
            
            shipment = {
                'shipment_id': shipment_id,
                'order_id': order['order_id'],
                'carrier_name': carrier,
                'tracking_number': f"{carrier[:3]}{random.randint(100000000, 999999999)}",
                'dispatch_facility_id': dispatch_facility,
                'dispatch_date': order['actual_dispatch_date'],
                'dispatch_time': f"{random.randint(8, 17):02d}:{random.randint(0, 59):02d}",
                'delivery_address_verified': order['delivery_address_full'],
                'delivery_pincode': order['delivery_pincode'],
                'estimated_delivery_date': order['estimated_delivery_date'],
                'attempted_delivery_dates': json.dumps(delivery_attempts),
                'successful_delivery_date': order['actual_delivery_date'] if order['delivery_status'] == 'DELIVERED' else None,
                'successful_delivery_time': f"{random.randint(9, 18):02d}:{random.randint(0, 59):02d}" if order['delivery_status'] == 'DELIVERED' else None,
                'delivery_person_name': fake.name() if order['delivery_status'] == 'DELIVERED' else None,
                'delivery_otp': str(random.randint(100000, 999999)) if order['delivery_status'] == 'DELIVERED' else None,
                'customer_signature_received': order['delivery_status'] == 'DELIVERED',
                'delivery_photos': json.dumps([f"photo_{i}.jpg" for i in range(random.randint(1, 3))]) if order['delivery_status'] == 'DELIVERED' else None,
                'total_weight_kg': round(total_weight, 2),
                'total_volume_cubic_cm': round(total_volume, 2),
                'transportation_cost': round(transportation_cost, 2),
                'distance_km': distance,
                'delivery_rating_by_customer': order['customer_satisfaction_rating'],
                'delivery_issues': random.choice(['TRAFFIC_DELAY', 'VEHICLE_BREAKDOWN', 'WEATHER']) if order['delay_days'] > 2 else None,
                'return_initiated': random.random() < 0.05
            }
            
            self.logistics_shipments_data.append(shipment)
        
        print(f"Generated {len(self.logistics_shipments_data)} logistics shipments")

    def validate_logistics_shipments(self):
        """Validate logistics shipments"""
        print("Validating logistics shipments...")
        
        invalid_orders = []
        invalid_facilities = []
        
        for shipment in self.logistics_shipments_data:
            if shipment['order_id'] not in self.valid_order_ids:
                invalid_orders.append(shipment['order_id'])
            
            if shipment['dispatch_facility_id'] not in self.valid_facility_ids:
                invalid_facilities.append(shipment['dispatch_facility_id'])
        
        if invalid_orders:
            raise ValueError(f"Logistics shipments reference invalid order_ids: {set(invalid_orders)}")
        
        if invalid_facilities:
            raise ValueError(f"Logistics shipments reference invalid facility_ids: {set(invalid_facilities)}")
        
        print(f"Logistics shipments validated.")

    def generate_supply_chain_events(self):
        """Generate supply chain events with unique IDs"""
        print("Generating supply chain events...")
        
        for order in self.orders_data:
            if order['order_id'] not in self.valid_order_ids:
                raise ValueError(f"Invalid order_id: {order['order_id']}")
            
            order_date = pd.to_datetime(order['order_date'])
            current_time = order_date
            
            # Order received event
            event_id = self.generate_unique_event_id()
            self.supply_chain_events_data.append({
                'event_id': event_id,
                'related_order_id': order['order_id'],
                'related_sku_code': None,
                'facility_id': None,
                'event_type': 'ORDER_RECEIVED',
                'event_timestamp': current_time,
                'expected_completion_time': current_time + timedelta(minutes=30),
                'actual_completion_time': current_time + timedelta(minutes=random.randint(15, 45)),
                'duration_minutes': random.randint(15, 45),
                'delay_minutes': max(0, random.randint(-15, 15)),
                'delay_category': 'NO_DELAY',
                'delay_root_cause': None,
                'responsible_team': 'SALES',
                'resolution_action': 'Order processed successfully',
                'impact_on_customer': 'NONE',
                'cost_of_delay': 0
            })
            current_time += timedelta(hours=1)
            
            # Events for each line item
            order_lines = [li for li in self.order_line_items_data if li['order_id'] == order['order_id']]
            
            for line_item in order_lines:
                if line_item['sku_code'] not in self.valid_sku_codes:
                    raise ValueError(f"Invalid sku_code: {line_item['sku_code']}")
                
                facility_id = 'FAC-HOS-MFG'
                if facility_id not in self.valid_facility_ids:
                    raise ValueError(f"Invalid facility_id: {facility_id}")
                
                # Inventory allocation
                delay = random.randint(0, 180) if random.random() < 0.1 else 0
                event_id = self.generate_unique_event_id()
                self.supply_chain_events_data.append({
                    'event_id': event_id,
                    'related_order_id': order['order_id'],
                    'related_sku_code': line_item['sku_code'],
                    'facility_id': facility_id,
                    'event_type': 'INVENTORY_ALLOCATED',
                    'event_timestamp': current_time,
                    'expected_completion_time': current_time + timedelta(hours=2),
                    'actual_completion_time': current_time + timedelta(hours=2, minutes=delay),
                    'duration_minutes': 120 + delay,
                    'delay_minutes': delay,
                    'delay_category': 'INVENTORY_SHORTAGE' if delay > 0 else 'NO_DELAY',
                    'delay_root_cause': 'Stock shortage' if delay > 0 else None,
                    'responsible_team': 'INVENTORY',
                    'resolution_action': 'Inventory allocated',
                    'impact_on_customer': 'NONE' if delay == 0 else 'MINOR',
                    'cost_of_delay': delay * 0.5
                })
                
                # Production events
                prod_delay = random.randint(0, 720) if random.random() < 0.15 else 0
                prod_time = pd.to_datetime(line_item['actual_manufacturing_date'])
                
                event_id = self.generate_unique_event_id()
                self.supply_chain_events_data.append({
                    'event_id': event_id,
                    'related_order_id': order['order_id'],
                    'related_sku_code': line_item['sku_code'],
                    'facility_id': facility_id,
                    'event_type': 'PRODUCTION_COMPLETED',
                    'event_timestamp': prod_time + timedelta(minutes=prod_delay),
                    'expected_completion_time': prod_time,
                    'actual_completion_time': prod_time + timedelta(minutes=prod_delay),
                    'duration_minutes': prod_delay,
                    'delay_minutes': prod_delay,
                    'delay_category': random.choice(['SUPPLIER_DELAY', 'EQUIPMENT_ISSUE', 'LABOR_SHORTAGE']) if prod_delay > 0 else 'NO_DELAY',
                    'delay_root_cause': 'Production delays' if prod_delay > 0 else None,
                    'responsible_team': 'PRODUCTION',
                    'resolution_action': 'Production completed',
                    'impact_on_customer': 'NONE' if prod_delay <= 240 else 'MODERATE',
                    'cost_of_delay': prod_delay * 1.2
                })
                
                # Quality check
                qc_delay = random.randint(60, 480) if line_item['quality_check_status'] == 'REWORK' else 0
                qc_time = pd.to_datetime(line_item['quality_check_date'])
                
                event_id = self.generate_unique_event_id()
                self.supply_chain_events_data.append({
                    'event_id': event_id,
                    'related_order_id': order['order_id'],
                    'related_sku_code': line_item['sku_code'],
                    'facility_id': facility_id,
                    'event_type': 'QC_COMPLETED',
                    'event_timestamp': qc_time + timedelta(minutes=qc_delay),
                    'expected_completion_time': qc_time,
                    'actual_completion_time': qc_time + timedelta(minutes=qc_delay),
                    'duration_minutes': 60 + qc_delay,
                    'delay_minutes': qc_delay,
                    'delay_category': 'QUALITY_ISSUE' if qc_delay > 0 else 'NO_DELAY',
                    'delay_root_cause': 'Quality rework required' if qc_delay > 0 else None,
                    'responsible_team': 'QC',
                    'resolution_action': 'Quality approved',
                    'impact_on_customer': 'NONE' if qc_delay == 0 else 'MODERATE',
                    'cost_of_delay': qc_delay * 0.8
                })
            
            # Dispatch event
            dispatch_delay = order['delay_days'] * 60 if order['delay_days'] > 0 else 0
            dispatch_time = pd.to_datetime(order['actual_dispatch_date'])
            dispatch_facility = 'FAC-HOS-MFG'
            
            if dispatch_facility not in self.valid_facility_ids:
                raise ValueError(f"Invalid dispatch facility: {dispatch_facility}")
            
            event_id = self.generate_unique_event_id()
            self.supply_chain_events_data.append({
                'event_id': event_id,
                'related_order_id': order['order_id'],
                'related_sku_code': None,
                'facility_id': dispatch_facility,
                'event_type': 'DISPATCHED',
                'event_timestamp': dispatch_time,
                'expected_completion_time': pd.to_datetime(order['estimated_dispatch_date']),
                'actual_completion_time': dispatch_time,
                'duration_minutes': 120,
                'delay_minutes': dispatch_delay,
                'delay_category': random.choice(['LOGISTICS_ISSUE', 'PACKAGING_DELAY']) if dispatch_delay > 0 else 'NO_DELAY',
                'delay_root_cause': 'Dispatch coordination' if dispatch_delay > 0 else None,
                'responsible_team': 'LOGISTICS',
                'resolution_action': 'Order dispatched',
                'impact_on_customer': 'NONE' if dispatch_delay <= 120 else 'MAJOR',
                'cost_of_delay': dispatch_delay * 1.5
            })
            
            # Delivery event
            delivery_delay = order['delay_days'] * 1440 if order['delay_days'] > 0 else 0
            delivery_time = pd.to_datetime(order['actual_delivery_date'])
            
            event_id = self.generate_unique_event_id()
            self.supply_chain_events_data.append({
                'event_id': event_id,
                'related_order_id': order['order_id'],
                'related_sku_code': None,
                'facility_id': None,
                'event_type': 'DELIVERED',
                'event_timestamp': delivery_time,
                'expected_completion_time': pd.to_datetime(order['promised_delivery_date']),
                'actual_completion_time': delivery_time,
                'duration_minutes': 30,
                'delay_minutes': delivery_delay,
                'delay_category': random.choice(['TRAFFIC_DELAY', 'CUSTOMER_UNAVAILABLE']) if delivery_delay > 0 else 'NO_DELAY',
                'delay_root_cause': 'Last mile delivery issues' if delivery_delay > 0 else None,
                'responsible_team': 'LOGISTICS',
                'resolution_action': 'Successfully delivered',
                'impact_on_customer': 'NONE' if delivery_delay == 0 else 'MAJOR',
                'cost_of_delay': delivery_delay * 2.0
            })
        
        print(f"Generated {len(self.supply_chain_events_data)} supply chain events")

    def validate_supply_chain_events(self):
        """Validate supply chain events"""
        print("Validating supply chain events...")
        
        invalid_orders = []
        invalid_skus = []
        invalid_facilities = []
        
        for event in self.supply_chain_events_data:
            if event['related_order_id'] and event['related_order_id'] not in self.valid_order_ids:
                invalid_orders.append(event['related_order_id'])
            
            if event['related_sku_code'] and event['related_sku_code'] not in self.valid_sku_codes:
                invalid_skus.append(event['related_sku_code'])
            
            if event['facility_id'] and event['facility_id'] not in self.valid_facility_ids:
                invalid_facilities.append(event['facility_id'])
        
        if invalid_orders:
            raise ValueError(f"Supply chain events reference invalid order_ids: {set(invalid_orders)}")
        
        if invalid_skus:
            raise ValueError(f"Supply chain events reference invalid sku_codes: {set(invalid_skus)}")
        
        if invalid_facilities:
            raise ValueError(f"Supply chain events reference invalid facility_ids: {set(invalid_facilities)}")
        
        print(f"Supply chain events validated.")

    def generate_demand_forecasts(self):
        """Generate demand forecasts with FIXED unique ID generation"""
        print("Generating 30 demand forecasts...")
        
        # Generate monthly forecasts for each product
        for product in self.products_data:
            sku_code = product['sku_code']
            
            if sku_code not in self.valid_sku_codes:
                raise ValueError(f"Invalid sku_code: {sku_code}")
            
            facility_id = 'FAC-HOS-DC'
            if facility_id not in self.valid_facility_ids:
                raise ValueError(f"Invalid facility_id: {facility_id}")
            
            # Get actual demand from line items
            sku_line_items = [li for li in self.order_line_items_data if li['sku_code'] == sku_code]
            
            for month in [1, 2, 3]:  # Jan, Feb, Mar 2024
                forecast_date = datetime(2023, 12, 1) + timedelta(days=(month-1)*30)
                forecast_for_date = datetime(2024, month, 1)
                
                # Generate UNIQUE forecast ID using the fixed method
                forecast_id = self.generate_unique_forecast_id(sku_code, month, 2024)
                
                # Calculate actual demand for this month
                month_line_items = [
                    li for li in sku_line_items 
                    if pd.to_datetime(li['actual_manufacturing_date']).month == month
                ]
                actual_demand = sum([li['quantity_ordered'] for li in month_line_items])
                
                # Generate forecast
                method = random.choice(['ARIMA', 'LINEAR_REGRESSION', 'SEASONAL_NAIVE', 'EXPONENTIAL_SMOOTHING'])
                base_forecast = max(1, actual_demand + random.randint(-5, 5))
                
                # Seasonal adjustment
                try:
                    seasonal_factors = json.loads(product['seasonal_demand_factor'])
                    seasonal_factor = float(seasonal_factors.get(str(month), 1.0))
                except:
                    seasonal_factor = random.uniform(0.9, 1.1)
                
                # Promotional adjustment
                promotional_adjustment = 0
                if random.random() < 0.15:
                    promotional_adjustment = int(base_forecast * random.uniform(0.1, 0.3))
                
                final_forecast = max(0, int(base_forecast * seasonal_factor) + promotional_adjustment)
                
                # Calculate error
                forecast_error = final_forecast - actual_demand
                forecast_error_percentage = (abs(forecast_error) / max(actual_demand, 1)) * 100
                
                # Accuracy rating
                if forecast_error_percentage <= 15:
                    accuracy_rating = 'EXCELLENT'
                elif forecast_error_percentage <= 35:
                    accuracy_rating = 'GOOD'
                elif forecast_error_percentage <= 60:
                    accuracy_rating = 'AVERAGE'
                else:
                    accuracy_rating = 'POOR'
                
                # External factors
                external_factors = []
                if month in [1, 3]:
                    external_factors.append('SEASONAL_DEMAND')
                if random.random() < 0.1:
                    external_factors.append(random.choice(['COMPETITOR_LAUNCH', 'SUPPLY_SHORTAGE']))
                
                forecast = {
                    'forecast_id': forecast_id,
                    'sku_code': sku_code,
                    'facility_id': facility_id,
                    'forecast_date': forecast_date.date(),
                    'forecast_for_date': forecast_for_date.date(),
                    'forecast_horizon_days': 30,
                    'forecasting_method': method,
                    'base_forecast': base_forecast,
                    'promotional_adjustment': promotional_adjustment,
                    'seasonal_adjustment': round(seasonal_factor, 2),
                    'external_factors': json.dumps(external_factors) if external_factors else None,
                    'final_forecast': final_forecast,
                    'actual_demand': actual_demand,
                    'forecast_error': forecast_error,
                    'forecast_error_percentage': round(forecast_error_percentage, 2),
                    'forecast_accuracy_rating': accuracy_rating
                }
                
                self.demand_forecasts_data.append(forecast)
        
        print(f"Generated {len(self.demand_forecasts_data)} demand forecasts")

    def validate_demand_forecasts(self):
        """Validate demand forecasts"""
        print("Validating demand forecasts...")
        
        invalid_skus = []
        invalid_facilities = []
        duplicate_forecast_ids = []
        
        forecast_ids = [f['forecast_id'] for f in self.demand_forecasts_data]
        
        for forecast in self.demand_forecasts_data:
            if forecast['sku_code'] not in self.valid_sku_codes:
                invalid_skus.append(forecast['sku_code'])
            
            if forecast['facility_id'] not in self.valid_facility_ids:
                invalid_facilities.append(forecast['facility_id'])
        
        # Check for duplicate forecast IDs
        duplicate_forecast_ids = set([fid for fid in forecast_ids if forecast_ids.count(fid) > 1])
        
        if invalid_skus:
            raise ValueError(f"Demand forecasts reference invalid sku_codes: {set(invalid_skus)}")
        
        if invalid_facilities:
            raise ValueError(f"Demand forecasts reference invalid facility_ids: {set(invalid_facilities)}")
        
        if duplicate_forecast_ids:
            raise ValueError(f"Duplicate forecast IDs found: {duplicate_forecast_ids}")
        
        print(f"Demand forecasts validated. All unique IDs confirmed.")

    def perform_comprehensive_validation(self):
        """Perform final comprehensive validation"""
        print("Performing comprehensive validation...")
        
        validation_results = []
        
        # Check all relationships
        dependent_tables_with_orders = [
            ('order_line_items', self.order_line_items_data, 'order_id'),
            ('logistics_shipments', self.logistics_shipments_data, 'order_id'),
            ('supply_chain_events', self.supply_chain_events_data, 'related_order_id')
        ]
        
        for table_name, data, order_field in dependent_tables_with_orders:
            invalid_orders = set()
            for record in data:
                if record.get(order_field) and record[order_field] not in self.valid_order_ids:
                    invalid_orders.add(record[order_field])
            
            if invalid_orders:
                validation_results.append(f"ERROR {table_name}: {len(invalid_orders)} invalid order references")
            else:
                validation_results.append(f"OK {table_name}: All order references valid")
        
        # Check SKU references
        dependent_tables_with_skus = [
            ('order_line_items', self.order_line_items_data, 'sku_code'),
            ('production_batches', self.production_batches_data, 'sku_code'),
            ('inventory_movements', self.inventory_movements_data, 'sku_code'),
            ('demand_forecasts', self.demand_forecasts_data, 'sku_code'),
            ('supply_chain_events', self.supply_chain_events_data, 'related_sku_code')
        ]
        
        for table_name, data, sku_field in dependent_tables_with_skus:
            invalid_skus = set()
            for record in data:
                if record.get(sku_field) and record[sku_field] not in self.valid_sku_codes:
                    invalid_skus.add(record[sku_field])
            
            if invalid_skus:
                validation_results.append(f"ERROR {table_name}: {len(invalid_skus)} invalid SKU references")
            else:
                validation_results.append(f"OK {table_name}: All SKU references valid")
        
        # Check for duplicate primary keys
        datasets_to_check_duplicates = [
            ('products', [p['sku_code'] for p in self.products_data]),
            ('customers', [c['customer_id'] for c in self.customers_data]),
            ('orders', [o['order_id'] for o in self.orders_data]),
            ('demand_forecasts', [f['forecast_id'] for f in self.demand_forecasts_data]),
            ('supply_chain_events', [e['event_id'] for e in self.supply_chain_events_data])
        ]
        
        for table_name, ids in datasets_to_check_duplicates:
            duplicates = set([id for id in ids if ids.count(id) > 1])
            if duplicates:
                validation_results.append(f"ERROR {table_name}: {len(duplicates)} duplicate primary keys")
            else:
                validation_results.append(f"OK {table_name}: No duplicate primary keys")
        
        # Print results
        print("Comprehensive Validation Results:")
        for result in validation_results:
            print(f"   {result}")
        
        # Check if any validation failed
        failed_validations = [r for r in validation_results if r.startswith('ERROR')]
        if failed_validations:
            raise ValueError(f"Validation failed: {len(failed_validations)} errors found")
        
        print("Comprehensive validation PASSED! Database ready for import.")

    # Helper methods
    def _get_product_dimensions(self, category, size, thickness=None):
        """Get realistic dimensions for products"""
        base_dimensions = {
            'MATTRESS': {
                'DOUBLE': '190x120', 'QUEEN': '190x150', 'KING': '190x180'
            },
            'BED': {
                'QUEEN': '200x160x90', 'KING': '210x190x90'
            },
            'SOFA': {
                'REGULAR': '180x90x80', 'LARGE': '220x100x85'
            },
            'CHAIR': {
                'STANDARD': '60x60x110'
            },
            'STORAGE': {
                'LARGE': '180x60x200', 'STANDARD': '120x40x180'
            },
            'PILLOW': {
                'STANDARD': '60x40x15'
            },
            'BEDDING': {
                'QUEEN': '220x240x5'
            }
        }
        
        base_dim = base_dimensions.get(category, {}).get(size, '100x50x30')
        
        if category == 'MATTRESS' and thickness:
            return f"{base_dim}x{thickness*2.54:.0f}"
        
        return base_dim

    def _get_materials_for_product(self, category):
        """Get material requirements for each product category"""
        materials = {
            'MATTRESS': ['foam', 'fabric', 'zipper'],
            'BED': ['wood', 'hardware', 'finish'],
            'SOFA': ['wood_frame', 'foam', 'fabric', 'springs'],
            'CHAIR': ['wood', 'fabric', 'foam', 'hardware'],
            'STORAGE': ['wood', 'hardware', 'finish'],
            'PILLOW': ['foam', 'fabric'],
            'BEDDING': ['fabric', 'filling']
        }
        return materials.get(category, ['basic_materials'])

    def _get_seasonal_factors(self, category):
        """Get seasonal demand factors by month"""
        if category == 'MATTRESS':
            return {str(i): [0.8, 0.9, 1.1, 1.2, 1.1, 0.9, 0.8, 0.9, 1.1, 1.3, 1.4, 1.2][i-1] for i in range(1, 13)}
        else:
            return {str(i): [0.9, 1.0, 1.2, 1.1, 1.0, 0.8, 0.7, 0.8, 1.0, 1.2, 1.3, 1.1][i-1] for i in range(1, 13)}

    def save_all_datasets(self):
        """Save all datasets to CSV files"""
        datasets = [
            ('products', self.products_data),
            ('customers', self.customers_data),
            ('facilities', self.facilities_data),
            ('suppliers', self.suppliers_data),
            ('orders', self.orders_data),
            ('order_line_items', self.order_line_items_data),
            ('purchase_orders', self.purchase_orders_data),
            ('production_batches', self.production_batches_data),
            ('inventory_movements', self.inventory_movements_data),
            ('logistics_shipments', self.logistics_shipments_data),
            ('supply_chain_events', self.supply_chain_events_data),
            ('demand_forecasts', self.demand_forecasts_data)
        ]
        
        total_records = 0
        total_size = 0
        
        for dataset_name, data in datasets:
            if data:
                df = pd.DataFrame(data)
                filename = f"{self.output_dir}/{dataset_name}.csv"
                df.to_csv(filename, index=False)
                
                file_size = os.path.getsize(filename) / (1024 * 1024)
                total_records += len(df)
                total_size += file_size
                
                print(f"Saved {dataset_name}: {len(df):,} records ({file_size:.2f} MB)")
        
        print(f"\nTotal: {total_records:,} records ({total_size:.1f} MB)")

    def generate_summary_report(self):
        """Generate summary report"""
        print("Generating summary report...")
        
        datasets_info = []
        dataset_files = [f for f in os.listdir(self.output_dir) if f.endswith('.csv')]
        
        for filename in dataset_files:
            filepath = f"{self.output_dir}/{filename}"
            df = pd.read_csv(filepath)
            file_size = os.path.getsize(filepath) / (1024 * 1024)
            
            datasets_info.append({
                'Dataset': filename.replace('.csv', ''),
                'Records': len(df),
                'Columns': len(df.columns),
                'Size_MB': round(file_size, 2),
                'Filename': filename
            })
        
        summary_df = pd.DataFrame(datasets_info)
        summary_df = summary_df.sort_values('Records', ascending=False)
        summary_filename = f"{self.output_dir}/dataset_summary.csv"
        summary_df.to_csv(summary_filename, index=False)
        
        summary_report = {
            'generation_date': datetime.now().isoformat(),
            'session_id': self.session_id,
            'date_range': {
                'start_date': self.start_date.isoformat(),
                'end_date': self.end_date.isoformat(),
                'total_days': (self.end_date - self.start_date).days + 1
            },
            'business_parameters': {
                'daily_orders_target': self.daily_orders,
                'otif_target': self.otif_target,
                'total_products': len(self.products_data),
                'total_customers': len(self.customers_data),
                'total_facilities': len(self.facilities_data),
                'total_suppliers': len(self.suppliers_data)
            },
            'validation_summary': {
                'total_valid_customer_ids': len(self.valid_customer_ids),
                'total_valid_order_ids': len(self.valid_order_ids),
                'total_valid_sku_codes': len(self.valid_sku_codes),
                'total_valid_facility_ids': len(self.valid_facility_ids),
                'total_valid_supplier_ids': len(self.valid_supplier_ids)
            },
            'id_collision_prevention': {
                'unique_forecast_ids': len(self.used_forecast_ids),
                'unique_event_ids': len(self.used_event_ids),
                'unique_movement_ids': len(self.used_movement_ids),
                'unique_shipment_ids': len(self.used_shipment_ids),
                'session_id_used': self.session_id
            },
            'generated_datasets': summary_df.to_dict('records'),
            'totals': {
                'total_records': summary_df['Records'].sum(),
                'total_size_mb': summary_df['Size_MB'].sum(),
                'total_datasets': len(summary_df)
            }
        }
        
        report_filename = f"{self.output_dir}/generation_report.json"
        with open(report_filename, 'w') as f:
            json.dump(summary_report, f, indent=2, default=str)
        
        print(f"\nGeneration Summary:")
        print(f"   Period: {self.start_date.date()} to {self.end_date.date()} (90 days)")
        print(f"   Session ID: {self.session_id}")
        print(f"   Business Scale: {len(self.products_data)} SKUs, {len(self.customers_data):,} customers")
        print(f"   Orders Generated: {len(self.orders_data):,} orders")
        print(f"   Line Items: {len(self.order_line_items_data):,}")
        print(f"   Total Datasets: {len(summary_df)}")
        print(f"   Total Records: {summary_df['Records'].sum():,}")
        print(f"   Total Size: {summary_df['Size_MB'].sum():.1f} MB")
        print(f"   ALL ID COLLISIONS PREVENTED")
        
        print(f"\nFiles saved in: {self.output_dir}")
        print(f"Summary report: {summary_filename}")
        print(f"Detailed report: {report_filename}")
        
        return summary_report


def main():
    """Main execution function"""
    print("Wakefit Final Supply Chain Data Generator")
    print("=" * 60)
    print("ALL ID COLLISION ISSUES RESOLVED")
    
    generator = WakefitFinalDataGenerator(output_dir='wakefit_final_data')
    
    try:
        generator.generate_all_data()
        
        print("\nSUCCESS: Complete dataset generated!")
        print("\nDatabase Import Order:")
        print("   1. products.csv")
        print("   2. customers.csv") 
        print("   3. facilities.csv")
        print("   4. suppliers.csv")
        print("   5. orders.csv")
        print("   6. order_line_items.csv")
        print("   7. All remaining files (any order)")
        
        print(f"\nKey Fixes Applied:")
        print(f"   - Unique SKU codes with numeric prefixes")
        print(f"   - Session-based IDs prevent cross-run collisions")
        print(f"   - UUID-enhanced unique ID generation")
        print(f"   - Collision detection and prevention")
        print(f"   - Comprehensive foreign key validation")
        
        print(f"\nAnalytics Ready:")
        print(f"   - Zero foreign key constraint violations")
        print(f"   - Zero duplicate primary keys")
        print(f"   - Complete referential integrity")
        print(f"   - 90-day operational dataset")
        
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())