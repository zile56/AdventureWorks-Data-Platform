from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text
import hashlib
import logging

# ------------------- Configuration -------------------
DB_CONNECTION = "postgresql+psycopg2://postgres:1004@postgres:5432/postgres"

# ------------------- Default DAG args -------------------
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# ------------------- DAG definition -------------------
with DAG(
    dag_id='silver_layer_complete',
    default_args=default_args,
    start_date=datetime(2025, 11, 6),
    schedule_interval='0 0 * * *',  # Daily at midnight
    catchup=False,
    description='Silver Layer: Complete SCD Type 2 using ALL Bronze sources'
) as dag:

    def get_db_engine():
        """Create PostgreSQL connection engine"""
        return create_engine(DB_CONNECTION)

    def calculate_hash(row, columns):
        """Calculate MD5 hash for change detection"""
        values = '|'.join([str(row[col]) if pd.notna(row[col]) else '' for col in columns])
        return hashlib.md5(values.encode()).hexdigest()

    def apply_scd2(df_bronze, table_name, business_key, hash_columns, execution_date, engine):
        """Generic SCD Type 2 implementation"""
        
        # Calculate hash for change detection
        df_bronze['record_hash'] = df_bronze.apply(
            lambda row: calculate_hash(row, hash_columns), axis=1
        )
        
        # Check if silver table exists
        table_exists = engine.dialect.has_table(engine.connect(), table_name)
        
        if not table_exists:
            # Initial load
            print(f"   🆕 Initial load - creating {table_name}")
            
            df_bronze['surrogate_key'] = range(1, len(df_bronze) + 1)
            df_bronze['effective_date'] = execution_date
            df_bronze['expiry_date'] = '9999-12-31'
            df_bronze['is_current'] = True
            df_bronze['created_date'] = datetime.now()
            df_bronze['updated_date'] = datetime.now()
            
            df_bronze.to_sql(table_name, engine, if_exists='replace', index=False)
            print(f"   ✅ Created {len(df_bronze):,} records")
            return len(df_bronze), 0, 0
            
        else:
            # SCD Type 2 - detect changes
            print(f"   🔄 SCD Type 2 - detecting changes")
            
            # Read existing dimension
            df_existing = pd.read_sql(
                f"SELECT * FROM {table_name} WHERE is_current = true",
                engine
            )
            
            # Check if record_hash column exists, if not add it
            if 'record_hash' not in df_existing.columns:
                print(f"   ⚠️  Adding missing record_hash column to existing records")
                df_existing['record_hash'] = df_existing.apply(
                    lambda row: calculate_hash(row, hash_columns), axis=1
                )
            
            # Merge to find changes
            df_merged = df_bronze.merge(
                df_existing[[business_key, 'record_hash', 'surrogate_key']],
                on=business_key,
                how='left',
                suffixes=('_new', '_old')
            )
            
            # Identify new, changed, and unchanged records
            new_records = df_merged[df_merged['surrogate_key'].isna()].copy()
            changed_records = df_merged[
                (df_merged['surrogate_key'].notna()) & 
                (df_merged['record_hash_new'] != df_merged['record_hash_old'])
            ].copy()
            
            new_count = len(new_records)
            changed_count = len(changed_records)
            
            print(f"   📊 New: {new_count:,}, Changed: {changed_count:,}")
            
            # Process changes
            if changed_count > 0:
                # Expire old records
                ids_to_expire = changed_records[business_key].tolist()
                with engine.begin() as conn:
                    placeholders = ','.join([f"'{id}'" for id in ids_to_expire])
                    conn.execute(text(f"""
                        UPDATE {table_name}
                        SET expiry_date = '{execution_date}',
                            is_current = false,
                            updated_date = '{datetime.now()}'
                        WHERE {business_key} IN ({placeholders}) AND is_current = true
                    """))
                
                # Insert new versions
                max_key = df_existing['surrogate_key'].max()
                changed_records['surrogate_key'] = range(max_key + 1, max_key + 1 + changed_count)
                changed_records['effective_date'] = execution_date
                changed_records['expiry_date'] = '9999-12-31'
                changed_records['is_current'] = True
                changed_records['created_date'] = datetime.now()
                changed_records['updated_date'] = datetime.now()
                
                # Drop merge columns
                cols_to_drop = [col for col in changed_records.columns if col.endswith('_old') or col.endswith('_new')]
                changed_records = changed_records.drop(columns=cols_to_drop)
                changed_records = changed_records.rename(columns={'record_hash_new': 'record_hash'} if 'record_hash_new' in changed_records.columns else {})
                
                changed_records.to_sql(table_name, engine, if_exists='append', index=False)
            
            # Insert new records
            if new_count > 0:
                max_key = pd.read_sql(f"SELECT MAX(surrogate_key) as max_key FROM {table_name}", engine).iloc[0, 0]
                
                new_records['surrogate_key'] = range(max_key + 1, max_key + 1 + new_count)
                new_records['effective_date'] = execution_date
                new_records['expiry_date'] = '9999-12-31'
                new_records['is_current'] = True
                new_records['created_date'] = datetime.now()
                new_records['updated_date'] = datetime.now()
                
                # Drop merge columns
                cols_to_drop = [col for col in new_records.columns if col.endswith('_old') or col.endswith('_new')]
                new_records = new_records.drop(columns=cols_to_drop)
                new_records = new_records.rename(columns={'record_hash_new': 'record_hash'} if 'record_hash_new' in new_records.columns else {})
                
                new_records.to_sql(table_name, engine, if_exists='append', index=False)
            
            return new_count, changed_count, len(df_existing)

    def create_dim_customer_complete(**context):
        """Create customer dimension with SCD Type 2 - combining ALL sources"""
        execution_date = context['ds']
        print(f"\n🔄 Processing dim_customer from ALL Bronze sources...")
        
        engine = get_db_engine()
        
        try:
            # Source 1: API Customers
            df1 = pd.read_sql("""
                SELECT 
                    CAST(customerid AS VARCHAR) as customer_id,
                    CAST(personid AS VARCHAR) as person_id,
                    CAST(storeid AS VARCHAR) as store_id,
                    CAST(territory AS VARCHAR) as territory_id,
                    accountnumber as account_number,
                    'api' as source_system
                FROM bronze_adventureworks_api_customers
            """, engine)
            
            # Source 2: Sales Customers (now has proper columns!)
            df2 = pd.read_sql("""
                SELECT 
                    CAST(customerid AS VARCHAR) as customer_id,
                    CAST(personid AS VARCHAR) as person_id,
                    CAST(storeid AS VARCHAR) as store_id,
                    CAST(territoryid AS VARCHAR) as territory_id,
                    accountnumber as account_number,
                    'sales' as source_system
                FROM bronze_sales_customer
            """, engine)
            
            # Combine all sources
            df_all = pd.concat([df1, df2], ignore_index=True)
            df_combined = df_all.drop_duplicates(subset=['customer_id'], keep='last')
            
            print(f"   📊 Combined {len(df1):,} + {len(df2):,} = {len(df_combined):,} unique customers")
            
            # Apply SCD Type 2
            hash_columns = ['person_id', 'store_id', 'territory_id', 'account_number']
            new, changed, existing = apply_scd2(
                df_combined, 
                'silver_sales_dim_customer',
                'customer_id',
                hash_columns,
                execution_date,
                engine
            )
            
            # Get final counts
            total = pd.read_sql("SELECT COUNT(*) as cnt FROM silver_sales_dim_customer", engine).iloc[0, 0]
            current = pd.read_sql("SELECT COUNT(*) as cnt FROM silver_sales_dim_customer WHERE is_current = true", engine).iloc[0, 0]
            
            print(f"   📊 Total: {total:,}, Current: {current:,}, Historical: {total-current:,}")
            
            return {'total': int(total), 'current': int(current), 'new': int(new), 'changed': int(changed)}
            
        except Exception as e:
            print(f"   ❌ FAILED: {e}")
            import traceback
            traceback.print_exc()
            raise
        finally:
            engine.dispose()

    def create_dim_product_complete(**context):
        """Create product dimension with SCD Type 2 - combining ALL sources"""
        execution_date = context['ds']
        print(f"\n🔄 Processing dim_product from ALL Bronze sources...")
        
        engine = get_db_engine()
        
        try:
            # Source 1: API Products
            df1 = pd.read_sql("""
                SELECT 
                    CAST(productid AS VARCHAR) as product_id,
                    name as product_name,
                    productnumber as product_number,
                    CAST(standardcost AS DECIMAL) as standard_cost,
                    CAST(listprice AS DECIMAL) as list_price,
                    'api' as source_system
                FROM bronze_adventureworks_api_products
            """, engine)
            
            # Source 2: Sales Products (now has proper columns!)
            df2 = pd.read_sql("""
                SELECT 
                    CAST(productid AS VARCHAR) as product_id,
                    name as product_name,
                    productnumber as product_number,
                    CAST(standardcost AS DECIMAL) as standard_cost,
                    CAST(listprice AS DECIMAL) as list_price,
                    'sales' as source_system
                FROM bronze_sales_products
            """, engine)
            
            # Combine all sources
            df_all = pd.concat([df1, df2], ignore_index=True)
            df_combined = df_all.drop_duplicates(subset=['product_id'], keep='last')
            
            print(f"   📊 Combined {len(df1):,} + {len(df2):,} = {len(df_combined):,} unique products")
            
            # Apply SCD Type 2
            hash_columns = ['product_name', 'product_number', 'standard_cost', 'list_price']
            new, changed, existing = apply_scd2(
                df_combined,
                'silver_sales_dim_product',
                'product_id',
                hash_columns,
                execution_date,
                engine
            )
            
            total = pd.read_sql("SELECT COUNT(*) as cnt FROM silver_sales_dim_product", engine).iloc[0, 0]
            current = pd.read_sql("SELECT COUNT(*) as cnt FROM silver_sales_dim_product WHERE is_current = true", engine).iloc[0, 0]
            
            print(f"   📊 Total: {total:,}, Current: {current:,}, Historical: {total-current:,}")
            
            return {'total': int(total), 'current': int(current)}
            
        except Exception as e:
            print(f"   ❌ FAILED: {e}")
            import traceback
            traceback.print_exc()
            raise
        finally:
            engine.dispose()

    def create_dim_territory_complete(**context):
        """Create territory dimension with SCD Type 2"""
        execution_date = context['ds']
        print(f"\n🔄 Processing dim_territory...")
        
        engine = get_db_engine()
        
        try:
            # Check which territory table exists and has proper columns
            try:
                df_combined = pd.read_sql("""
                    SELECT 
                        CAST(territoryid AS VARCHAR) as territory_id,
                        name as territory_name,
                        countryregioncode as country_region,
                        "group" as group_name,
                        'kaggle' as source_system
                    FROM bronze_adventureworks_kaggle_territories
                """, engine)
            except:
                # Fallback: create minimal territory dimension
                print("   ⚠️  No territory table found, creating minimal dimension")
                df_combined = pd.DataFrame({
                    'territory_id': ['1'],
                    'territory_name': ['Unknown'],
                    'country_region': ['US'],
                    'group_name': ['North America'],
                    'source_system': ['default']
                })
            
            df_combined = df_combined.drop_duplicates(subset=['territory_id'], keep='last')
            
            print(f"   📊 Loaded {len(df_combined):,} unique territories")
            
            # Apply SCD Type 2
            hash_columns = ['territory_name', 'country_region', 'group_name']
            new, changed, existing = apply_scd2(
                df_combined,
                'silver_sales_dim_territory',
                'territory_id',
                hash_columns,
                execution_date,
                engine
            )
            
            total = pd.read_sql("SELECT COUNT(*) as cnt FROM silver_sales_dim_territory", engine).iloc[0, 0]
            current = pd.read_sql("SELECT COUNT(*) as cnt FROM silver_sales_dim_territory WHERE is_current = true", engine).iloc[0, 0]
            
            print(f"   📊 Total: {total:,}, Current: {current:,}, Historical: {total-current:,}")
            
            return {'total': int(total), 'current': int(current)}
            
        finally:
            engine.dispose()

    def create_dim_product_category(**context):
        """Create product category dimension"""
        execution_date = context['ds']
        print(f"\n🔄 Processing dim_product_category...")
        
        engine = get_db_engine()
        
        try:
            df_combined = pd.read_sql("""
                SELECT 
                    CAST(productcategoryid AS VARCHAR) as product_category_id,
                    name as category_name,
                    'api' as source_system
                FROM bronze_adventureworks_api_product_categories
            """, engine)
            
            df_combined = df_combined.drop_duplicates(subset=['product_category_id'], keep='last')
            
            print(f"   📊 Loaded {len(df_combined):,} product categories")
            
            # Apply SCD Type 2
            hash_columns = ['category_name']
            new, changed, existing = apply_scd2(
                df_combined,
                'silver_sales_dim_product_category',
                'product_category_id',
                hash_columns,
                execution_date,
                engine
            )
            
            total = pd.read_sql("SELECT COUNT(*) as cnt FROM silver_sales_dim_product_category", engine).iloc[0, 0]
            current = pd.read_sql("SELECT COUNT(*) as cnt FROM silver_sales_dim_product_category WHERE is_current = true", engine).iloc[0, 0]
            
            print(f"   📊 Total: {total:,}, Current: {current:,}, Historical: {total-current:,}")
            
            return {'total': int(total), 'current': int(current)}
            
        finally:
            engine.dispose()

    def create_dim_product_subcategory(**context):
        """Create product subcategory dimension"""
        execution_date = context['ds']
        print(f"\n🔄 Processing dim_product_subcategory...")
        
        engine = get_db_engine()
        
        try:
            df_combined = pd.read_sql("""
                SELECT 
                    CAST(productsubcategoryid AS VARCHAR) as product_subcategory_id,
                    CAST(category AS VARCHAR) as product_category_id,
                    name as subcategory_name,
                    'api' as source_system
                FROM bronze_adventureworks_api_product_subcategories
            """, engine)
            
            df_combined = df_combined.drop_duplicates(subset=['product_subcategory_id'], keep='last')
            
            print(f"   📊 Loaded {len(df_combined):,} product subcategories")
            
            # Apply SCD Type 2
            hash_columns = ['product_category_id', 'subcategory_name']
            new, changed, existing = apply_scd2(
                df_combined,
                'silver_sales_dim_product_subcategory',
                'product_subcategory_id',
                hash_columns,
                execution_date,
                engine
            )
            
            total = pd.read_sql("SELECT COUNT(*) as cnt FROM silver_sales_dim_product_subcategory", engine).iloc[0, 0]
            current = pd.read_sql("SELECT COUNT(*) as cnt FROM silver_sales_dim_product_subcategory WHERE is_current = true", engine).iloc[0, 0]
            
            print(f"   📊 Total: {total:,}, Current: {current:,}, Historical: {total-current:,}")
            
            return {'total': int(total), 'current': int(current)}
            
        finally:
            engine.dispose()

    def create_dim_employee_complete(**context):
        """Create employee dimension with SCD Type 2"""
        execution_date = context['ds']
        print(f"\n🔄 Processing dim_employee...")
        
        engine = get_db_engine()
        
        try:
            # Check which employee table exists and has proper columns
            try:
                df_combined = pd.read_sql("""
                    SELECT 
                        CAST(employeeid AS VARCHAR) as employee_id,
                        nationalidnumber as person_id,
                        jobtitle as job_title,
                        hiredate as hire_date,
                        'adventureworks' as source_system
                    FROM bronze_adventureworks_employee
                """, engine)
            except:
                # Fallback: create minimal employee dimension
                print("   ⚠️  No employee table found, creating minimal dimension")
                df_combined = pd.DataFrame({
                    'employee_id': ['1'],
                    'person_id': ['Unknown'],
                    'job_title': ['Unknown'],
                    'hire_date': ['2020-01-01'],
                    'source_system': ['default']
                })
            
            df_combined = df_combined.drop_duplicates(subset=['employee_id'], keep='last')
            
            print(f"   📊 Loaded {len(df_combined):,} unique employees")
            
            # Apply SCD Type 2
            hash_columns = ['person_id', 'job_title', 'hire_date']
            new, changed, existing = apply_scd2(
                df_combined,
                'silver_hr_dim_employee',
                'employee_id',
                hash_columns,
                execution_date,
                engine
            )
            
            total = pd.read_sql("SELECT COUNT(*) as cnt FROM silver_hr_dim_employee", engine).iloc[0, 0]
            current = pd.read_sql("SELECT COUNT(*) as cnt FROM silver_hr_dim_employee WHERE is_current = true", engine).iloc[0, 0]
            
            print(f"   📊 Total: {total:,}, Current: {current:,}, Historical: {total-current:,}")
            
            return {'total': int(total), 'current': int(current)}
            
        finally:
            engine.dispose()

    def create_fact_sales_orders_complete(**context):
        """Combine ALL sales order sources"""
        execution_date = context['ds']
        print(f"\n🔄 Processing fact_sales_orders from ALL sources...")
        
        engine = get_db_engine()
        
        try:
            # Source 1: API Sales Orders (has proper column names)
            df1 = pd.read_sql("""
                SELECT 
                    CAST(salesorderid AS VARCHAR) as sales_order_id,
                    CAST(customerid AS VARCHAR) as customer_id,
                    territory as territory_id,
                    orderdate as order_date,
                    duedate as due_date,
                    shipdate as ship_date,
                    CAST(subtotal AS DECIMAL) as subtotal,
                    CAST(taxamount AS DECIMAL) as tax_amt,
                    CAST(freight AS DECIMAL) as freight,
                    CAST(totaldue AS DECIMAL) as total_due,
                    'api' as source_system
                FROM bronze_adventureworks_api_sales_orders
            """, engine)
            
            # Combine all sources
            df_combined = df1.drop_duplicates(subset=['sales_order_id'], keep='last')
            
            print(f"   📊 Combined from {len(df1):,} = {len(df_combined):,} unique orders")
            
            # Get dimension keys
            dim_customer = pd.read_sql(
                "SELECT surrogate_key as customer_key, customer_id FROM silver_sales_dim_customer WHERE is_current = true",
                engine
            )
            dim_territory = pd.read_sql(
                "SELECT surrogate_key as territory_key, territory_id FROM silver_sales_dim_territory WHERE is_current = true",
                engine
            )
            
            # Join to get surrogate keys
            df_combined = df_combined.merge(dim_customer, on='customer_id', how='left')
            df_combined = df_combined.merge(dim_territory, on='territory_id', how='left')
            
            # Data quality checks
            missing_customer = df_combined['customer_key'].isna().sum()
            missing_territory = df_combined['territory_key'].isna().sum()
            
            print(f"   📊 Data Quality: Missing customer refs: {missing_customer:,}, Missing territory refs: {missing_territory:,}")
            
            # Create table with proper schema if it doesn't exist
            table_exists = engine.dialect.has_table(engine.connect(), 'silver_sales_fact_orders')
            if not table_exists:
                print("   🔧 Creating table with proper schema...")
                with engine.begin() as conn:
                    conn.execute(text("""
                        CREATE TABLE silver_sales_fact_orders (
                            order_key BIGSERIAL PRIMARY KEY,
                            sales_order_id INTEGER NOT NULL,
                            customer_key BIGINT,
                            territory_key BIGINT,
                            order_date DATE,
                            due_date DATE,
                            ship_date DATE,
                            subtotal DECIMAL(18,2),
                            tax_amt DECIMAL(18,2),
                            freight DECIMAL(18,2),
                            total_due DECIMAL(18,2),
                            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            source_system VARCHAR(50)
                        )
                    """))
                    conn.execute(text("CREATE INDEX idx_fact_orders_customer ON silver_sales_fact_orders(customer_key)"))
                    conn.execute(text("CREATE INDEX idx_fact_orders_date ON silver_sales_fact_orders(order_date)"))
            else:
                # Truncate existing data
                with engine.begin() as conn:
                    conn.execute(text("TRUNCATE TABLE silver_sales_fact_orders"))
            
            # Create fact table with proper data types
            fact_orders = pd.DataFrame({
                'sales_order_id': pd.to_numeric(df_combined['sales_order_id'], errors='coerce').astype('Int64'),
                'customer_key': df_combined['customer_key'].astype('Int64'),
                'territory_key': df_combined['territory_key'].astype('Int64'),
                'order_date': pd.to_datetime(df_combined['order_date']).dt.date,
                'due_date': pd.to_datetime(df_combined['due_date']).dt.date,
                'ship_date': pd.to_datetime(df_combined['ship_date']).dt.date,
                'subtotal': pd.to_numeric(df_combined['subtotal'], errors='coerce'),
                'tax_amt': pd.to_numeric(df_combined['tax_amt'], errors='coerce'),
                'freight': pd.to_numeric(df_combined['freight'], errors='coerce'),
                'total_due': pd.to_numeric(df_combined['total_due'], errors='coerce'),
                'created_date': datetime.now(),
                'source_system': df_combined['source_system']
            })
            
            fact_orders.to_sql('silver_sales_fact_orders', engine, if_exists='append', index=False)
            print(f"   ✅ Created {len(fact_orders):,} order records")
            
            return {'total': len(fact_orders), 'missing_customer': int(missing_customer), 'missing_territory': int(missing_territory)}
            
        finally:
            engine.dispose()

    def create_fact_order_details_complete(**context):
        """Combine ALL order detail sources"""
        execution_date = context['ds']
        print(f"\n🔄 Processing fact_order_details from ALL sources...")
        
        engine = get_db_engine()
        
        try:
            # Source 1: Sales Order Details (has proper columns)
            df1 = pd.read_sql("""
                SELECT 
                    CAST(salesorderid AS VARCHAR) as sales_order_id,
                    CAST(salesorderdetailid AS VARCHAR) as sales_order_detail_id,
                    CAST(productid AS VARCHAR) as product_id,
                    CAST(orderqty AS INTEGER) as order_qty,
                    CAST(unitprice AS DECIMAL) as unit_price,
                    CAST(unitpricediscount AS DECIMAL) as unit_price_discount,
                    CAST(linetotal AS DECIMAL) as line_total,
                    'sales' as source_system
                FROM bronze_sales_sales_order_detail
            """, engine)
            
            # Note: bronze_adventureworks_api_sales_order_details is actually a sales orders table with nested JSON, not order details
            # We only use bronze_sales_sales_order_detail which has proper structure
            
            df_combined = df1.drop_duplicates(subset=['sales_order_detail_id'], keep='last')
            
            print(f"   📊 Loaded {len(df_combined):,} unique order details from Sales source")
            
            # Get dimension keys
            dim_product = pd.read_sql(
                "SELECT surrogate_key as product_key, product_id FROM silver_sales_dim_product WHERE is_current = true",
                engine
            )
            fact_orders = pd.read_sql(
                "SELECT order_key, sales_order_id FROM silver_sales_fact_orders",
                engine
            )
            
            # Convert sales_order_id to string for consistent merging
            df_combined['sales_order_id'] = df_combined['sales_order_id'].astype(str)
            fact_orders['sales_order_id'] = fact_orders['sales_order_id'].astype(str)
            
            # Join to get surrogate keys
            df_combined = df_combined.merge(dim_product, on='product_id', how='left')
            df_combined = df_combined.merge(fact_orders, on='sales_order_id', how='left')
            
            # Data quality checks
            missing_product = df_combined['product_key'].isna().sum()
            missing_order = df_combined['order_key'].isna().sum()
            
            print(f"   📊 Data Quality: Missing product refs: {missing_product:,}, Missing order refs: {missing_order:,}")
            
            # Create table with proper schema if it doesn't exist
            table_exists = engine.dialect.has_table(engine.connect(), 'silver_sales_fact_order_details')
            if not table_exists:
                print("   🔧 Creating table with proper schema...")
                with engine.begin() as conn:
                    conn.execute(text("""
                        CREATE TABLE silver_sales_fact_order_details (
                            order_detail_key BIGSERIAL PRIMARY KEY,
                            order_key BIGINT,
                            product_key BIGINT,
                            order_qty INTEGER,
                            unit_price DECIMAL(18,2),
                            unit_price_discount DECIMAL(18,2),
                            line_total DECIMAL(18,2),
                            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            source_system VARCHAR(50)
                        )
                    """))
                    conn.execute(text("CREATE INDEX idx_fact_details_order ON silver_sales_fact_order_details(order_key)"))
                    conn.execute(text("CREATE INDEX idx_fact_details_product ON silver_sales_fact_order_details(product_key)"))
            else:
                # Truncate existing data
                with engine.begin() as conn:
                    conn.execute(text("TRUNCATE TABLE silver_sales_fact_order_details"))
            
            # Create fact table with proper data types
            fact_details = pd.DataFrame({
                'order_key': df_combined['order_key'].astype('Int64'),
                'product_key': df_combined['product_key'].astype('Int64'),
                'order_qty': pd.to_numeric(df_combined['order_qty'], errors='coerce').astype('Int64'),
                'unit_price': pd.to_numeric(df_combined['unit_price'], errors='coerce'),
                'unit_price_discount': pd.to_numeric(df_combined['unit_price_discount'], errors='coerce'),
                'line_total': pd.to_numeric(df_combined['line_total'], errors='coerce'),
                'created_date': datetime.now(),
                'source_system': df_combined['source_system']
            })
            
            fact_details.to_sql('silver_sales_fact_order_details', engine, if_exists='append', index=False)
            print(f"   ✅ Created {len(fact_details):,} order detail records")
            
            return {'total': len(fact_details), 'missing_product': int(missing_product), 'missing_order': int(missing_order)}
            
        finally:
            engine.dispose()

    def validate_silver_layer(**context):
        """Comprehensive validation"""
        execution_date = context['ds']
        print(f"\n🔍 Validating Silver layer for {execution_date}")
        
        engine = get_db_engine()
        
        try:
            results = {}
            
            # Dimension tables
            dim_tables = [
                'silver_sales_dim_customer',
                'silver_sales_dim_product',
                'silver_sales_dim_product_category',
                'silver_sales_dim_product_subcategory',
                'silver_sales_dim_territory',
                'silver_hr_dim_employee'
            ]
            
            print("\n📊 DIMENSION TABLES:")
            for table in dim_tables:
                total = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {table}", engine).iloc[0, 0]
                current = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {table} WHERE is_current = true", engine).iloc[0, 0]
                historical = total - current
                results[table] = {'total': total, 'current': current, 'historical': historical}
                print(f"   ✅ {table}: {total:,} total ({current:,} current, {historical:,} historical)")
            
            # Fact tables
            fact_tables = [
                'silver_sales_fact_orders',
                'silver_sales_fact_order_details'
            ]
            
            print("\n📊 FACT TABLES:")
            for table in fact_tables:
                count = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {table}", engine).iloc[0, 0]
                results[table] = count
                print(f"   ✅ {table}: {count:,} records")
            
            # Summary
            total_records = 0
            for v in results.values():
                if isinstance(v, int):
                    total_records += v
                elif isinstance(v, dict):
                    total_records += v.get('total', 0)
            
            print(f"\n📊 SILVER LAYER SUMMARY:")
            print(f"   Total tables: {len(results)}")
            print(f"   Total records: {total_records:,}")
            print(f"\n🎉 Silver layer validation passed!")
            
            return results
            
        finally:
            engine.dispose()

    # ==================== TASK DEFINITIONS ====================
    
    dim_customer_task = PythonOperator(
        task_id='create_dim_customer_complete',
        python_callable=create_dim_customer_complete,
        provide_context=True
    )
    
    dim_product_task = PythonOperator(
        task_id='create_dim_product_complete',
        python_callable=create_dim_product_complete,
        provide_context=True
    )
    
    dim_product_category_task = PythonOperator(
        task_id='create_dim_product_category',
        python_callable=create_dim_product_category,
        provide_context=True
    )
    
    dim_product_subcategory_task = PythonOperator(
        task_id='create_dim_product_subcategory',
        python_callable=create_dim_product_subcategory,
        provide_context=True
    )
    
    dim_territory_task = PythonOperator(
        task_id='create_dim_territory_complete',
        python_callable=create_dim_territory_complete,
        provide_context=True
    )
    
    dim_employee_task = PythonOperator(
        task_id='create_dim_employee_complete',
        python_callable=create_dim_employee_complete,
        provide_context=True
    )
    
    fact_orders_task = PythonOperator(
        task_id='create_fact_sales_orders_complete',
        python_callable=create_fact_sales_orders_complete,
        provide_context=True
    )
    
    fact_details_task = PythonOperator(
        task_id='create_fact_order_details_complete',
        python_callable=create_fact_order_details_complete,
        provide_context=True
    )
    
    validate_task = PythonOperator(
        task_id='validate_silver_layer',
        python_callable=validate_silver_layer,
        provide_context=True
    )
    
    # ==================== TASK DEPENDENCIES ====================
    
    # Product categories before subcategories
    dim_product_category_task >> dim_product_subcategory_task
    
    # All dimensions first (parallel where possible)
    [dim_customer_task, dim_product_task, dim_product_category_task, dim_territory_task, dim_employee_task] >> fact_orders_task
    
    # Orders before details
    [fact_orders_task, dim_product_task] >> fact_details_task
    
    # Validate after everything
    fact_details_task >> validate_task
