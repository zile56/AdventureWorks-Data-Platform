"""
Gold Layer DAG - Creates Business Intelligence Views
Uses corrected SQL with surrogate key JOINs
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

# Configuration
DB_CONNECTION = "postgresql+psycopg2://postgres:1004@postgres:5432/postgres"

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='gold_layer_simple_working',
    default_args=default_args,
    start_date=datetime(2025, 11, 12),
    schedule_interval='@daily',
    catchup=False,
    description='Gold Layer: Create BI Views with Corrected SQL',
    tags=['gold', 'bi', 'analytics']
) as dag:

    def create_gold_top_products(**context):
        """Create gold_top_products view"""
        print("🥇 Creating gold_top_products view...")
        engine = create_engine(DB_CONNECTION)
        
        sql = """
        DROP VIEW IF EXISTS gold_top_products CASCADE;
        
        CREATE OR REPLACE VIEW gold_top_products AS
        SELECT 
            p.product_id,
            p.product_name,
            p.product_number,
            p.color,
            p.standard_cost,
            p.list_price,
            COUNT(DISTINCT od.order_detail_key) as times_ordered,
            SUM(od.order_qty) as total_quantity_sold,
            SUM(od.line_total) as total_revenue,
            AVG(od.unit_price) as avg_selling_price,
            SUM(od.line_total) - (SUM(od.order_qty) * COALESCE(p.standard_cost, 0)) as estimated_profit,
            RANK() OVER (ORDER BY SUM(od.order_qty) DESC) as quantity_rank,
            RANK() OVER (ORDER BY SUM(od.line_total) DESC) as revenue_rank,
            MIN(o.order_date) as first_sale_date,
            MAX(o.order_date) as last_sale_date,
            CASE 
                WHEN RANK() OVER (ORDER BY SUM(od.line_total) DESC) <= 10 THEN 'Top 10 by Revenue'
                WHEN RANK() OVER (ORDER BY SUM(od.order_qty) DESC) <= 10 THEN 'Top 10 by Volume'
                ELSE 'Other'
            END as product_tier,
            CURRENT_TIMESTAMP as report_generated_at
        FROM silver_sales_dim_product p
        LEFT JOIN silver_sales_fact_order_details od ON p.surrogate_key = od.product_key
        LEFT JOIN silver_sales_fact_orders o ON od.order_key = o.order_key
        WHERE p.is_current = true
        GROUP BY p.product_id, p.product_name, p.product_number, p.color, p.standard_cost, p.list_price, p.surrogate_key
        ORDER BY total_revenue DESC NULLS LAST;
        """
        
        with engine.connect() as conn:
            conn.execute(text(sql))
            conn.execute(text("COMMIT"))
            
        print("✅ gold_top_products created successfully")
        return {'status': 'success', 'view': 'gold_top_products'}

    def create_gold_customer_analysis(**context):
        """Create gold_customer_analysis view"""
        print("👥 Creating gold_customer_analysis view...")
        engine = create_engine(DB_CONNECTION)
        
        sql = """
        DROP VIEW IF EXISTS gold_customer_analysis CASCADE;
        
        CREATE OR REPLACE VIEW gold_customer_analysis AS
        SELECT 
            c.customer_id,
            c.account_number,
            c.territory_id,
            t.territory_name,
            t.country_region,
            COUNT(DISTINCT o.order_key) as total_orders,
            SUM(o.total_due) as total_spent,
            AVG(o.total_due) as avg_order_value,
            SUM(o.subtotal) as total_subtotal,
            SUM(o.tax_amt) as total_tax_paid,
            SUM(o.freight) as total_freight_paid,
            MIN(o.order_date) as first_purchase_date,
            MAX(o.order_date) as last_purchase_date,
            MAX(o.order_date) - MIN(o.order_date) as customer_lifetime_days,
            COUNT(DISTINCT od.product_key) as unique_products_purchased,
            SUM(od.order_qty) as total_items_purchased,
            CASE 
                WHEN SUM(o.total_due) > 10000 THEN 'High Value'
                WHEN SUM(o.total_due) > 5000 THEN 'Medium Value'
                WHEN SUM(o.total_due) > 0 THEN 'Low Value'
                ELSE 'No Purchases'
            END as customer_segment,
            CURRENT_TIMESTAMP as report_generated_at
        FROM silver_sales_dim_customer c
        LEFT JOIN silver_sales_fact_orders o ON c.surrogate_key = o.customer_key
        LEFT JOIN silver_sales_fact_order_details od ON o.order_key = od.order_key
        LEFT JOIN silver_sales_dim_territory t ON c.territory_id = t.territory_id AND t.is_current = true
        WHERE c.is_current = true
        GROUP BY c.customer_id, c.account_number, c.territory_id, t.territory_name, t.country_region, c.surrogate_key
        ORDER BY total_spent DESC NULLS LAST;
        """
        
        with engine.connect() as conn:
            conn.execute(text(sql))
            conn.execute(text("COMMIT"))
            
        print("✅ gold_customer_analysis created successfully")
        return {'status': 'success', 'view': 'gold_customer_analysis'}

    def create_gold_sales_trends(**context):
        """Create gold_sales_trends view"""
        print("📈 Creating gold_sales_trends view...")
        engine = create_engine(DB_CONNECTION)
        
        sql = """
        DROP VIEW IF EXISTS gold_sales_trends CASCADE;
        
        CREATE OR REPLACE VIEW gold_sales_trends AS
        SELECT 
            EXTRACT(YEAR FROM o.order_date) as year,
            EXTRACT(MONTH FROM o.order_date) as month,
            TO_CHAR(o.order_date, 'YYYY-MM') as year_month,
            t.territory_name,
            t.country_region,
            t.group_name as region_group,
            COUNT(DISTINCT o.order_key) as total_orders,
            COUNT(DISTINCT o.customer_key) as unique_customers,
            SUM(od.order_qty) as total_quantity,
            SUM(o.total_due) as total_revenue,
            AVG(o.total_due) as avg_order_value,
            SUM(o.subtotal) as total_subtotal,
            SUM(o.tax_amt) as total_tax,
            SUM(o.freight) as total_freight,
            CURRENT_TIMESTAMP as report_generated_at
        FROM silver_sales_fact_orders o
        LEFT JOIN silver_sales_fact_order_details od ON o.order_key = od.order_key
        LEFT JOIN silver_sales_dim_territory t ON o.territory_key = t.surrogate_key AND t.is_current = true
        GROUP BY 
            EXTRACT(YEAR FROM o.order_date),
            EXTRACT(MONTH FROM o.order_date),
            TO_CHAR(o.order_date, 'YYYY-MM'),
            t.territory_name, t.country_region, t.group_name
        ORDER BY year DESC, month DESC;
        """
        
        with engine.connect() as conn:
            conn.execute(text(sql))
            conn.execute(text("COMMIT"))
            
        print("✅ gold_sales_trends created successfully")
        return {'status': 'success', 'view': 'gold_sales_trends'}

    def verify_gold_views(**context):
        """Verify all gold views were created and have data"""
        print("🔍 Verifying gold views...")
        engine = create_engine(DB_CONNECTION)
        
        views = ['gold_top_products', 'gold_customer_analysis', 'gold_sales_trends']
        results = {}
        
        for view in views:
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) as cnt FROM {view}"))
                count = result.fetchone()[0]
                results[view] = count
                print(f"   ✅ {view}: {count:,} records")
        
        print(f"\n🎉 Gold layer complete! Total views: {len(results)}")
        return results

    # Define tasks
    task_products = PythonOperator(
        task_id='create_gold_top_products',
        python_callable=create_gold_top_products
    )
    
    task_customers = PythonOperator(
        task_id='create_gold_customer_analysis',
        python_callable=create_gold_customer_analysis
    )
    
    task_trends = PythonOperator(
        task_id='create_gold_sales_trends',
        python_callable=create_gold_sales_trends
    )
    
    task_verify = PythonOperator(
        task_id='verify_gold_views',
        python_callable=verify_gold_views
    )
    
    # All views can be created in parallel, then verify
    [task_products, task_customers, task_trends] >> task_verify
