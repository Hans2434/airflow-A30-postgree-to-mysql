import logging
import re
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook

# Konfigurasi Default Arguments (Sesuai Requirement)
default_args = {
    'owner': 'data-engineering-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

# Inisialisasi DAG
dag = DAG(
    'postgres_to_mysql_etl',
    default_args=default_args,
    description='ETL Pipeline from Operational Postgres to Warehouse MySQL',
    schedule_interval=timedelta(hours=6),
    start_date=datetime(2023, 1, 1), 
    catchup=False,
    tags=['etl', 'postgresql', 'mysql', 'data-pipeline'],
)

# --- FUNGSI EKSTRAKSI (EXTRACT) ---

def extract_customers_from_postgres(**context):

    logging.info("Starting extraction: Customers")
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        # Query dengan parameter binding untuk keamanan
        sql = """
            SELECT id, name, phone, state, updated_at 
            FROM raw_data.customers 
            WHERE updated_at >= CURRENT_DATE - INTERVAL '1 day'
        """
        records = pg_hook.get_records(sql)

        data = [
            {'id': r[0], 'name': r[1], 'phone': r[2], 'state': r[3]} 
            for r in records
        ]
        
        logging.info(f"Extracted {len(data)} customers.")
        context['ti'].xcom_push(key='customers_data', value=data)
        
    except Exception as e:
        logging.error(f"Error extracting customers: {e}")
        raise

def extract_products_from_postgres(**context):

    logging.info("Starting extraction: Products")
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        sql = """
            SELECT p.id, p.name, p.price, p.cost, p.category, s.supplier_name, p.updated_at
            FROM raw_data.products p
            JOIN raw_data.suppliers s ON p.supplier_id = s.id
            WHERE p.updated_at >= CURRENT_DATE - INTERVAL '1 day'
        """
        records = pg_hook.get_records(sql)
        
        data = [
            {
                'id': r[0], 'name': r[1], 'price': float(r[2]), 
                'cost': float(r[3]), 'category': r[4], 'supplier_name': r[5]
            }
            for r in records
        ]
        
        logging.info(f"Extracted {len(data)} products.")
        context['ti'].xcom_push(key='products_data', value=data)
        
    except Exception as e:
        logging.error(f"Error extracting products: {e}")
        raise

def extract_orders_from_postgres(**context):

    logging.info("Starting extraction: Orders")
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        sql = """
            SELECT id, customer_id, total_amount, status, created_at, updated_at
            FROM raw_data.orders
            WHERE updated_at >= CURRENT_DATE - INTERVAL '1 day'
        """
        records = pg_hook.get_records(sql)
        
        data = [
            {
                'id': r[0], 'customer_id': r[1], 
                'total_amount': float(r[2]), 'status': r[3], 'created_at': str(r[4])
            }
            for r in records
        ]
        
        logging.info(f"Extracted {len(data)} orders.")
        context['ti'].xcom_push(key='orders_data', value=data)

    except Exception as e:
        logging.error(f"Error extracting orders: {e}")
        raise


# --- FUNGSI TRANSFORMAZI & LOAD (TRANSFORM & LOAD) ---

def transform_and_load_customers(**context):

    logging.info("Starting Transform & Load: Customers")
    try:
        customers = context['ti'].xcom_pull(task_ids='extract_customers', key='customers_data')
        if not customers:
            logging.warning("No customer data found in XCom.")
            return

        mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
        rows_to_insert = []

        for c in customers:
            # 1. Transform Phone: (XXX) XXX-XXXX
            raw_phone = str(c.get('phone', ''))
            digits = re.sub(r'\D', '', raw_phone) # Hapus non-digit
            if len(digits) >= 10:
                formatted_phone = f"({digits[:3]}) {digits[3:6]}-{digits[6:10]}"
            else:
                formatted_phone = raw_phone # Fallback jika format tidak valid

            # 2. Transform State: Uppercase
            state = c.get('state', '').upper()

            rows_to_insert.append((c['id'], c['name'], formatted_phone, state))

        # 3. Loading (UPSERT)
        # INSERT INTO table (...) VALUES (...) ON DUPLICATE KEY UPDATE ...
        sql = """
            INSERT INTO dim_customers (id, name, phone, state) 
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                name=VALUES(name), 
                phone=VALUES(phone), 
                state=VALUES(state)
        """
        
        mysql_hook.insert_rows(
            table='dim_customers',
            rows=rows_to_insert,
            target_fields=['id', 'name', 'phone', 'state'],
            replace=False, # Kita pakai manual ON DUPLICATE KEY di bawah jika insert_rows tidak support upsert natif di versi lama
        )
        
        # OPSI ALTERNATIF (Manual Upsert Loop untuk Assignment ini):
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()
        cursor.executemany(sql, rows_to_insert)
        conn.commit()
        cursor.close()
        
        logging.info(f"Upserted {len(rows_to_insert)} customers into dim_customers.")

    except Exception as e:
        logging.error(f"Error transforming/loading customers: {e}")
        raise

def transform_and_load_products(**context):
    logging.info("Starting Transform & Load: Products")
    try:
        products = context['ti'].xcom_pull(task_ids='extract_products', key='products_data')
        if not products:
            logging.warning("No product data found.")
            return

        mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
        rows_to_insert = []

        for p in products:
            price = p['price']
            cost = p['cost']
            
            # 1. Transform Margin: ((price - cost) / price) * 100
            try:
                margin = ((price - cost) / price) * 100 if price != 0 else 0
            except ZeroDivisionError:
                margin = 0
            
            # 2. Transform Category: Title Case
            category = p['category'].title()

            rows_to_insert.append((p['id'], p['name'], margin, category, p['supplier_name']))

        # 3. Loading (UPSERT)
        sql = """
            INSERT INTO dim_products (id, name, profit_margin, category, supplier_name) 
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                profit_margin=VALUES(profit_margin), 
                category=VALUES(category),
                supplier_name=VALUES(supplier_name)
        """
        
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()
        cursor.executemany(sql, rows_to_insert)
        conn.commit()
        cursor.close()
        
        logging.info(f"Upserted {len(rows_to_insert)} products into dim_products.")

    except Exception as e:
        logging.error(f"Error transforming/loading products: {e}")
        raise

def transform_and_load_orders(**context):
    logging.info("Starting Transform & Load: Orders")
    try:
        orders = context['ti'].xcom_pull(task_ids='extract_orders', key='orders_data')
        if not orders:
            logging.warning("No order data found.")
            return

        mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
        rows_to_insert = []

        for o in orders:
            # 1. Transform Status: Lowercase
            status = o['status'].lower()
            
            # 2. Validate Amount: Must be positive
            amount = o['total_amount']
            if amount < 0:
                logging.warning(f"Order ID {o['id']} has negative amount: {amount}. Setting to 0.")
                amount = 0
            
            rows_to_insert.append((o['id'], o['customer_id'], amount, status, o['created_at']))

        # 3. Loading (UPSERT)
        sql = """
            INSERT INTO fact_orders (id, customer_id, total_amount, status, created_at) 
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                status=VALUES(status), 
                total_amount=VALUES(total_amount)
        """
        
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()
        cursor.executemany(sql, rows_to_insert)
        conn.commit()
        cursor.close()
        
        logging.info(f"Upserted {len(rows_to_insert)} orders into fact_orders.")

    except Exception as e:
        logging.error(f"Error transforming/loading orders: {e}")
        raise


# --- DEFINISI TASK ---

with dag:
    # Task Extract
    t_extract_customers = PythonOperator(
        task_id='extract_customers',
        python_callable=extract_customers_from_postgres,
        provide_context=True
    )

    t_extract_products = PythonOperator(
        task_id='extract_products',
        python_callable=extract_products_from_postgres,
        provide_context=True
    )

    t_extract_orders = PythonOperator(
        task_id='extract_orders',
        python_callable=extract_orders_from_postgres,
        provide_context=True
    )

    # Task Transform & Load
    t_load_customers = PythonOperator(
        task_id='transform_and_load_customers',
        python_callable=transform_and_load_customers,
        provide_context=True
    )

    t_load_products = PythonOperator(
        task_id='transform_and_load_products',
        python_callable=transform_and_load_products,
        provide_context=True
    )

    t_load_orders = PythonOperator(
        task_id='transform_and_load_orders',
        python_callable=transform_and_load_orders,
        provide_context=True
    )

    # --- DEPENDENSI ---
    # Alur dibuat paralel per entitas: Extract -> Load
    t_extract_customers >> t_load_customers
    t_extract_products >> t_load_products
    t_extract_orders >> t_load_orders