from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Variable # Correct Airflow 3 SDK Import
from datetime import datetime, timedelta
import random

# Default settings for the DAG
default_args = {
    'owner': 'lior',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# TASK 1: Simulate scraping based on a UI Variable
def simulate_scrape():
    # In Airflow 3, Variable.get uses 'default=' instead of 'default_var='
    target = Variable.get("practice_scraper_target", default="apartments")
    
    print(f"Scraping data for target: {target}")
    
    # Generate some random "scraped" data points
    data_points = [random.randint(5000, 15000) for _ in range(3)]
    return {"category": target, "prices": data_points}

# TASK 2: Analyze the data and update the Dashboard UI
def analyze_and_report(ti):
    # Pull the scraped data from the previous task
    scraped_info = ti.xcom_pull(task_ids='scrape_data_task')
    
    category = scraped_info['category']
    prices = scraped_info['prices']
    avg_price = sum(prices) / len(prices)
    
    # Create a nice summary for the dashboard
    summary = f"🏠 {category.upper()} | Avg Rent: ₪{avg_price:,.0f}"
    
    # Correct Airflow 3 method to pin info to the Grid View
    ti.set_note(summary)
    
    print(f"Analysis Complete: {summary}")

# DAG Definition
with DAG(
    dag_id='property_price_practice_v2',
    default_args=default_args,
    description='Practice DAG for Variables and Dashboard Notes',
    schedule=None, # Manual trigger only for practice
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=['matzati_casa', 'learning'],
) as dag:

    scrape_data = PythonOperator(
        task_id='scrape_data_task',
        python_callable=simulate_scrape
    )

    analyze_data = PythonOperator(
        task_id='analyze_data_task',
        python_callable=analyze_and_report
    )

    # Set the workflow order
    scrape_data >> analyze_data