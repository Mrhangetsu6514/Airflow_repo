from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Variable
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup

default_args = {
    'owner': 'liorj',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# TASK 1: Scrape real HTML from the web
def scrape_market_data():
    # Use a variable to store the URL so you can change it in the UI
    # Practice URL: A simple page that won't block you
    url = Variable.get("scraper_url", default="https://www.google.com/search?q=bitcoin+price+ils")
    
    # We must provide a 'User-Agent' so the website thinks we are a real browser
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        # Convert the raw HTML into a searchable 'Soup' object
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # This is where the magic happens for Matzati Casa:
        # You find the specific HTML tag that holds the price.
        # For this Google example, we'll just grab the page title for practice.
        page_title = soup.title.string
        
        print(f"Successfully scraped: {page_title}")
        return {"title": page_title, "status": "success", "url": url}
    else:
        raise Exception(f"Failed to fetch data. Status code: {response.status_code}")

# TASK 2: Extract and log the results
def log_scraped_info(ti):
    # Pull the dictionary from the previous task
    data = ti.xcom_pull(task_ids='scrape_web_task')
    
    print("--- WEB SCRAPER REPORT ---")
    print(f"Target URL: {data['url']}")
    print(f"Data Found: {data['title']}")

with DAG(
    dag_id='matzati_real_scraper_v1',
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=['matzati_casa', 'web_scraping']
) as dag:

    fetch_task = PythonOperator(
        task_id='scrape_web_task',
        python_callable=scrape_market_data
    )

    parse_task = PythonOperator(
        task_id='parse_data_task',
        python_callable=log_scraped_info
    )

    fetch_task >> parse_task