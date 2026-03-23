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

def scrape_real_exchange_rate():
    # Target the specific Google Finance Quote page for USD to ILS
    url = "https://www.google.com/finance/quote/USD-ILS"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    print(f"Fetching live rate from: {url}")
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 1. Target the specific currency price class
        # This class name is common for currency and stock prices on Google Finance
        price_element = soup.find('div', class_='YMlKec fxKbKc')
        
        if price_element:
            price_text = price_element.get_text()
            # Clean the text (remove the ₪ symbol) and convert to float
            price_float = float(price_text.replace('₪', '').replace(',', '').strip())
            
            print(f"Current USD/ILS Rate: {price_float}")
            return {
                "rate": price_float,
                "currency": "ILS",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        else:
            # If the class changed, we'll log the title so we can debug
            print(f"Could not find price element. Page title was: {soup.title.string}")
            raise Exception("Scraper could not locate the price element on the page.")
    else:
        raise Exception(f"Failed to fetch. Status code: {response.status_code}")

def process_market_analysis(ti):
    # CRITICAL: You must specify the task_id in Airflow 3
    data = ti.xcom_pull(task_ids='run_web_scraper')
    
    if data:
        print("--- MATZATI MARKET REPORT ---")
        # Now it will find the 'status' key in the dictionary returned by the scraper
        print(f"Status: {data.get('status', 'Unknown')}")
        print(f"Current Market Context: {data.get('title', 'No Title')}")
    else:
        print("No data received from the scraper.")

with DAG(
    dag_id='Currency_scraper_v2', # Now it should work with analysis
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 3, 1),
    catchup=False,
) as dag:

    run_scraper = PythonOperator(
        task_id='run_web_scraper',
        python_callable=scrape_real_exchange_rate
    )

    run_analysis = PythonOperator(
        task_id='run_market_analysis',
        python_callable=process_market_analysis
    )

    run_scraper >> run_analysis