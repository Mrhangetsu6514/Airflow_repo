from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
# Use the new SDK Variable instead of airflow.models
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
    url = "https://www.google.com/finance/quote/USD-ILS"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch. Status code: {response.status_code}")

    soup = BeautifulSoup(response.text, 'html.parser')
    price_element = soup.find('div', class_='YMlKec fxKbKc')
    
    if not price_element:
        raise Exception("Scraper could not locate the price element.")

    # Clean and convert to float for math
    raw_price = price_element.get_text().replace('₪', '').replace(',', '').strip()
    current_rate = float(raw_price)

    # --- History Logic ---
    # Retrieve the last rate. We cast to float because Variables are stored as strings.
    prev_rate_str = Variable.get("usd_ils_last_rate", default_var=str(current_rate))
    prev_rate = float(prev_rate_str)
    
    daily_change = current_rate - prev_rate
    
    # FIX: Explicitly cast the float to a string to avoid the ValidationError
    Variable.set("usd_ils_last_rate", str(current_rate))

    return {
        "rate": current_rate,
        "previous_rate": prev_rate,
        "daily_change": round(daily_change, 4),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

def process_market_analysis(ti):
    data = ti.xcom_pull(task_ids='run_web_scraper')
    
    if data:
        print("--- USD/ILS DAILY REPORT ---")
        print(f"Current Rate: {data['rate']} ₪")
        print(f"Daily Change: {data['daily_change']}")
    else:
        print("No data received.")

with DAG(
    dag_id='Currency_scraper_v3',
    default_args=default_args,
    schedule='@daily',
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