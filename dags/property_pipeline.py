from airflow.sdk import Asset, dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import datetime
import random

listings_asset = Asset(
    uri="postgres://postgres:5432/airflow/public/listings_summary",
    name="matzati_listings"
)

@dag(
    dag_id="matzati_property_pipeline",
    schedule="@daily",
    start_date=datetime.datetime(2026, 1, 1),
    catchup=False,
)
def property_pipeline():

    @task(outlets=[listings_asset])
    def update_tableau_data():
        # Generating a larger set of mock data
        cities = ['Tel Aviv', 'Holon', 'Haifa', 'Jerusalem', 'Rishon LeZion', 'Bat Yam']
        data = {
            'property_id': [100 + i for i in range(10)],
            'price_nis': [random.randint(3000, 9000) for _ in range(10)],
            'city': [random.choice(cities) for _ in range(10)],
            'last_updated': [datetime.datetime.now()] * 10
        }
        df = pd.DataFrame(data)

        # Connect using the 'postgres_default' you created in the UI
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        engine = pg_hook.get_sqlalchemy_engine()
        
        # 'replace' ensures the old test data is swapped with the new set
        df.to_sql('listings_summary', engine, if_exists='replace', index=False)
        print(f"Uploaded {len(df)} listings to Postgres.")

    update_tableau_data()

property_pipeline()