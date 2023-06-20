import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime
from zipfile import ZipFile
from io import BytesIO

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv.deprecated'

def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def get_top_10_domain_zone():
    top_dz_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_dz_df['dom_zone'] = top_dz_df.domain.str.split('.').str[-1]
    top_data_top_10 = top_dz_df.dom_zone.value_counts().head(10).reset_index().sort_values('dom_zone', ascending=False).rename(columns={'index':'dom_zone', 'dom_zone':'count'})
    with open('top_data_top_10.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index=False, header=False))


def get_max_len_name():
    top_dz_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_dz_df['len_name'] = top_dz_df.domain.str.len()
    top_max_len_name = top_dz_df.sort_values(['len_name', 'domain'], ascending = [False, True]).head(1)
    with open('top_max_len_name.csv', 'w') as f:
        f.write(top_max_len_name.to_csv(index=False, header=False))

def get_airflow_place():
    top_dz_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_place = top_dz_df.query('domain == "airflow.com"')[{'domain', 'rank'}]
    with open('airflow_place.csv', 'w') as f:
        f.write(airflow_place.to_csv(index=False, header=False))

def print_data(ds):
    with open('top_data_top_10.csv', 'r') as f:
        top_10_data = f.read()
    with open('top_max_len_name.csv', 'r') as f:
        max_name_data = f.read()
    with open('airflow_place.csv', 'r') as f:
        airflow_data = f.read()
    date = ds

    print(f'Top 10 domains zone for date {date}')
    print(top_10_data)

    print(f'Longest domain name for date {date}')
    print(max_name_data)
                              
    print(f'Airflow rank for date {date}')
    print(airflow_data)


default_args = {
    'owner': 'r-shamjan',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 5, 30),
}
schedule_interval = '30 16 * * *'

r_sham_dag = DAG('r-shamjan', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=r_sham_dag)

t2 = PythonOperator(task_id='get_top_10_domain_zone',
                    python_callable=get_top_10_domain_zone,
                    dag=r_sham_dag)

t2_max_len_name = PythonOperator(task_id='get_max_len_name',
                        python_callable=get_max_len_name,
                        dag=r_sham_dag)

t2_airflow_place = PythonOperator(task_id='get_airflow_place',
                        python_callable=get_airflow_place,
                        dag=r_sham_dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=r_sham_dag)

t1 >> [t2, t2_max_len_name, t2_airflow_place] >> t3