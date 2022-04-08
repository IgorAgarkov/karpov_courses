import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def top_domains():
    '''
    Находит топ-10 доменных зон по численности доменов
    '''
    top_1m = pd.read_csv(TOP_1M_DOMAINS_FILE, index_col=0, names=['sites'])
    top_1m['domain'] = top_1m.sites.apply(lambda x: x[x.rfind('.') + 1:])
    top_1m = top_1m.groupby('domain', as_index=False).agg({'sites': 'count'}).rename(columns={'sites': 'num'})
    top_10_domains = top_1m.sort_values('num', ascending=False).head(10).domain
    with open('top_10_domain.csv', 'w') as f:
        f.write(top_10_domains.to_csv(index=False, header=False))

def longest_domain():
    '''
    Находит самый длинный домен, если их несколько - берёт первый по алфавиту
    '''
    top_1m = pd.read_csv(TOP_1M_DOMAINS_FILE, index_col=0, names=['sites'])
    top_1m['domain'] = top_1m.sites.apply(lambda x: x[x.rfind('.') + 1:])
    top_1m['length'] = top_1m.domain.apply(lambda x: len(x))
    top_1m = top_1m.sort_values('length', ascending=False)
    top_1m = top_1m.length.head(1)
    with open('longest_domain.csv', 'w') as f:
        f.write(top_1m.to_csv(index=False, header=False))

def airflow_position():
    '''
    Показывает на каком месте находится сайт airflow.com
    '''
    top_1m = pd.read_csv(TOP_1M_DOMAINS_FILE, index_col=0, names=['sites'])
    
    top_1m = top_1m.query('sites == "airflow.com"')
    if len(top_1m) == 0:
        with open('airflow_position.csv', 'w') as f:
            f.write('Домена airflow.com нет в списке')
    else:
        num = top_1m.reset_index()['index']
        with open('airflow_position.csv', 'w') as f:
            f.write(num.to_csv(index=False, header=False))

def print_data(ds):
    with open('top_10_domain.csv', 'r') as f:
        top_10_domain = f.read()
    with open('longest_domain.csv', 'r') as f:
        longest_domain = f.read()
    with open('airflow_position.csv', 'r') as f:
        airflow_position = f.read()
    date = ds

    print(f'Топ 10 доменов по популярности на дату {date}')
    print(top_10_domain)

    print(f'Самый длинный домен на дату {date}')
    print(longest_domain)
    print(f'Место сайта airflow.com на дату {date}')
    print(airflow_position)


default_args = {
    'owner': 'i-agarkov-18',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 3, 26),
}
schedule_interval = '0 12 * * *'

dag = DAG('top_10_ru_new_i-agarkov-18', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_domains',
                    python_callable=top_domains,
                    dag=dag)

t3 = PythonOperator(task_id='get_longest_domain',
                        python_callable=longest_domain,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflow_position',
                        python_callable=airflow_position,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5
