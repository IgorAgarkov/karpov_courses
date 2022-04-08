import pandas as pd 
from datetime import timedelta 
from datetime import datetime 
from airflow.decorators import dag, task 
from airflow.operators.python import get_current_context 
from airflow.models import Variable 

default_args = { 
    'owner': 'a.batalov', 
    'depends_on_past': False, 
    'retries': 2, 
    'retry_delay': timedelta(minutes=5), 
    'start_date': datetime(2021, 10, 7), 
    'schedule_interval': '0 12 * * *' 
} 

@dag(default_args=default_args, catchup=False) 
def i_agarkov_18_airflow_2():
    
    @task(retries=3) 
    def get_data(): 
        df = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        return df 
    
    @task(retries=4, retry_delay=timedelta(10)) 
    def global_bestseller(df):
        sale_year = 1994 + hash(f'i-agarkov-18') % 23 # определяем год
        global_bestseller = (
            df
                .query('Year == @sale_year')
                .groupby('Name', as_index=False)
                .agg({'Global_Sales': 'sum'})
                .rename(columns={'Global_Sales': 'quantity', 'Name': 'words_bestseller'})
                .sort_values('quantity', ascending=False)
                .words_bestseller
                .head(1)
                .to_list()
#                 .to_csv(index=False)
        )
        return global_bestseller
    
    
    @task() 
    def europ_genre(df):
        sale_year = 1994 + hash(f'i-agarkov-18') % 23
        genre = (
            df
                .query('Year == @sale_year')
                .groupby('Genre', as_index=False)
                .agg({'EU_Sales': 'sum'})
                .query('EU_Sales == EU_Sales.max()')
                .Genre
                .to_list()
#                 .to_csv(index=False)
        )
        return genre 
    
    

    @task() 
    def na_platform(df):
        sale_year = 1994 + hash(f'i-agarkov-18') % 23
        platform = (
            df
                .query('Year == @sale_year')
                .groupby(['Platform', 'Name'], as_index=False)
                .agg({"EU_Sales": 'sum'})
                .query('EU_Sales > 1')
                .groupby('Platform', as_index=False)
                .agg({'Name': 'count'})
                .rename(columns={'Name': 'quantity'})
                .query('quantity == quantity.max()')
                .Platform
                .to_list()
#                 .to_csv(index=False)
        )
        return platform 
    
    
    @task() 
    def jap_publisher(df):
        sale_year = 1994 + hash(f'i-agarkov-18') % 23
        publisher = (
            df
                .query('Year == @sale_year')
                .groupby('Publisher', as_index=False)
                .agg({'JP_Sales': 'mean'})
                .query('JP_Sales == JP_Sales.max()')
                .Publisher
                .to_list()
#                 .to_csv(index=False)
        )
        return publisher

    
    @task()
    def jap_quantity(df):
        sale_year = 1994 + hash(f'i-agarkov-18') % 23
        quantity = (
            df
                .query('Year == @sale_year')
                .groupby('Name', as_index=False)
                .agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'})
                .query('EU_Sales > JP_Sales')
                .shape[0]
        )
        return quantity
    
    
    
    @task() 
    def print_data(global_bestseller, genre, platform, publisher, quantity): 
        date = 1994 + hash(f'i-agarkov-18') % 23 
        print(f'Самая продаваемая игра мире в {date}:') 
        print(*global_bestseller)
        print()
        
        print(f'Самые продаваемые жанры в Европе в {date}:')
        print(*genre, sep=', ')
        print()
        
        print(f'Платформа с нибольшим количеством игр, которые продались более чем миллионным тиражом в Северной Америке в {date}:')
        print(*platform, sep=', ')
        print()
        
        print(f'У какого издателя самые высокие средние продажи в Японии в {date}:')
        print(*publisher, sep=', ')
        print()
        
        print(f'Количество игр продававшихся в Европе лучше, чем в Японии в {date}:')
        print(quantity)
        
    df = get_data() 
    global_bestseller = global_bestseller(df) 
    genre     = europ_genre(df) 
    platform  = na_platform(df) 
    publisher = jap_publisher(df) 
    quantity  = jap_quantity(df)
    
    print_data(global_bestseller, genre, platform, publisher, quantity) 

i_agarkov_18_airflow_2 = i_agarkov_18_airflow_2()