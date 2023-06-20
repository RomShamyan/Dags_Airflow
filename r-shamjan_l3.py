import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime
from io import StringIO
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

df_vgsales = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

my_year = 1994 + hash(f'r-shamjan') % 23

default_args = {
    'owner': 'r-shamjan',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 6, 2),
    'schedule_interval': '30 15 * * *'
}

CHAT_ID = -814345397
try:
    BOT_TOKEN = Variable.get('telegram_secret')
except:
    BOT_TOKEN = ''
    
BOT_TOKEN = '5921557042:AAHGg-pLzNsevRb3dsGwWbYXJIq33x9TztU'

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass

@dag(default_args=default_args, catchup=False)
def r_shamyan_less3():
    @task(retries=3)
    def get_data():
        sales_data = pd.read_csv(df_vgsales).query('Year == @my_year')
        return sales_data

    @task(retries=4, retry_delay=timedelta(10)) 
    # Какая игра была самой продаваемой в этом году во всем мире?
    def get_task_1(sales_data):
        best_game = sales_data.groupby('Name', as_index=False).agg({'Global_Sales' : 'sum'}).sort_values('Global_Sales', ascending = False).head(1)
        return best_game

    @task() 
    # Игры какого жанра были самыми продаваемыми в Европе?
    def get_task_2(sales_data):
        genre_EU = sales_data.groupby('Genre', as_index=False).agg({'EU_Sales' : 'sum'}).sort_values('EU_Sales', ascending = False).head(1)
        return genre_EU

    @task() 
    # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    def get_task_3(sales_data):
        platform_NA = sales_data.groupby('Platform', as_index=False).agg({'NA_Sales' : 'sum'}).sort_values('NA_Sales', ascending = False).query('NA_Sales > 1')
        return platform_NA

    @task() 
    # У какого издателя самые высокие средние продажи в Японии?
    def get_task_4(sales_data):
        publish_JP = sales_data.groupby('Publisher', as_index=False).agg({'JP_Sales' : 'mean'}).sort_values('JP_Sales', ascending = False).head(1)
        return publish_JP
    
    @task() 
    # Сколько игр продались лучше в Европе, чем в Японии?
    def get_task_5(sales_data):
        EU_more_JP = sales_data.query('EU_Sales > JP_Sales').Name.count()
        return EU_more_JP

    @task(on_success_callback=send_message)
    def print_data(best_game, genre_EU, platform_NA, publish_JP, EU_more_JP):

        context = get_current_context()
        date = context['ds']


        print(f'''Data for {my_year} for {date}
                  Best world games sales: {best_game}
                  Best genre sales in Europe: {genre_EU}
                  Best platform sales in North America: {platform_NA}
                  Mean publisher sales in Japan: {publish_JP}
                  Count of Europe sales better than Japan: {EU_more_JP}''')

    sales_data = get_data()
    
    best_game = get_task_1(sales_data)
    genre_EU = get_task_2(sales_data)
    platform_NA = get_task_3(sales_data)
    publish_JP = get_task_4(sales_data)
    EU_more_JP = get_task_5(sales_data)

    print_data(best_game, genre_EU, platform_NA, publish_JP, EU_more_JP)

r_shamyan_less3 = r_shamyan_less3()
