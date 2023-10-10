import mysql.connector
from datetime import datetime, timedelta
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import Dimension

# Установка соединения с базой данных
conn = mysql.connector.connect(
    host="localhost",
    user="username",    
    password="password",
    database="prod"
)

# Создание курсора для выполнения SQL-запросов
cursor = conn.cursor()

# Вычисление даты "сегодня - 2 дня"
today = datetime.now().date()
two_days_ago = today - timedelta(days=2)

# Формирование SQL-запроса
query = "SELECT order_number FROM orders WHERE site_id IN (1, 2, 3) AND created_at >= %s"
params = (two_days_ago,)

# Выполнение SQL-запроса
cursor.execute(query, params)

# Получение результатов запроса
results = cursor.fetchall()

def query_google_analytics(order_number):
    # Создание клиента для работы с API Google Analytics
    client = BetaAnalyticsDataClient()

    # Определение параметров запроса
    dimensions = [Dimension(name='ga:medium'), Dimension(name='ga:source')]
    metric = {'name': 'ga:sessions'}
    date_range = {'start_date': '2022-01-01', 'end_date': '2022-01-31'}
    order_number_filter = f'ga:eventLabel=={order_number}'

    # Выполнение запроса
    response = client.run_report(
        property=f'properties/{'YOUR_PROPERTY_ID'}',
        dimensions=dimensions,
        metrics=[metric],
        date_ranges=[date_range],
        filters=[order_number_filter]
    )

    # Обработка результатов запроса
    for row in response.rows:
        medium = row.dimension_values[0].value
        source = row.dimension_values[1].value
        sessions = row.metric_values[0].value


#  для каждого order_number производится запрос в ga4 для получения параметров medium/source 

for order_number in results:
    query_google_analytics(order_number)


# Закрытие курсора и соединения с базой данных
cursor.close()
conn.close()