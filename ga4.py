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

def query_google_analytics(order_number,start_date,end_date):
    # Создание клиента для работы с API Google Analytics
    client = BetaAnalyticsDataClient()

    # Определение параметров запроса
    dimensions = [Dimension(name='ga:medium'), Dimension(name='ga:source')]
    metric = {'name': 'ga:sessions'}
    date_range = {'start_date': start_date, 'end_date': end_date}
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

        if medium or source != 0:
            # Выполнение запроса с использованием параметров
            query = "INSERT INTO analytics_data (order_id, medium, source) VALUES (%s, %s, %s)"
            values = (f'{medium}/{source}/shn{order_number}', medium, source)
            cursor.execute(query, values)

            # Подтверждение изменений в базе данных
            conn.commit()
        else:
            query = "SELECT orders.order_id, channels.channel FROM orders JOIN nodes ON orders.order_id=nodes.order_id JOIN channels ON nodes.channel_id=channels.channel_id"
            result = cursor.execute(query)
            if result['channel'] != 0:
                # Выполнение запроса с использованием параметров
                query = "INSERT INTO analytics_data (order_id, medium, source) VALUES (%s, %s, %s)"
                values = (f'{medium}/{source}/shn{order_number}', medium, source)
                cursor.execute(query, values)
                # Подтверждение изменений в базе данных
                conn.commit()
            else:
                # Выполнение запроса с использованием параметров
                query = "INSERT INTO analytics_data (order_id, medium, source) VALUES (%s, %s, %s)"
                values = (f'{medium}/{source}/shn{order_number}', medium, source)
                cursor.execute(query, values)
                # Подтверждение изменений в базе данных
                conn.commit()

#  для каждого order_number производится запрос в ga4 для получения параметров medium/source 

for order_number in results:
    query_google_analytics(order_number,today,two_days_ago)

# Закрытие курсора и соединения с базой данных
cursor.close()
conn.close()