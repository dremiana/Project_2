import csv
import psycopg2
import task_2.connection_to_db as ctd
import logging

# Логирование в консоль, для отслеживания ошибок
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

csv_files = {'deal_info.csv': 'rd.temp_deal',
             'product_info.csv': 'rd.temp_product'}

try:
    # Подключение к базе данных
    conn = psycopg2.connect(host=ctd.host, database=ctd.dbname, user=ctd.user, password=ctd.password)
    cursor = conn.cursor()

    logging.info("Подключение к базе данных установлено")

    # Создание копий таблиц
    for csv_file, table_name in csv_files.items():
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        (LIKE rd.{table_name.split('_')[-1]} INCLUDING ALL)
        """)
        conn.commit()

        logging.info(f"Создана копия таблицы {table_name}")

        # Чтение данных из csv
        with open(csv_file, 'r', encoding='ansi') as csvfile:
            csvreader = csv.reader(csvfile, delimiter=',')
            headers = next(csvreader)

            # Запрос для вставки данных в таблицу
            insert_query = f"""
            INSERT INTO {table_name} ({','.join(headers)})
            VALUES ({','.join(['%s' for _ in headers])})
            """

            # Вставляем данные в таблицу
            for row in csvreader:
                # Заменяем пустые строки на None
                row = [None if value == '' else value for value in row]
                cursor.execute(insert_query, row)

            conn.commit()

        logging.info(f"Данные в таблицу {table_name} добавлены")


except Exception as e:
    logging.error(f"Произошла ошибка: {str(e)}")

finally:
    if conn:
        cursor.close()
        conn.close()
        logging.info("Соединение с базой данных закрыто")
