"""Common connections"""

import psycopg2


def create_conn() -> psycopg2.connect:
    """Создание подключение к базе postgres, возвращает"""
    return psycopg2.connect(
        dbname='postgres',
        user='postgres',
        password=1111,
        host='localhost',
        port=5433)
