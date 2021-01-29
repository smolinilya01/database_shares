"""Функции для помощи с обработкой ошибок"""

from common.connections import create_conn
from joblib import Parallel, delayed, cpu_count
from pandas import read_sql
from datetime import datetime


def delete_data_with_date(date: datetime) -> None:
    """Удаляет данные в таблицах акия после определенной даты

    :param date: дата, после которой нужно удалить данные (включительно)
    """
    with create_conn() as conn:
        q_0 = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' and 
        table_name not in ('div', 'list_share', 'files', 'direction_operation')
        """
        share_tables = read_sql(q_0, conn)

        num_cores = cpu_count()
        Parallel(n_jobs=num_cores) \
            (delayed(delete_sql)(table, date) for table in share_tables.iloc[:, 0])

        print("""Потом обязательно нужно удалить строки из таблицы files!!!!!""")
        print(f"""Которые отвечают за файлы после {date}!!!!""")


def delete_sql(table: str, date: datetime) -> None:
    """Удаляет данные после даты date

    :param table: название таблицы
    :param date: дата, после которой нужно удалить данные (включительно)
    """
    with create_conn() as conn:
        cur = conn.cursor()
        q_0 = f"""delete from {table} where dt > '{date}'"""
        cur.execute(q_0)
        conn.commit()
