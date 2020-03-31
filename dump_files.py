"""Dump files

Выгрузка всех акций в файлы для просмотра через эксель"""

from common.connections import create_conn
from pandas import DataFrame, read_sql
from joblib import Parallel, delayed, cpu_count

DUMP_PATH = r"E:\!Base shares 2016\Экспорт"


def dump_files() -> None:
    """Выгружает готовые файлы (csv) с данными для графиков"""
    with create_conn() as conn:

        q_0 = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' and 
              table_name not in ('div', 'list_share', 'files', 'direction_operation')
        ORDER BY table_name
        """
        tables = read_sql(q_0, conn)

        num_cores = cpu_count()
        Parallel(n_jobs=num_cores) \
            (delayed(dump_share)(table) for table in tables.iloc[:, 0])


def dump_share(name_table: str) -> None:
    """Выгружает данные по акции в таблице table, подходит для распараллеливания.

    :param name_table: наименование таблицы в DB postgres
    """
    with create_conn() as conn:
        q_0 = f"""select count(micex_id) from {name_table}"""
        count_rows = read_sql(q_0, conn).iloc[0, 0]
        # по столько срок пропускать, что бы получить примерно 25_000 строк
        div_rows: int = count_rows // 25_000

        q_1 = f"""select share_name from list_share where id = {int(name_table.split('_')[1])}"""
        name_share: str = read_sql(q_1, conn).iloc[0, 0]

        q_2 = f"""
        select 
            t.dt, t.micex_id, t.price, t.cum_delta
        from 
            (select dt, micex_id, price, cum_delta, row_number() over(order by micex_id) as num 
            from {name_table}) as t 
        where 
            t.num % {div_rows} = 0
        """
        data: DataFrame = read_sql(q_2, conn, parse_dates=['dt'])
        data['name_share'] = name_share
        data['empty_1'], data['empty_2'], data['empty_3'], data['empty_4'] = \
            None, None, None, None

        order_columns = [
            'dt', 'micex_id', 'empty_1',
            'name_share', 'price', 'empty_2',
            'empty_3', 'empty_4', 'cum_delta'
        ]
        data = data[order_columns]

        path_file = DUMP_PATH + '\\' + name_share.split(' [')[0] + '.csv'
        data.to_csv(path_file, encoding='ansi', index=False, sep=',')


if __name__ == "__main__":
    dump_files()
