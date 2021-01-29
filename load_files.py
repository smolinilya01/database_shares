"""
Load files into DB
Обязательно нужно соблюдать полное наличие данных.
То есть в папке с данными не должно быть пропусков дней.
Если день будет пропушен, то функционал не позволяет добавить его.
ПРидется переписывать таблицу, что приведет к увелияению стоимости хранения в базе.
"""

from os import walk, path as os_path
from common.connections import create_conn
from pandas import (
    DataFrame, Series,
    read_sql, read_csv, read_excel,
)
from joblib import Parallel, delayed, cpu_count
from common.functions import str_in_tdelta

LOAD_PATH = r"F:\!Data_Shares\База эксель\База эксель"


def load_files() -> None:
    """Главная функция загрузки файлов в базу.
    Файлы - это файлы эксель с данными по торгам.
    """
    with create_conn() as conn:
        total_files = files_in_dir(path=LOAD_PATH)
        db_files = files_in_db(conn=conn)

        need_files = total_files\
            [~(total_files['name'].isin(db_files.iloc[:, 0]))]\
            ['path']

        create_operation_table(conn=conn)

        for path_ in need_files:
            add_data(path=path_, conn=conn)


def files_in_dir(path: str) -> DataFrame:
    """Возвращает список файлов в папке с данными, наименование файла с полным путем.

    :param path: путь к папке с выгрузками из квика
    """
    files = []
    for dir_ in walk(path):
        files_dir = [(f, dir_[0] + '\\' + f) for f in dir_[2]]
        files.extend(files_dir)
    files = DataFrame(data=files, columns=['name', 'path'])
    files = files.sort_values(by=['name'])
    files = files[~(files['name'].map(lambda x: x.startswith('~')))]
    return files


def files_in_db(conn: create_conn) -> DataFrame:
    """Возвращает наименования файлов, которые уже есть в базе.

    :param conn: соединение с DB postgres
    """
    cur = conn.cursor()
    q_0 = """
    CREATE TABLE IF NOT EXISTS public.files(
        name text PRIMARY KEY,
        path text NOT NULL
    )"""
    cur.execute(q_0)
    conn.commit()

    q_1 = """
    SELECT name FROM files
    """
    files = read_sql(q_1, conn)

    return files


def create_operation_table(conn: create_conn) -> None:
    """Создает, если это требуется таблицу со сторонами операций.

    :param conn: соединение с DB postgres
    """
    cur = conn.cursor()

    q_0 = """
    CREATE TABLE IF NOT EXISTS public.direction_operation(
        id int2 NOT NULL,
        direction text NOT NULL,
        CONSTRAINT direction_operation_pkey PRIMARY KEY (id)
    )
    """
    cur.execute(q_0)
    conn.commit()

    if len(read_sql('SELECT id FROM public.direction_operation  ', conn)) == 2:
        return None
    q_1 = """INSERT INTO public.direction_operation VALUES(-1, 'Продажа')"""
    cur.execute(q_1)

    q_2 = """INSERT INTO public.direction_operation VALUES(1, 'Купля')"""
    cur.execute(q_2)

    conn.commit()


def add_data(path: str, conn: create_conn) -> None:
    """Добавление данных из файла в DB postgres.
    Распараллеливание по акциям.

    :param path: путь к фалу
    :param conn: соединение с DB postgres
    """
    cur = conn.cursor()
    extend = os_path.splitext(os_path.split(path)[1])[1]

    if extend == '.xlsx':
        data = read_excel(path, parse_dates=[0])  # parse_dates=['Дата']
    elif extend == '.csv':
        data = read_csv(path, parse_dates=[0], sep='\t')  # parse_dates=['Дата']
    else:
        raise ValueError(f'Файл с неизвестных расширением {path}')

    if 'Инструмент' in data.columns:
        data = data.rename({'Инструмент': 'Бумага'}, axis=1)

    file_shares = Series(data=data['Бумага'].unique())
    db_shares = shares_in_db(conn)

    shares_to_list_share = file_shares[~(file_shares.isin(db_shares.iloc[:, 0]))]
    if len(shares_to_list_share) > 0:
        q_0 = """INSERT INTO public.list_share(share_name) VALUES(%s)"""
        cur.executemany(q_0, [(i,) for i in shares_to_list_share])
        conn.commit()

    num_cores = cpu_count()
    Parallel(n_jobs=num_cores)\
        (delayed(insert_share_data)(data, name) for name in file_shares)

    # добавление файла в список
    q_1 = """INSERT INTO public.files VALUES(%s, %s)"""
    cur.execute(q_1, (os_path.split(path)[1], path))
    conn.commit()


def shares_in_db(conn: create_conn) -> DataFrame:
    """Возвращает акции, которые есть в базе

    :param conn: соединение с DB postgres"""
    cur = conn.cursor()

    q_0 = """
    CREATE TABLE IF NOT EXISTS public.list_share(
        id serial PRIMARY KEY NOT NULL,
        share_name text NOT NULL
    )
    """
    cur.execute(q_0)
    conn.commit()

    q_1 = """SELECT share_name FROM list_share"""

    return read_sql(q_1, conn)


def insert_share_data(data: DataFrame, name: str) -> None:
    """Дописывает в конец таблицы данные по акции в соответствующую таблицу.

    :param data: данные из файла
    :param name: название акции"""
    with create_conn() as conn:
        cur = conn.cursor()

        q_0 = f"""SELECT id FROM list_share WHERE share_name = '{name}'"""
        id_share = read_sql(q_0, conn).iloc[0, 0]

        q_1 = f"""
        CREATE TABLE IF NOT EXISTS public.share_{id_share}(
            dt timestamp NOT NULL,
            micex_id int8 NOT NULL,
            price float4 NOT NULL,
            count int4 NOT NULL,
            amount float8 NOT NULL,
            operation int2 NOT NULL,
            cum_delta decimal NOT NULL
            CONSTRAINT positive_amount CHECK ((amount > (0)::double precision)),
            CONSTRAINT positive_count CHECK ((count >= 0)),
            CONSTRAINT positive_price CHECK ((price > (0)::double precision)),
            CONSTRAINT sell_buy_side CHECK (((operation = -1) OR (operation = 1))),
            CONSTRAINT share_{id_share}_pkey PRIMARY KEY (micex_id),
            CONSTRAINT share_{id_share}_operation_fkey FOREIGN KEY (operation) REFERENCES direction_operation(id)
        );
        CREATE INDEX IF NOT EXISTS share_{id_share}_dt ON public.share_{id_share} USING btree (dt);
        """
        cur.execute(q_1)
        conn.commit()

        cur_share_data = data[data['Бумага'] == name].\
            sort_values(by=['Номер']).\
            copy()

        # преобразовывание таблиы к стан форме
        cur_share_data['Операция'] = cur_share_data['Операция'].replace({
            'Продажа': -1,
            'Купля': 1
        })
        cur_share_data['dt'] = (cur_share_data.iloc[:, 0] +
                                cur_share_data['Время'].map(str_in_tdelta))  # cur_share_data['Дата']

        q_2 = f"""
        SELECT cum_delta 
        FROM public.share_{id_share}
        WHERE micex_id = (SELECT MAX(micex_id) FROM public.share_{id_share})
        """
        try:
            last_cum_delta = read_sql(q_2, conn).iloc[0, 0]
        except:
            last_cum_delta = 0

        cur_share_data['cum_delta'] = cur_share_data['Операция'] * cur_share_data['Объем']
        cur_share_data.iloc[0, -1] += last_cum_delta  # первое значение кум дельты увеличивается на последнее значение кумдельты из базы
        cur_share_data['cum_delta'] = cur_share_data['cum_delta'].cumsum()

        cur_share_data = cur_share_data[[
            'dt', 'Номер', 'Цена',
            'Кол-во', 'Объем', 'Операция',
            'cum_delta'
        ]]

        q_3 = f"""INSERT INTO public.share_{id_share} VALUES(%s,%s,%s,%s,%s,%s,%s)"""

        cur.executemany(q_3, cur_share_data.values)
        conn.commit()


if __name__ == '__main__':
    load_files()
