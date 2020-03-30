"""Common functions"""

from datetime import timedelta, time
from typing import Union


def str_in_tdelta(x: Union[str, time]) -> timedelta:
    """Преобразует значение в timedelta.

    :param x: значение из столбца Время
    """
    if isinstance(x, str):
        val = [int(i) for i in x.split(sep=':')]
        return timedelta(hours=val[0], minutes=val[1], seconds=val[2])
    elif isinstance(x, time):
        return timedelta(hours=x.hour, minutes=x.minute, seconds=x.second)
