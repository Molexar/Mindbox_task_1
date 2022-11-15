import random
import pandas as pd

df = pd.DataFrame(data={'customer_id': [1, 2, 3, 4, 1, 1, 3], 'product_id': [2, 1, 3, 4, 4, 4, 1],
                        'timestamp': [10000, 10000, 10000, 1000000, 10000, 10, 10000]})

DELTA = 180


def application(subframe):
    """
    Алгоритм прост:
    1. Сортирую полученный сгруппированный датафрейм по времени посещения
    2. Затем иду по столбцу timestamp, ищу отклонение предыдущего значения от текущего
        Если оно больше 3 минут, то текущий просмотр попадает в другую сессию, генерю для нее id
    3. Таким образом на выходе получаю сабфрейм с номерами сессий,
        в данном случае разница между любым временем просмотра в каждой сессии от других значений будет более 3 минут,
        а внутри сессии значения попарно могут отличаться на более чем 3 минуты,
        Например [100 250 400]. Тут разница между 100 и 400 более 180, но эти просмотры образуют сессию,
        так как последовательная разность менее 3 минут

    Итоговая сложность алгоритма не считая groupby составляет O(n + n * log(n)), при сортировке quicksort
    """
    temp = subframe.sort_values(by=['timestamp'])
    temp_id = random.getrandbits(32)
    prev = 0
    for index, row in temp.iterrows():
        if abs(row['timestamp'] - prev) > DELTA:
            temp_id = random.getrandbits(32)
        row['session'] = temp_id
        prev = row['timestamp']
    return temp


def set_sessions(dataframe: pd.DataFrame):
    dataframe['session'] = 0
    allocated = dataframe.groupby('customer_id', as_index=False, group_keys=True).apply(application)
    return dataframe.drop('session', axis=1).merge(allocated, on=['customer_id', 'product_id', 'timestamp'],
                                                   how='inner')


print(set_sessions(df))
