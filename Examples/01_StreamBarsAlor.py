from datetime import datetime, timedelta
from time import sleep

from MarketPy.Schedule import MOEXStocks, MOEXFutures
from AlorPy import AlorPy  # Работа с Alor OpenAPI V2
from AlorPy.Config import Config, ConfigDemo  # Файл конфигурации


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    exchange = 'MOEX'  # Биржа 'MOEX' или 'SPBX'
    # symbol = 'SBER'  # Тикер
    symbol = 'SiU3'  # Формат фьючерса: <Тикер><Месяц экспирации><Последняя цифра года> Месяц экспирации: 3-H, 6-M, 9-U, 12-Z
    time_frame = timedelta(minutes=1)  # 1 минута
    # time_frame = timedelta(minutes=5)  # 5 минут
    # time_frame = timedelta(minutes=15)  # 15 минут
    # time_frame = timedelta(hours=1)  # 1 час
    delta = timedelta(seconds=3)  # Задержка перед запросом нового бара
    # schedule = MOEXStocks()  # Расписание фондового рынка Московской биржи
    schedule = MOEXFutures()  # Расписание срочного рынка Московской биржи

    ap_provider = AlorPy(Config.UserName, Config.RefreshToken)  # Провайдер работает со счетом по токену (из файла Config.py) Подключаемся к торговому счету
    while True:
        market_datetime_now = schedule.utc_to_msk_datetime(datetime.utcnow())  # Текущее время на бирже
        print('Текущее время на бирже:', market_datetime_now)
        trade_bar_datetime = schedule.get_trade_bar_datetime(market_datetime_now, time_frame)  # Дата и время бара, который будем получать
        print('Будем получать бар:', trade_bar_datetime)
        seconds_from = schedule.msk_datetime_to_utc_timestamp(trade_bar_datetime)  # Дата и время бара в timestamp UTC
        # print(trade_bar_datetime, '->', seconds_from, '->', schedule.utc_timestamp_to_msk_datetime(seconds_from))
        next_trade_bar_datetime = schedule.get_trade_bar_datetime(trade_bar_datetime + time_frame, time_frame)  # Дата и время следующего бара, когда будем получать
        print('Время получения бара:', next_trade_bar_datetime)
        sleep_time = next_trade_bar_datetime - market_datetime_now + delta  # Время ожидания
        print('Одижание в секундах:', sleep_time.seconds)
        sleep(sleep_time.seconds)  # Ждем
        bars = ap_provider.get_history(exchange, symbol, time_frame.seconds, seconds_from)['history']  # Дату начала нужно брать на секунду меньше, чтобы бар был получен
        # print(bars)
        bar = bars[0]  # Получаем первый (завершенный) бар из Alor
        print('Получен бар:', datetime.fromtimestamp(int(bar['time'])), 'Open =', bar['open'], 'High =', bar['high'], 'Low =', bar['low'], 'Close =', bar['close'], 'Volume =', bar['volume'])
        print()
