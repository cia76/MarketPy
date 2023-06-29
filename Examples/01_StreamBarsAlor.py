from datetime import datetime, timedelta
from time import sleep

from MarketPy.Schedule import MOEXStocks, MOEXFutures
from AlorPy import AlorPy  # Работа с Alor OpenAPI V2
from AlorPy.Config import Config, ConfigDemo  # Файл конфигурации


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    exchange = 'MOEX'  # Биржа 'MOEX' или 'SPBX'
    # symbol = 'SBER'  # Тикер
    symbol = 'SiU3'  # Формат фьючерса: <Тикер><Месяц экспирации><Последняя цифра года> Месяц экспирации: 3-H, 6-M, 9-U, 12-Z
    # time_frame = timedelta(minutes=1)  # 1 минута
    # time_frame = timedelta(minutes=5)  # 5 минут
    # time_frame = timedelta(minutes=15)  # 15 минут
    # time_frame = timedelta(hours=1)  # 1 час
    time_frame = timedelta(days=1)  # 1 день
    delta = timedelta(seconds=3)  # Задержка перед запросом нового бара
    # schedule = MOEXStocks()  # Расписание фондового рынка Московской биржи
    schedule = MOEXFutures()  # Расписание срочного рынка Московской биржи

    ap_provider = AlorPy(Config.UserName, Config.RefreshToken)  # Провайдер работает со счетом по токену (из файла Config.py) Подключаемся к торговому счету
    tf = 'D' if time_frame == timedelta(days=1) else 'W' if time_frame == timedelta(weeks=1) else time_frame.seconds  # Временной интервал для дневок, неделек и интрадея
    while True:
        market_datetime_now = schedule.utc_to_msk_datetime(datetime.utcnow())  # Текущее время на бирже
        print('\nТекущее время на бирже:', market_datetime_now)
        trade_bar_open_datetime = schedule.get_trade_bar_open_datetime(market_datetime_now, time_frame)  # Дата и время бара, который будем получать
        print('Будем получать бар:', trade_bar_open_datetime, 'UTC+03')
        seconds_from = schedule.msk_datetime_to_utc_timestamp(trade_bar_open_datetime)  # Дата и время бара в timestamp UTC
        # print(trade_bar_datetime, '->', seconds_from, '->', schedule.utc_timestamp_to_msk_datetime(seconds_from))
        trade_bar_request_datetime = schedule.get_trade_bar_request_datetime(trade_bar_open_datetime, time_frame)  # Дата и время запроса бара на бирже
        print('Время запроса бара:', trade_bar_request_datetime)
        sleep_time = trade_bar_request_datetime - market_datetime_now + delta  # Время ожидания
        print('Одижание в секундах:', sleep_time.seconds)
        sleep(sleep_time.seconds)  # Ждем
        bars = ap_provider.get_history(exchange, symbol, tf, seconds_from)['history']  # Получаем последний сформированный и текущий несформированный (если имеется) бары
        # print(bars)
        if len(bars) == 0:  # Если бары не получены
            print('Бар не получен')
        else:  # Получен последний сформированный бар
            bar = bars[0]  # Получаем первый (завершенный) бар
            print('Получен бар:', datetime.fromtimestamp(int(bar['time'])), 'Open =', bar['open'], 'High =', bar['high'], 'Low =', bar['low'], 'Close =', bar['close'], 'Volume =', bar['volume'])
