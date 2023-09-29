from threading import Thread, Event
from datetime import datetime, timedelta

from MarketPy.Schedule import Schedule, MOEXStocks, MOEXFutures

from AlorPy import AlorPy  # Работа с Alor OpenAPI V2
from AlorPy.Config import Config  # Файл конфигурации


def stream_bars(exchange, symbol, schedule, time_frame, delta):
    """Поток получения новых бар по расписанию биржи

    :param str exchange: Биржа 'MOEX' или 'SPBX'
    :param str symbol: Тикер
    :param Schedule schedule: Расписание торгов
    :param timedelta time_frame: Временной интервал
    :param timedelta delta: Смещение в будущее, чтобы гарантированно получить сформированный бар
    """
    ap_provider = AlorPy(Config.UserName, Config.RefreshToken)  # Провайдер Alor
    tf = 'D' if time_frame == timedelta(days=1) else 'W' if time_frame == timedelta(weeks=1) else str(time_frame.seconds)  # Временной интервал для дневок, неделек и интрадея
    while True:
        market_datetime_now = schedule.utc_to_msk_datetime(datetime.utcnow())  # Текущее время на бирже
        print('\nТекущее время на бирже:', market_datetime_now)
        trade_bar_open_datetime = schedule.get_trade_bar_open_datetime(market_datetime_now, time_frame)  # Дата и время бара, который будем получать
        print('Будем получать бар:', trade_bar_open_datetime, 'UTC+03')
        trade_bar_request_datetime = schedule.get_trade_bar_request_datetime(trade_bar_open_datetime, time_frame)  # Дата и время запроса бара на бирже
        print('Время запроса бара:', trade_bar_request_datetime)
        sleep_time_secs = (trade_bar_request_datetime - market_datetime_now + delta).total_seconds()  # Время ожидания в секундах
        print('Ожидание в секундах:', sleep_time_secs)
        exit_event_set = exit_event.wait(sleep_time_secs)  # Ждем нового бара или события выхода из потока
        if exit_event_set:  # Если произошло событие выхода из потока
            ap_provider.close_web_socket()  # Перед выходом закрываем соединение с WebSocket
            return  # Выходим из потока, дальше не продолжаем

        seconds_from = schedule.msk_datetime_to_utc_timestamp(trade_bar_open_datetime)  # Дата и время бара в timestamp UTC
        bars = ap_provider.get_history(exchange, symbol, tf, seconds_from)  # Получаем ответ на запрос истории рынка
        if not bars:  # Если ничего не получили
            print('Данные не получены')
            continue  # Будем получать следующий бар
        bars = bars['history']  # Последний сформированный и текущий несформированный (если имеется) бары
        # print(bars)
        if len(bars) == 0:  # Если бары не получены
            print('Бар не получен')
            continue  # Будем получать следующий бар

        bar = bars[0]  # Получаем первый (завершенный) бар
        dt = schedule.utc_timestamp_to_msk_datetime(int(bar['time']))
        open_ = float(bar['open'])
        high = float(bar['high'])
        low = float(bar['low'])
        close = float(bar['close'])
        volume = int(bar['volume'])
        print('Получен бар:', dt, 'Open =', open_, 'High =', high, 'Low =', low, 'Close =', close, 'Volume =', volume)


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    exchange = 'MOEX'  # Биржа 'MOEX' или 'SPBX'
    symbol = 'SBER'  # Тикер
    # symbol = 'SiU3'  # Формат фьючерса: <Тикер><Месяц экспирации><Последняя цифра года> Месяц экспирации: 3-H, 6-M, 9-U, 12-Z
    schedule = MOEXStocks()  # Расписание фондового рынка Московской биржи
    # schedule = MOEXFutures()  # Расписание срочного рынка Московской биржи
    time_frame = timedelta(minutes=1)  # 1 минута
    # time_frame = timedelta(minutes=5)  # 5 минут
    # time_frame = timedelta(minutes=15)  # 15 минут
    # time_frame = timedelta(hours=1)  # 1 час
    # time_frame = timedelta(days=1)  # 1 день
    delta = timedelta(seconds=3)  # Задержка перед запросом нового бара

    exit_event = Event()  # Определяем событие выхода из потока
    stream_bars_thread = Thread(name='stream_bars', target=stream_bars, args=(exchange, symbol, schedule, time_frame, delta))  # Создаем поток получения новых бар
    stream_bars_thread.start()  # Запускаем поток
    input()  # Ожидаем нажатия на клавишу Ввод (Enter)
    exit_event.set()  # Устанавливаем событие выхода из потока
