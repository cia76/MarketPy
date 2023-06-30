from threading import Thread, Event
from datetime import datetime, timedelta

from MarketPy.Schedule import MOEXStocks, MOEXFutures

from TinkoffPy import TinkoffPy  # Работа с Tinkoff Invest API из Python
from TinkoffPy.Config import Config  # Файл конфигурации

from TinkoffPy.grpc.marketdata_pb2 import GetCandlesRequest, CandleInterval
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.json_format import MessageToDict


def stream_bars(class_code, security_code, schedule, time_frame, delta):
    """Поток получения новых бар по расписанию биржи

    :param str class_code: Торговая площадка
    :param str security_code: Тикер
    :param MOEXStocks|MOEXFutures schedule: Расписание торгов
    :param timedelta time_frame: Временной интервал
    :param timedelta delta: Смещение в будущее, чтобы гарантированно получить сформированный бар
    """
    tp_provider = TinkoffPy(Config.Token)  # Провайдер Tinkoff
    if time_frame == timedelta(minutes=1):  # 1 минута
        tf = CandleInterval.CANDLE_INTERVAL_1_MIN
    elif time_frame == timedelta(minutes=2):  # 2 минуты
        tf = CandleInterval.CANDLE_INTERVAL_2_MIN
    elif time_frame == timedelta(minutes=3):  # 3 минуты
        tf = CandleInterval.CANDLE_INTERVAL_3_MIN
    elif time_frame == timedelta(minutes=5):  # 5 минут
        tf = CandleInterval.CANDLE_INTERVAL_5_MIN
    elif time_frame == timedelta(minutes=10):  # 10 минут
        tf = CandleInterval.CANDLE_INTERVAL_10_MIN
    elif time_frame == timedelta(minutes=15):  # 15 минут
        tf = CandleInterval.CANDLE_INTERVAL_15_MIN
    elif time_frame == timedelta(minutes=30):  # 30 минут
        tf = CandleInterval.CANDLE_INTERVAL_30_MIN
    elif time_frame == timedelta(hours=1):  # 1 час
        tf = CandleInterval.CANDLE_INTERVAL_HOUR
    elif time_frame == timedelta(hours=2):  # 2 часа
        tf = CandleInterval.CANDLE_INTERVAL_2_HOUR
    elif time_frame == timedelta(hours=4):  # 4 часа
        tf = CandleInterval.CANDLE_INTERVAL_4_HOUR
    elif time_frame == timedelta(days=1):  # 1 день
        tf = CandleInterval.CANDLE_INTERVAL_DAY
    elif time_frame == timedelta(weeks=1):  # 1 неделя
        tf = CandleInterval.CANDLE_INTERVAL_WEEK
    # Также есть интервал CandleInterval.CANDLE_INTERVAL_MONTH - 1 месяц
    else:  # Если временной интервал задан неверно
        print('Временной интервал задан неверно')
        return  # то выходим, дальше не продолжаем
    intraday = tf not in (CandleInterval.CANDLE_INTERVAL_DAY,
                          CandleInterval.CANDLE_INTERVAL_WEEK,
                          CandleInterval.CANDLE_INTERVAL_MONTH)  # Внутридневные интервалы = не дневные интервалы
    figi = tp_provider.get_symbol_info(class_code, security_code).figi  # Уникальный код тикера
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
            tp_provider.close_channel()  # Закрываем канал перед выходом
            return  # Выходим из потока, дальше не продолжаем

        trade_bar_open_datetime_utc = schedule.msk_to_utc_datetime(trade_bar_open_datetime)  # Дата и время бара в UTC
        seconds_from = schedule.msk_datetime_to_utc_timestamp(trade_bar_open_datetime)  # Дата и время бара в timestamp UTC
        date_from = Timestamp(seconds=seconds_from, nanos=trade_bar_open_datetime_utc.microsecond * 1_000)  # Дата и время бара в Google Timestamp UTC
        to = Timestamp(seconds=int(seconds_from + time_frame.total_seconds()), nanos=trade_bar_open_datetime_utc.microsecond * 1_000)  # Дата и время окончания бара в Google Timestamp UTC
        request = GetCandlesRequest(instrument_id=figi, to=to, interval=tf)  # Запрос на получение бар
        from_ = getattr(request, 'from')  # т.к. from - ключевое слово в Python, то получаем атрибут from из атрибута интервала
        from_.seconds = date_from.seconds
        from_.nanos = date_from.nanos
        bars = MessageToDict(tp_provider.call_function(tp_provider.stub_marketdata.GetCandles, request), including_default_value_fields=True)['candles']  # Получаем бары, переводим в словарь/список
        if len(bars) == 0:  # Если новых бар нет
            print('Бар не получен')
            continue  # Будем получать следующий бар

        bar = bars[0]  # Получаем первый (завершенный) бар
        # Дату/время UTC получаем в формате ISO 8601. Пример: 2023-06-16T20:01:00Z
        # В статье https://stackoverflow.com/questions/127803/how-do-i-parse-an-iso-8601-formatted-date описывается проблема, что Z на конце нужно убирать
        dt_utc = datetime.fromisoformat(bar['time'][:-1])  # Дата и время начала бара в UTC
        dt = schedule.utc_to_msk_datetime(dt_utc) if intraday else\
            datetime(dt_utc.year, dt_utc.month, dt_utc.day)  # Дату/время переводим из UTC в МСК
        open_ = int(bar['open']['units']) + int(bar['open']['nano']) / 10 ** 9
        high = int(bar['high']['units']) + int(bar['high']['nano']) / 10 ** 9
        low = int(bar['low']['units']) + int(bar['low']['nano']) / 10 ** 9
        close = int(bar['close']['units']) + int(bar['close']['nano']) / 10 ** 9
        volume = int(bar['volume'])
        print('Получен бар:', dt, 'Open =', open_, 'High =', high, 'Low =', low, 'Close =', close, 'Volume =', volume)


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    class_code = 'TQBR'  # Код площадки
    security_code = 'SBER'  # Тикер
    # class_code = 'SPBFUT'  # Код площадки
    # security_code = 'SiU3'  # Формат фьючерса: <Тикер><Месяц экспирации><Последняя цифра года> Месяц экспирации: 3-H, 6-M, 9-U, 12-Z
    schedule = MOEXStocks()  # Расписание фондового рынка Московской биржи
    # schedule = MOEXFutures()  # Расписание срочного рынка Московской биржи
    time_frame = timedelta(minutes=1)  # 1 минута
    # time_frame = timedelta(minutes=5)  # 5 минут
    # time_frame = timedelta(minutes=15)  # 15 минут
    # time_frame = timedelta(hours=1)  # 1 час
    # time_frame = timedelta(days=1)  # 1 день
    # time_frame = timedelta(weeks=1)  # 1 неделя
    delta = timedelta(seconds=5)  # Задержка перед запросом нового бара

    exit_event = Event()  # Определяем событие выхода из потока
    stream_bars_thread = Thread(name='stream_bars', target=stream_bars, args=(class_code, security_code, schedule, time_frame, delta))  # Создаем поток получения новых бар
    stream_bars_thread.start()  # Запускаем поток
    input()  # Ожидаем нажатия на клавишу Ввод (Enter)
    exit_event.set()  # Устанавливаем событие выхода из потока
