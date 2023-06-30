from threading import Thread, Event
from datetime import datetime, timedelta

from MarketPy.Schedule import MOEXStocks, MOEXFutures

from FinamPy import FinamPy  # Работа с сервером TRANSAQ
from FinamPy.Config import Config  # Файл конфигурации

from FinamPy.proto.tradeapi.v1.candles_pb2 import DayCandleTimeFrame, DayCandleInterval, IntradayCandleTimeFrame, IntradayCandleInterval
from google.type.date_pb2 import Date
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.json_format import MessageToDict


def stream_bars(board, code, schedule, time_frame, delta):
    fp_provider = FinamPy(Config.AccessToken)  # Провайдер Finam
    if time_frame == timedelta(minutes=1):  # 1 минута
        tf = IntradayCandleTimeFrame.INTRADAYCANDLE_TIMEFRAME_M1
    elif time_frame == timedelta(minutes=5):  # 5 минут
        tf = IntradayCandleTimeFrame.INTRADAYCANDLE_TIMEFRAME_M5
    elif time_frame == timedelta(minutes=15):  # 15 минут
        tf = IntradayCandleTimeFrame.INTRADAYCANDLE_TIMEFRAME_M15
    elif time_frame == timedelta(hours=1):  # 1 час
        tf = IntradayCandleTimeFrame.INTRADAYCANDLE_TIMEFRAME_H1
    elif time_frame == timedelta(days=1):  # 1 день
        tf = DayCandleTimeFrame.DAYCANDLE_TIMEFRAME_D1
    elif time_frame == timedelta(weeks=1):  # 1 неделя
        tf = DayCandleTimeFrame.DAYCANDLE_TIMEFRAME_W1
    else:  # Если временной интервал задан неверно
        print('Временной интервал задан неверно')
        return  # то выходим, дальше не продолжаем
    intraday = tf in (IntradayCandleTimeFrame.INTRADAYCANDLE_TIMEFRAME_M1,
                      IntradayCandleTimeFrame.INTRADAYCANDLE_TIMEFRAME_M5,
                      IntradayCandleTimeFrame.INTRADAYCANDLE_TIMEFRAME_M15,
                      IntradayCandleTimeFrame.INTRADAYCANDLE_TIMEFRAME_H1)  # Внутридневные интервалы
    interval = IntradayCandleInterval(count=1) if intraday else DayCandleInterval(count=1)  # Принимаем последний завершенный бар
    while True:
        market_datetime_now = schedule.utc_to_msk_datetime(datetime.utcnow())  # Текущее время на бирже
        print('\nТекущее время на бирже:', market_datetime_now)
        trade_bar_open_datetime = schedule.get_trade_bar_open_datetime(market_datetime_now, time_frame)  # Дата и время бара, который будем получать
        print('Будем получать бар:', trade_bar_open_datetime, 'UTC+03')
        trade_bar_request_datetime = schedule.get_trade_bar_request_datetime(trade_bar_open_datetime, time_frame)  # Дата и время запроса бара на бирже
        print('Время запроса бара:', trade_bar_request_datetime)
        sleep_time_secs = (trade_bar_request_datetime - market_datetime_now + delta).total_seconds()  # Время ожидания в секундах
        print('Одижание в секундах:', sleep_time_secs)
        exit_event_set = exit_event.wait(sleep_time_secs)  # Ждем нового бара или события выхода из потока
        if exit_event_set:  # Если произошло событие выхода из потока
            fp_provider.close_channel()  # Закрываем канал перед выходом
            return  # Выходим из потока, дальше не продолжаем

        trade_bar_open_datetime_utc = schedule.msk_to_utc_datetime(trade_bar_open_datetime)  # Дата и время бара в UTC
        from_ = getattr(interval, 'from')  # т.к. from - ключевое слово в Python, то получаем атрибут from из атрибута интервала
        if intraday:  # Для интрадея datetime -> Timestamp
            seconds_from = schedule.msk_datetime_to_utc_timestamp(trade_bar_open_datetime)  # Дата и время бара в timestamp UTC
            date_from = Timestamp(seconds=seconds_from, nanos=trade_bar_open_datetime_utc.microsecond * 1_000)
            from_.seconds = date_from.seconds
            from_.nanos = date_from.nanos
        else:  # Для дневных интервалов и выше datetime -> Date
            date_from = Date(year=trade_bar_open_datetime_utc.year, month=trade_bar_open_datetime_utc.month, day=trade_bar_open_datetime_utc.day)
            from_.year = date_from.year
            from_.month = date_from.month
            from_.day = date_from.day
        bars = MessageToDict(fp_provider.get_intraday_candles(board, code, tf, interval) if intraday else
                             fp_provider.get_day_candles(board, code, tf, interval),
                             including_default_value_fields=True)['candles']  # Получаем бары, переводим в словарь/список
        if len(bars) == 0:  # Если новых бар нет
            print('Бар не получен')
            continue  # Будем получать следующий бар

        bar = bars[0]  # Получаем первый (завершенный) бар
        # Дату/время UTC получаем в формате ISO 8601. Пример: 2023-06-16T20:01:00Z
        # В статье https://stackoverflow.com/questions/127803/how-do-i-parse-an-iso-8601-formatted-date описывается проблема, что Z на конце нужно убирать
        dt = fp_provider.utc_to_msk_datetime(datetime.fromisoformat(bar['timestamp'][:-1])) if intraday else \
            datetime(bar['date']['year'], bar['date']['month'], bar['date']['day'])  # Дату/время переводим из UTC в МСК
        open_ = round(int(bar['open']['num']) * 10 ** -int(bar['open']['scale']), int(bar['open']['scale']))
        high = round(int(bar['high']['num']) * 10 ** -int(bar['high']['scale']), int(bar['high']['scale']))
        low = round(int(bar['low']['num']) * 10 ** -int(bar['low']['scale']), int(bar['low']['scale']))
        close = round(int(bar['close']['num']) * 10 ** -int(bar['close']['scale']), int(bar['close']['scale']))
        volume = bar['volume']
        print('Получен бар:', dt, 'Open =', open_, 'High =', high, 'Low =', low, 'Close =', close, 'Volume =', volume)


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    # board = 'TQBR'  # Код площадки
    # code = 'SBER'  # Тикер
    board = 'FUT'  # Код площадки
    code = 'SiU3'  # Формат фьючерса: <Тикер><Месяц экспирации><Последняя цифра года> Месяц экспирации: 3-H, 6-M, 9-U, 12-Z
    # schedule = MOEXStocks()  # Расписание фондового рынка Московской биржи
    schedule = MOEXFutures()  # Расписание срочного рынка Московской биржи
    time_frame = timedelta(minutes=1)  # 1 минута
    # time_frame = timedelta(minutes=5)  # 5 минут
    # time_frame = timedelta(minutes=15)  # 15 минут
    # time_frame = timedelta(hours=1)  # 1 час
    # time_frame = timedelta(days=1)  # 1 день
    # time_frame = timedelta(weeks=1)  # 1 неделя
    delta = timedelta(seconds=3)  # Задержка перед запросом нового бара

    exit_event = Event()  # Определяем событие выхода из потока
    stream_bars_thread = Thread(name='stream_bars', target=stream_bars, args=(board, code, schedule, time_frame, delta))  # Создаем поток получения новых бар
    stream_bars_thread.start()  # Запускаем поток
    input()  # Ожидаем нажатия на клавиши Ввод (Enter)
    exit_event.set()  # Устанавливаем событие выхода из потока
