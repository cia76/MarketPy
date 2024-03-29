import logging
from datetime import datetime
from threading import Thread, Event

from MarketPy.Schedule import Schedule, MOEXStocks, MOEXFutures

from FinamPy import FinamPy  # Работа с сервером TRANSAQ
from FinamPy.Config import Config  # Файл конфигурации

from FinamPy.proto.tradeapi.v1.candles_pb2 import DayCandleInterval, IntradayCandleInterval
from google.type.date_pb2 import Date
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.json_format import MessageToDict


logger = logging.getLogger('Schedule.StreamBarsFinam')  # Будем вести лог


# noinspection PyShadowingNames
def stream_bars(board, code, schedule, tf):
    """Поток получения новых бар по расписанию биржи

    :param str board: Код режима торгов
    :param str code: Тикер
    :param Schedule schedule: Расписание торгов
    :param str tf: Временной интервал https://ru.wikipedia.org/wiki/Таймфрейм
    """
    fp_provider = FinamPy(Config.AccessToken)  # Провайдер Finam
    tf_finam, intraday = fp_provider.timeframe_to_finam_timeframe(tf)  # Временной интервал Финам, внутридневной интервал
    interval = IntradayCandleInterval(count=1) if intraday else DayCandleInterval(count=1)  # Принимаем последний завершенный бар
    while True:
        market_datetime_now = schedule.utc_to_msk_datetime(datetime.utcnow())  # Текущее время на бирже
        logger.debug(f'Текущее время на бирже: {market_datetime_now.strftime("%d.%m.%Y %H:%M:%S")}')
        trade_bar_open_datetime = schedule.trade_bar_open_datetime(market_datetime_now, tf)  # Дата и время открытия бара, который будем получать
        logger.debug(f'Нужно получить бар: {trade_bar_open_datetime.strftime("%d.%m.%Y %H:%M:%S")}')
        trade_bar_request_datetime = schedule.trade_bar_request_datetime(market_datetime_now, tf)  # Дата и время запроса бара на бирже
        logger.debug(f'Время запроса бара: {trade_bar_request_datetime.strftime("%d.%m.%Y %H:%M:%S")}')
        sleep_time_secs = (trade_bar_request_datetime - market_datetime_now).total_seconds()  # Время ожидания в секундах
        logger.debug(f'Ожидание в секундах: {sleep_time_secs}')
        exit_event_set = exit_event.wait(sleep_time_secs)  # Ждем нового бара или события выхода из потока
        if exit_event_set:  # Если произошло событие выхода из потока
            fp_provider.close_channel()  # Закрываем канал перед выходом
            return  # Выходим из потока, дальше не продолжаем

        trade_bar_open_datetime_utc = schedule.msk_to_utc_datetime(trade_bar_open_datetime)  # Дата и время бара в UTC
        from_ = getattr(interval, 'from')  # т.к. from - ключевое слово в Python, то получаем атрибут from из атрибута интервала
        if intraday:  # Для интрадея datetime -> Timestamp
            seconds_from = schedule.msk_datetime_to_utc_timestamp(trade_bar_open_datetime)  # Дата и время открытия бара в timestamp UTC
            date_from = Timestamp(seconds=seconds_from)  # Дата и время открытия бара в Google Timestamp UTC
            from_.seconds = date_from.seconds
        else:  # Для дневных интервалов и выше datetime -> Date
            date_from = Date(year=trade_bar_open_datetime_utc.year, month=trade_bar_open_datetime_utc.month, day=trade_bar_open_datetime_utc.day)
            from_.year = date_from.year
            from_.month = date_from.month
            from_.day = date_from.day
        bars = MessageToDict(fp_provider.get_intraday_candles(board, code, tf_finam, interval) if intraday else
                             fp_provider.get_day_candles(board, code, tf_finam, interval),
                             including_default_value_fields=True)  # Получаем ответ на запрос истории рынка
        if not bars:  # Если ничего не получили
            logger.warning('Данные не получены')
            continue  # Будем получать следующий бар
        logger.debug(f'Получены данные {bars}')
        bars = bars['candles']
        if len(bars) == 0:  # Если бары не получены
            logger.warning('Бар не получен')
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
        volume = int(bar['volume'])
        logger.info(f'Получен бар: {board}.{code} ({tf}/{tf_finam}) - {dt.strftime("%d.%m.%Y %H:%M:%S")} - Open = {open_}, High = {high}, Low = {low}, Close = {close}, Volume = {volume}')


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    board = 'TQBR'  # Акции ММВБ
    code = 'SBER'  # Тикер
    schedule = MOEXStocks()  # Расписание фондового рынка Московской Биржи
    # board = 'FUT'  # Фьючерсы
    # code = 'SiH4'  # Формат фьючерса: <Тикер><Месяц экспирации><Последняя цифра года> Месяц экспирации: 3-H, 6-M, 9-U, 12-Z
    # schedule = MOEXFutures()  # Расписание срочного рынка Московской Биржи
    tf = 'M1'  # 1 минута
    # tf = 'M5'  # 5 минут
    # tf = 'M15'  # 15 минут
    # tf = 'M60'  # 1 час
    # tf = 'D1'  # 1 день
    # tf = 'W1'  # 1 неделя

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.DEBUG,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('StreamBarsFinam.log'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=Schedule.market_timezone).timetuple()  # В логе время указываем по временнОй зоне расписания (МСК)

    print('\nEnter - выход')
    exit_event = Event()  # Определяем событие выхода из потока
    stream_bars_thread = Thread(name='schedule_bars_finam', target=stream_bars, args=(board, code, schedule, tf))  # Создаем поток получения новых бар
    stream_bars_thread.start()  # Запускаем поток
    input()  # Ожидаем нажатия на клавиши Ввод (Enter)
    exit_event.set()  # Устанавливаем событие выхода из потока
