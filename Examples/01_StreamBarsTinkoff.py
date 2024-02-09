import logging
from datetime import datetime, timedelta
from threading import Thread, Event

from MarketPy.Schedule import Schedule, MOEXStocks, MOEXFutures

from TinkoffPy import TinkoffPy  # Работа с Tinkoff Invest API из Python
from TinkoffPy.Config import Config  # Файл конфигурации

from TinkoffPy.grpc.marketdata_pb2 import GetCandlesRequest
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.json_format import MessageToDict


logger = logging.getLogger('Schedule.StreamBarsTinkoff')  # Будем вести лог


# noinspection PyShadowingNames
def stream_bars(class_code, security_code, schedule, tf):
    """Поток получения новых бар по расписанию биржи

    :param str class_code: Код режима торгов
    :param str security_code: Тикер
    :param Schedule schedule: Расписание торгов
    :param str tf: Временной интервал https://ru.wikipedia.org/wiki/Таймфрейм
    """
    tp_provider = TinkoffPy(Config.Token)  # Провайдер Tinkoff
    tf_tinkoff, intraday = tp_provider.timeframe_to_tinkoff_timeframe(tf)  # Временной интервал Финам, внутридневной интервал
    figi = tp_provider.get_symbol_info(class_code, security_code).figi  # Уникальный код тикера
    while True:
        market_datetime_now = schedule.utc_to_msk_datetime(datetime.utcnow())  # Текущее время на бирже
        logger.debug(f'Текущее время на бирже: {market_datetime_now.strftime("%d.%m.%Y %H:%M:%S")}')
        trade_bar_open_datetime = schedule.trade_bar_open_datetime(market_datetime_now, tf)  # Дата и время открытия бара, который будем получать
        logger.debug(f'Нужно получить бар: {trade_bar_open_datetime.strftime("%d.%m.%Y %H:%M:%S")}')
        trade_bar_request_datetime = schedule.trade_bar_request_datetime(trade_bar_open_datetime, tf)  # Дата и время запроса бара на бирже
        logger.debug(f'Время запроса бара: {trade_bar_request_datetime.strftime("%d.%m.%Y %H:%M:%S")}')
        sleep_time_secs = (trade_bar_request_datetime - market_datetime_now).total_seconds()  # Время ожидания в секундах
        logger.debug(f'Ожидание в секундах: {sleep_time_secs}')
        exit_event_set = exit_event.wait(sleep_time_secs)  # Ждем нового бара или события выхода из потока
        if exit_event_set:  # Если произошло событие выхода из потока
            tp_provider.close_channel()  # Закрываем канал перед выходом
            return  # Выходим из потока, дальше не продолжаем

        ts_from = Timestamp(seconds=schedule.msk_datetime_to_utc_timestamp(trade_bar_open_datetime))  # Дата и время открытия бара в Google Timestamp UTC
        trade_bar_close_datetime = schedule.trade_bar_close_datetime(market_datetime_now, tf)  # Дата и время закрытия бара, который будем получать
        ts_to = Timestamp(seconds=schedule.msk_datetime_to_utc_timestamp(trade_bar_close_datetime))  # Дата и время закрытия бара в Google Timestamp UTC
        request = GetCandlesRequest(instrument_id=figi, to=ts_to, interval=tf_tinkoff)  # Запрос на получение бар
        from_ = getattr(request, 'from')  # т.к. from - ключевое слово в Python, то получаем атрибут from из атрибута интервала
        from_.seconds = ts_from.seconds
        bars = MessageToDict(tp_provider.call_function(tp_provider.stub_marketdata.GetCandles, request), including_default_value_fields=True)  # Получаем бары, переводим в словарь/список
        if not bars:  # Если ничего не получили
            logger.warning('Данные не получены')
            continue  # Будем получать следующий бар
        logger.debug(f'Получены данные {bars}')
        bars = bars['candles']
        if len(bars) == 0:  # Если новых бар нет
            logger.warning('Бар не получен')
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
        logger.info(f'Получен бар: {class_code}.{security_code} ({tf}/{tf_tinkoff}) - {dt.strftime("%d.%m.%Y %H:%M:%S")} - Open = {open_}, High = {high}, Low = {low}, Close = {close}, Volume = {volume}')


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    class_code = 'TQBR'  # Акции ММВБ
    security_code = 'SBER'  # Тикер
    schedule = MOEXStocks()  # Расписание фондового рынка Московской Биржи
    # class_code = 'SPBFUT'  # Фьючерсы
    # security_code = 'SiH4'  # Формат фьючерса: <Тикер><Месяц экспирации><Последняя цифра года> Месяц экспирации: 3-H, 6-M, 9-U, 12-Z
    # schedule = MOEXFutures()  # Расписание срочного рынка Московской Биржи
    schedule.delta = timedelta(seconds=10)  # Даже при увеличенной задержке во времени при получения бара, 18% получены не будут
    tf = 'M1'  # 1 минута
    # tf = 'M5'  # 5 минут
    # tf = 'M15'  # 15 минут
    # tf = 'M60'  # 1 час
    # tf = 'D1'  # 1 день
    # tf = 'W1'  # 1 неделя

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.DEBUG,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('StreamBarsTinkoff.log'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=Schedule.market_timezone).timetuple()  # В логе время указываем по временнОй зоне расписания (МСК)

    print('\nEnter - выход')
    exit_event = Event()  # Определяем событие выхода из потока
    stream_bars_thread = Thread(name='stream_bars', target=stream_bars, args=(class_code, security_code, schedule, tf))  # Создаем поток получения новых бар
    stream_bars_thread.start()  # Запускаем поток
    input()  # Ожидаем нажатия на клавишу Ввод (Enter)
    exit_event.set()  # Устанавливаем событие выхода из потока
