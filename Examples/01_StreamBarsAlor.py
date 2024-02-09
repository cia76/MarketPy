from datetime import datetime
import logging
from threading import Thread, Event

from MarketPy.Schedule import Schedule, MOEXStocks, MOEXFutures

from AlorPy import AlorPy  # Работа с Alor OpenAPI V2
from AlorPy.Config import Config  # Файл конфигурации


logger = logging.getLogger('Schedule.StreamBarsAlor')  # Будем вести лог


# noinspection PyShadowingNames
def stream_bars(board, symbol, schedule, tf):
    """Поток получения новых бар по расписанию биржи

    :param str board: Код режима торгов
    :param str symbol: Тикер
    :param Schedule schedule: Расписание торгов
    :param str tf: Временной интервал https://ru.wikipedia.org/wiki/Таймфрейм
    """
    ap_provider = AlorPy(Config.UserName, Config.RefreshToken)  # Провайдер Alor
    tf_alor, _ = ap_provider.timeframe_to_alor_timeframe(tf)  # Временной интервал Алор
    exchange = ap_provider.get_exchange(board, symbol)  # Биржа, где торгуется тикер
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
            ap_provider.close_web_socket()  # Перед выходом закрываем соединение с WebSocket
            return  # Выходим из потока, дальше не продолжаем

        seconds_from = schedule.msk_datetime_to_utc_timestamp(trade_bar_open_datetime)  # Дата и время бара в timestamp UTC
        bars = ap_provider.get_history(exchange, symbol, tf_alor, seconds_from)  # Получаем ответ на запрос истории рынка
        if not bars:  # Если ничего не получили
            logger.warning('Данные не получены')
            continue  # Будем получать следующий бар
        logger.debug(f'Получены данные {bars}')
        bars = bars['history']  # Последний сформированный и текущий несформированный (если имеется) бары
        if len(bars) == 0:  # Если бары не получены
            logger.warning('Бар не получен')
            continue  # Будем получать следующий бар

        bar = bars[0]  # Получаем первый (завершенный) бар
        dt = schedule.utc_timestamp_to_msk_datetime(int(bar['time']))
        open_ = float(bar['open'])
        high = float(bar['high'])
        low = float(bar['low'])
        close = float(bar['close'])
        volume = int(bar['volume'])
        logger.info(f'Получен бар: {board}.{symbol} ({tf}/{tf_alor}) - {dt.strftime("%d.%m.%Y %H:%M:%S")} - Open = {open_}, High = {high}, Low = {low}, Close = {close}, Volume = {volume}')


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    board = 'TQBR'  # Акции ММВБ
    symbol = 'SBER'  # Тикер
    schedule = MOEXStocks()  # Расписание фондового рынка Московской Биржи
    # board = 'SPBFUT'  # Фьючерсы
    # symbol = 'SiH4'  # Формат фьючерса: <Тикер><Месяц экспирации><Последняя цифра года> Месяц экспирации: 3-H, 6-M, 9-U, 12-Z
    # schedule = MOEXFutures()  # Расписание срочного рынка Московской Биржи
    tf = 'M1'  # 1 минута
    # tf = 'M5'  # 5 минут
    # tf = 'M15'  # 15 минут
    # tf = 'M60'  # 1 час
    # tf = 'D'  # 1 день

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.DEBUG,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('StreamBarsAlor.log'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=Schedule.market_timezone).timetuple()  # В логе время указываем по временнОй зоне расписания (МСК)
    logging.getLogger('urllib3').setLevel(logging.CRITICAL + 1)

    print('\nEnter - выход')
    exit_event = Event()  # Определяем событие выхода из потока
    stream_bars_thread = Thread(name='schedule_bars_alor', target=stream_bars, args=(board, symbol, schedule, tf))  # Создаем поток получения новых бар
    stream_bars_thread.start()  # Запускаем поток
    input()  # Ожидаем нажатия на клавишу Ввод (Enter)
    exit_event.set()  # Устанавливаем событие выхода из потока
