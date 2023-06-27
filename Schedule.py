from typing import Union  # Объединение типов
from datetime import datetime, timedelta, time

from pytz import timezone, utc  # Работаем с временнОй зоной и UTC


class Session:
    """Торговая сессия"""

    def __init__(self, time_begin: time, time_end: time):
        self.time_begin = time_begin  # Время начала сессии
        self.time_end = time_end  # Время окончания сессии


class Schedule:
    """Расписание торгов"""
    market_timezone = timezone('Europe/Moscow')  # ВременнАя зона работы биржи

    def __init__(self, trade_sessions: list[Session]):
        self.trade_sessions = sorted(trade_sessions, key=lambda session: session.time_begin)  # Список торговых сессий сортируем по возрастанию времени начала сессии

    def get_trade_session(self, dt_market: datetime) -> Union[Session, None]:
        """Торговая сессия по дате и времени на бирже

        :param datetime dt_market: Дата и время на бирже
        :return: Дата и время на бирже. None, если торги не идут
        """
        if dt_market.weekday() in (5, 6):  # Если задан выходной день
            return None  # То торги не идут, торговой сессии нет
        t_market = dt_market.time()  # Время на бирже
        for session in self.trade_sessions:  # Пробегаемся по всем торговым сессиям
            if session.time_begin <= t_market <= session.time_end:  # Если время внутри сессии
                return session  # Возвращаем найденную торговую сессию
        return None  # Если время попадает в клиринг/перерыв, то торговой сессии нет

    def time_until_trade(self, dt_market: datetime) -> timedelta:
        """Время, через которое можжно будет торговать

        :param datetime dt_market: Дата и время на бирже
        :return: Время, через которое можжно будет торговать. 0 секунд, если торговать можно прямо сейчас
        """
        session = self.get_trade_session(dt_market)  # Пробуем получить торговую сессию
        if session:  # Если нашли торговую сессию
            return timedelta()  # То ждать не нужно, торговать можно прямо сейчас
        for s in self.trade_sessions:  # Пробегаемся по всем торговым сессиям
            if s.time_begin > dt_market.time():  # Если сессия начинается позже текущего времени на бирже
                session = s  # То это искомая сессия
                break  # Сессию нашли, дальше поиск вести не нужно
        d_market = dt_market.date()  # Дата на бирже
        if not session:  # Сессия не найдена, если время позже окончания последней сессии
            session = self.trade_sessions[0]  # Будет первая торговая сессия
            d_market += timedelta(1)  # Следующего дня
        w_market = d_market.weekday()  # День недели даты на бирже
        if w_market in (5, 6):  # Если биржа на выходных не работает, и задан выходной день
            d_market += timedelta(7 - w_market)  # То будем ждать первой торговой сессии понедельника
        dt_next_session = datetime(d_market.year, d_market.month, d_market.day, session.time_begin.hour, session.time_begin.minute, session.time_begin.second)
        return dt_next_session - dt_market

    def get_trade_bar_datetime(self, market_datetime, time_frame):
        """Дата и время открытия бара на бирже. Для сессии последний открытый бар. В перерывах - следующий бар

        :param datetime market_datetime: Дата и время на бирже
        :param timedelta time_frame: Временной интервал
        :return: Дата и время открытия бара на бирже
        """
        dt = market_datetime.replace(microsecond=0)  # Время без микросекунд
        session = self.get_trade_session(dt)  # Пробуем получить торговую сессию
        return dt + timedelta(seconds=-int(dt.timestamp() % time_frame.seconds) if session else self.time_until_trade(dt).seconds)

    def utc_to_msk_datetime(self, dt, tzinfo=False) -> datetime:
        """Перевод времени из UTC в московское

        :param datetime dt: Время UTC
        :param bool tzinfo: Отображать временнУю зону
        :return: Московское время
        """
        dt_utc = utc.localize(dt)  # Задаем временнУю зону UTC
        dt_msk = dt_utc.astimezone(self.market_timezone)  # Переводим в МСК
        return dt_msk if tzinfo else dt_msk.replace(tzinfo=None)

    def msk_to_utc_datetime(self, dt, tzinfo=False) -> datetime:
        """Перевод времени из московского в UTC

        :param datetime dt: Московское время
        :param bool tzinfo: Отображать временнУю зону
        :return: Время UTC
        """
        dt_msk = self.market_timezone.localize(dt)  # Задаем временнУю зону МСК
        dt_utc = dt_msk.astimezone(utc)  # Переводим в UTC
        return dt_utc if tzinfo else dt_utc.replace(tzinfo=None)

    def utc_timestamp_to_msk_datetime(self, seconds) -> datetime:
        """Перевод кол-ва секунд, прошедших с 01.01.1970 00:00 UTC в московское время

        :param int seconds: Кол-во секунд, прошедших с 01.01.1970 00:00 UTC
        :return: Московское время без временнОй зоны
        """
        dt_utc = datetime.utcfromtimestamp(seconds)  # Переводим кол-во секунд, прошедших с 01.01.1970 в UTC
        return self.utc_to_msk_datetime(dt_utc)  # Переводим время из UTC в московское

    def msk_datetime_to_utc_timestamp(self, dt) -> int:
        """Перевод московского времени в кол-во секунд, прошедших с 01.01.1970 00:00 UTC

        :param datetime dt: Московское время
        :return: Кол-во секунд, прошедших с 01.01.1970 00:00 UTC
        """
        dt_msk = self.market_timezone.localize(dt)  # Заданное время ставим в зону МСК
        return int(dt_msk.timestamp())  # Переводим в кол-во секунд, прошедших с 01.01.1970 в UTC


class MOEXStocks(Schedule):
    """Расписание торгов Московской биржи: Фондовый рынок"""

    def __init__(self):
        super(MOEXStocks, self).__init__(
            [Session(time(10, 0, 0), time(18, 39, 59)),  # Основная торговая сессия
             Session(time(19, 5, 0), time(23, 49, 59))])  # Вечерняя торговая сессия


class MOEXFutures(Schedule):
    """Расписание торгов Московской биржи: Срочный рынок"""

    def __init__(self):
        super(MOEXFutures, self).__init__(
            [Session(time(9, 0, 0), time(9, 59, 59)),  # Утренняя дополнительная торговая сессия
             Session(time(10, 0, 0), time(13, 59, 59)),  # Основная торговая сессия (Дневной расчетный период)
             Session(time(14, 5, 0), time(18, 49, 59)),  # Основная торговая сессия (Вечерний расчетный период)
             Session(time(19, 5, 0), time(23, 49, 59))])  # Вечерняя дополнительная торговая сессия
