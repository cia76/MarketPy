from typing import Tuple, Union  # Кортеж, объединение типов
from datetime import datetime, timedelta, time

from pytz import timezone, utc  # Работаем с временнОй зоной и UTC


class Session:
    """Торговая сессия"""

    def __init__(self, time_begin, time_end):
        """
        :param time time_begin: Время начала сессии
        :param time time_end: Время окончания сессии
        """
        self.time_begin = time_begin  # Время начала сессии
        self.time_end = time_end  # Время окончания сессии


class Schedule:
    """Расписание торгов"""
    market_timezone = timezone('Europe/Moscow')  # ВременнАя зона работы биржи

    def __init__(self, trade_sessions, delta):
        """
        :param list[Session] trade_sessions: Список торговых сессий
        :param timedelta delta: Задержка, чтобы гарантированно получить сформированный бар
        """
        self.trade_sessions = sorted(trade_sessions, key=lambda session: session.time_begin)  # Список торговых сессий сортируем по возрастанию времени начала сессии
        self.delta = delta  # Задержка, чтобы гарантированно получить сформированный бар

    def trade_session(self, dt_market) -> Union[Session, None]:
        """Торговая сессия по дате и времени на бирже. None, если торги не идут

        :param datetime dt_market: Дата и время на бирже
        :return: Дата и время на бирже. None, если торги не идут
        """
        if dt_market.weekday() in (5, 6):  # Если задан выходной день (суббота или воскресенье)
            return None  # То торги не идут, торговой сессии нет
        t_market = dt_market.time()  # Время на бирже
        for session in self.trade_sessions:  # Пробегаемся по всем торговым сессиям
            if session.time_begin <= t_market <= session.time_end:  # Если время внутри сессии
                return session  # Возвращаем найденную торговую сессию
        return None  # Если время попадает в клиринг/перерыв, то торговой сессии нет

    def last_session_time_end(self, dt_market) -> datetime:
        """Дата и время окончания предыдущей торговой сессии по дате и времени на бирже

        :param datetime dt_market: Дата и время на бирже
        :return: Дата и время окончания предыдущей торговой сессии
        """
        if dt_market.weekday() in (5, 6):  # Если выходной день (суббота или воскресенье)
            return datetime.combine((dt_market - timedelta(days=dt_market.weekday()-4)).date(), self.trade_sessions[-1].time_end)  # то окончание последней сессии пятницы
        t_market = dt_market.time()  # Время на бирже
        if dt_market.weekday() == 0 and t_market < self.trade_sessions[0].time_begin:  # Если утро понедельника до начала торгов
            return datetime.combine((dt_market - timedelta(days=3)).date(), self.trade_sessions[-1].time_end)  # то окончание последней сессии пятницы
        i = -1  # Номер сессии, до которой не дошло время на бирже
        for session in self.trade_sessions:  # Пробегаемся по всем торговым сессиям
            if t_market < session.time_end:  # Если время на бирже не дошло до времени начала сессии
                break  # то сессия найдена, выходим, больше не ищем
            i += 1  # До этой сессии время на бирже дошло, переходим к следующей сессии
        if i == -1:  # Если последняя торговая сессия была вчера
            return datetime.combine((dt_market - timedelta(days=1)).date(), self.trade_sessions[-1].time_end)  # то окончание последней сессии вчера
        return datetime.combine(dt_market.date(), self.trade_sessions[i].time_end)  # Окончание последней сессии сегодня

    def time_until_trade(self, dt_market: datetime) -> timedelta:
        """Время, через которое можно будет торговать

        :param datetime dt_market: Дата и время на бирже
        :return: Время, через которое можжно будет торговать. 0 секунд, если торговать можно прямо сейчас
        """
        session = self.trade_session(dt_market)  # Пробуем получить торговую сессию
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

    def trade_bar_open_datetime(self, dt_market, tf) -> datetime:
        """Дата и время открытия последнего закрытого или открытого бара по дате и времени на бирже с временнЫм интервалом

        :param datetime dt_market: Дата и время на бирже
        :param str tf: Временной интервал https://ru.wikipedia.org/wiki/Таймфрейм
        :return: Дата и время открытия последнего закрытого или открытого бара
        """
        tf_timeframe, tf_compression, _ = self.parse_tf(tf)  # Разбираем временной интервал на период и размер
        if tf_timeframe == 'Y':  # Годовой временной интервал
            return datetime(dt_market.year, 1, 1)  # 1 января
        if tf_timeframe == 'MN':  # Месячный временной интервал
            return datetime(dt_market.year, dt_market.month, 1)  # 1 число месяца
        if tf_timeframe == 'W':  # Недельный временной интервал
            market_date = datetime.combine(dt_market.date(), datetime.min.time())  # Дата на бирже без времени
            return market_date - timedelta(days=market_date.weekday())  # Вычитаем кол-во дней, прошедших с пн. Крайний понедельник
        session = self.trade_session(dt_market)  # Пробуем получить торговую сессию
        if not session:  # Если на заданные дату и время на бирже перерыв
            dt_market = self.last_session_time_end(dt_market)  # то смещаем их на дату и время окончания последней торговой сессии
            session = self.trade_session(dt_market)  # Получаем эту сессию
        if tf_timeframe == 'D':  # Дневной временной интервал
            return datetime.combine(dt_market.date(), datetime.min.time())  # Сегодняшняя дата или предыдущей торговой сессии
        if tf_timeframe == 'M':  # Минутный временной интервал
            dt_session_begin = datetime.combine(dt_market.date(), session.time_begin)  # Дата и время начала торговой сессии
            session_seconds = (dt_market - dt_session_begin).total_seconds()  # Время от начала сессии в секундах
            bars_count = session_seconds // (tf_compression * 60)  # Кол-во баров с заданным интервалом от начала сессии
            return dt_session_begin + timedelta(minutes=tf_compression * bars_count)  # Смещаем на начало последнего бара
        raise NotImplementedError  # С часовым графиком H не работаем. Заменяем минутным. Пример: H1 = M60

    def trade_bar_close_datetime(self, dt_market, tf) -> datetime:
        """Дата и время закрытия последнего закрытого или открытого бара по дате и времени на бирже с временнЫм интервалом

        :param datetime dt_market: Дата и время на бирже
        :param str tf: Временной интервал https://ru.wikipedia.org/wiki/Таймфрейм
        :return: Дата и время открытия последнего закрытого или открытого бара
        """
        tf_timeframe, tf_compression, _ = self.parse_tf(tf)  # Разбираем временной интервал на период и размер
        if tf_timeframe == 'Y':  # Годовой временной интервал
            return datetime(dt_market.year + 1, 1, 1)  # 1 января следующего года
        if tf_timeframe == 'MN':  # Месячный временной интервал
            year = dt_market.year + dt_market.month // 12  # Год
            month = dt_market.month % 12 + 1  # Месяц
            return datetime(year, month, 1)  # 1 число следующего месяца
        if tf_timeframe == 'W':  # Недельный временной интервал
            market_date = datetime.combine(dt_market.date(), datetime.min.time())  # Дата на бирже без времени
            return market_date + timedelta(weeks=1, days=-market_date.weekday())  # Добавляем неделю. Вычитаем кол-во дней, прошедших с пн. Следующий понедельник
        session = self.trade_session(dt_market)  # Пробуем получить торговую сессию
        if not session:  # Если на заданные дату и время на бирже перерыв
            dt_market = self.last_session_time_end(dt_market)  # то смещаем их на дату и время окончания последней торговой сессии
            session = self.trade_session(dt_market)  # Получаем эту сессию
        if tf_timeframe == 'D':  # Дневной временной интервал
            return datetime.combine((dt_market + timedelta(days=1)).date(), datetime.min.time())  # Завтрашняя дата сегодняшней или следующей за предыдущей торговой сессией
        if tf_timeframe == 'M':  # Минутный временной интервал
            dt_session_begin = datetime.combine(dt_market.date(), session.time_begin)  # Дата и время начала торговой сессии
            session_seconds = (dt_market - dt_session_begin).total_seconds()  # Время от начала сессии в секундах
            bars_count = session_seconds // (tf_compression * 60)  # Кол-во баров с заданным интервалом от начала сессии
            return dt_session_begin + timedelta(minutes=tf_compression * (bars_count + 1))  # Смещаем на конец последнего бара
        raise NotImplementedError  # С часовым графиком H не работаем. Заменяем минутным. Пример: H1 = M60

    def trade_bar_request_datetime(self, dt_open, tf) -> datetime:
        """Дата и время запроса бара на бирже. Если идет торговая сессия, то на открытии следующего бара. В перерывах - в начале следующей сессии

        :param datetime dt_open: Дата и время открытия бара
        :param str tf: Временной интервал https://ru.wikipedia.org/wiki/Таймфрейм
        :return: Дата и время запроса бара на бирже
        """
        dt = self.trade_bar_close_datetime(dt_open, tf)  # По дате открытия бара получаем дату закрытия
        return dt + timedelta(seconds=self.time_until_trade(dt).total_seconds()) + self.delta  # Если дата закрытия попадает в перерыве, то добавляем время до начала следующей сессии. Добавляем задержку

    @staticmethod
    def parse_tf(tf) -> Tuple[str, int, bool]:
        """Разбор временнОго интервала на период, размер, является ли внутридневным интервалом

        :param str tf: Временной интервал https://ru.wikipedia.org/wiki/Таймфрейм
        :return: Период и размер временнОго интрервала
        """
        if 'MN' in tf:  # Сначала разбираем месяц, т.к. если начать с минут M, то месяц MN также разберется как минуты
            return tf[0:2], int(tf[2:]), False  # В периоде будет 2 символа. Интервал переводим в целое. Не внутридневной интервал
        return tf[0], int(tf[1:]), tf[0] == 'M'  # В остальных случаях в периоде будет 1 символ. Интервал переводим в целое. Минутный интервал внутридневной. Остальные - нет

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
    """Расписание торгов Московской Биржи: Фондовый рынок"""

    def __init__(self):
        super(MOEXStocks, self).__init__(
            [Session(time(10, 0, 0), time(18, 39, 59)),  # Основная торговая сессия
             Session(time(19, 5, 0), time(23, 49, 59))],  # Вечерняя торговая сессия
            timedelta(seconds=3))  # Задержка 3 секунды, чтобы гарантированно получить бар


class MOEXFutures(Schedule):
    """Расписание торгов Московской Биржи: Срочный рынок"""

    def __init__(self):
        super(MOEXFutures, self).__init__(
            [Session(time(9, 0, 0), time(9, 59, 59)),  # Утренняя дополнительная торговая сессия
             Session(time(10, 0, 0), time(13, 59, 59)),  # Основная торговая сессия (Дневной расчетный период)
             Session(time(14, 5, 0), time(18, 49, 59)),  # Основная торговая сессия (Вечерний расчетный период)
             Session(time(19, 5, 0), time(23, 49, 59))],  # Вечерняя дополнительная торговая сессия
            timedelta(seconds=3))  # Задержка 3 секунды, чтобы гарантированно получить бар
