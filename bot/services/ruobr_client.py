"""
Асинхронный клиент для Ruobr API.
Реализует неблокирующие запросы с повторными попытками и обработкой ошибок.
Использует AsyncRuobr из ruobr_api для нативных асинхронных запросов
без блокировки потоков (asyncio.to_thread).
"""
import asyncio
import logging
from dataclasses import dataclass
from datetime import date, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional

import httpx
from ruobr_api import AsyncRuobr
from ruobr_api.exceptions import (
    AuthenticationException,
    NoChildrenException,
    NoSuccessException,
)

from ..config import config

logger = logging.getLogger(__name__)


class RuobrError(Exception):
    """Базовая ошибка Ruobr API."""
    pass


class AuthenticationError(RuobrError):
    """Ошибка аутентификации."""
    pass


class NetworkError(RuobrError):
    """Ошибка сети."""
    pass


class RateLimitError(RuobrError):
    """Превышение лимита запросов."""
    pass


class DataError(RuobrError):
    """Ошибка данных."""
    pass


@dataclass
class Child:
    """Информация о ребёнке."""
    id: int
    first_name: str
    last_name: str
    middle_name: str
    birth_date: str
    gender: int
    group: str
    school: str

    @property
    def full_name(self) -> str:
        parts = [self.last_name, self.first_name, self.middle_name]
        return " ".join(p for p in parts if p).strip()

    @property
    def gender_icon(self) -> str:
        return "♂" if self.gender == 1 else "♀"

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Child':
        return cls(
            id=int(data.get("id", 0)),
            first_name=data.get("first_name", ""),
            last_name=data.get("last_name", ""),
            middle_name=data.get("middle_name", ""),
            birth_date=data.get("birth_date", ""),
            gender=data.get("gender", 1),
            group=data.get("group", ""),
            school=data.get("school", "")
        )


@dataclass
class FoodInfo:
    """Информация о питании."""
    child_id: int
    balance: float
    has_food: bool
    visits: List[Dict[str, Any]]

    @classmethod
    def from_dict(cls, child_id: int, data: Dict[str, Any]) -> 'FoodInfo':
        balance_raw = str(data.get("balance", "0")).replace(",", ".")
        try:
            balance = float(balance_raw)
        except ValueError:
            balance = 0.0

        # Извлекаем визиты из различных возможных ключей
        # API может возвращать данные под разными именами
        visits: List[Dict[str, Any]] = []

        # Приоритетный порядок проверки ключей
        visit_keys = ["vizit", "visit", "visits", "orders", "items"]
        for key in visit_keys:
            val = data.get(key)
            if val and isinstance(val, list) and len(val) > 0:
                visits = val
                logger.info(f"FoodInfo: found visits under key '{key}' ({len(visits)} items)")
                break

        # Если не нашли по известным ключам — ищем любой список со словарями,
        # содержащими поля характерные для визитов питания
        if not visits:
            visit_like_keys = {"date", "ordered", "state", "dishes", "price_sum", "price", "line_name"}
            for key, value in data.items():
                if key in ("balance", "balance_str"):
                    continue
                if isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
                    first_keys = set(value[0].keys())
                    if first_keys & visit_like_keys:
                        visits = value
                        logger.info(f"FoodInfo: auto-detected visits under key '{key}' ({len(visits)} items), fields: {list(first_keys)}")
                        break

        if not visits:
            # Логируем структуру данных для отладки
            logger.warning(
                f"FoodInfo: no visits found in food data for child {child_id}. "
                f"Available keys: {list(data.keys())}"
            )
            for key, value in data.items():
                if isinstance(value, list):
                    sample = value[0] if value else None
                    if isinstance(sample, dict):
                        logger.info(f"  {key}: list[{len(value)}], first item keys: {list(sample.keys())}")
                    else:
                        logger.info(f"  {key}: list[{len(value)}], item type: {type(sample).__name__}")
                elif isinstance(value, dict):
                    logger.info(f"  {key}: dict, keys: {list(value.keys())[:10]}")
                else:
                    logger.info(f"  {key}: {type(value).__name__} = {str(value)[:200]}")

        return cls(
            child_id=child_id,
            balance=balance,
            has_food=bool(data.get("balance")),
            visits=visits
        )


@dataclass
class Lesson:
    """Информация об уроке."""
    date: str
    time_start: str
    time_end: str
    subject: str
    topic: str
    room: str
    homework: List[Dict[str, Any]]
    marks: List[Dict[str, Any]]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Lesson':
        return cls(
            date=data.get("date", ""),
            time_start=data.get("time_start", ""),
            time_end=data.get("time_end", ""),
            subject=data.get("subject", ""),
            topic=data.get("topic", ""),
            room=data.get("room", ""),
            homework=data.get("task", []) or [],
            marks=data.get("marks", []) or []
        )


@dataclass
class Classmate:
    """Информация об однокласснике."""
    first_name: str
    last_name: str
    middle_name: str
    birth_date: str
    gender: int  # 1 - мальчик, 2 - девочка
    avatar: str

    @property
    def full_name(self) -> str:
        return f"{self.last_name} {self.first_name} {self.middle_name}".strip()

    @property
    def gender_icon(self) -> str:
        return "♂" if self.gender == 1 else "♀"

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Classmate':
        return cls(
            first_name=data.get("first_name", ""),
            last_name=data.get("last_name", ""),
            middle_name=data.get("middle_name", ""),
            birth_date=data.get("birth_date", ""),
            gender=data.get("gender", 1),
            avatar=data.get("avatar", "")
        )


@dataclass
class AchievementDirection:
    """Направление дополнительного образования."""
    direction: str
    count: int
    percent: int
    programs: List[Dict[str, Any]]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AchievementDirection':
        # Программы могут быть вложены в направление под разными ключами
        programs = (
            data.get("list", []) or
            data.get("do_list", []) or
            data.get("programs", []) or
            []
        )
        return cls(
            direction=data.get("direction_str", "") or data.get("name", ""),
            count=data.get("cnt", 0) or data.get("count", 0),
            percent=data.get("percent_int", 0) or data.get("percent", 0),
            programs=programs if isinstance(programs, list) else []
        )


@dataclass
class CertificateProgram:
    """Программа дополнительного образования из сертификата ПФДО.
    Данные приходят из petition_bad (завершённые) и petition_good (активные).
    """
    name: str               # program_name_short
    name_full: str          # program_name_full
    org: str                # program_school
    sum: str                # sum / program_sum (стоимость программы)
    fund: str               # fund_str (Бесплатно / Сертификат ПФ / Платно)
    status: str             # статус (текст)
    start_date: str         # pt_pfdo_contract_start_day
    end_date: str           # pt_pfdo_contract_date_end
    direction: str          # направление из do_direction (если сопоставлено)
    module_name: str        # module_name
    territory: str          # program_territory

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CertificateProgram':
        # Стоимость может приходить под разными ключами
        raw_sum = (
            data.get("sum", "") or
            data.get("program_sum", "") or
            data.get("contract_sum", "") or
            data.get("price", "")
        )
        # Форматируем: если число — округляем до 2 знаков
        sum_str = str(raw_sum).strip() if raw_sum else ""
        if sum_str:
            try:
                val = float(sum_str.replace(" ", "").replace("\xa0", ""))
                sum_str = f"{val:,.2f}".replace(",", " ")
            except (ValueError, TypeError):
                pass

        return cls(
            name=data.get("program_name_short", "") or data.get("text", ""),
            name_full=data.get("program_name_full", ""),
            org=data.get("program_school", "") or data.get("org", ""),
            sum=sum_str,
            fund=data.get("fund_str", ""),
            status=data.get("status", ""),
            start_date=str(data.get("pt_pfdo_contract_start_day", "") or ""),
            end_date=str(data.get("pt_pfdo_contract_date_end", "") or ""),
            direction="",  # Заполняется позже при группировке
            module_name=data.get("module_name", ""),
            territory=data.get("program_territory", "")
        )

    @property
    def is_active(self) -> bool:
        """Активная программа (обучается, зачислен)."""
        if not self.status:
            return False
        status_lower = self.status.lower()
        # Активные: не содержат "завершено"
        return "завершено" not in status_lower and "окончен" not in status_lower


@dataclass
class Certificate:
    """Данные по сертификату ПФДО."""
    number: str                     # number_cert
    nominal: str                    # rmc_nominal
    balance: str                    # balance
    balance_start: str              # balance_start
    group_name: str                 # cert_group_name
    territory: str                  # cert_territory
    programs_active: List[CertificateProgram]   # petition_good
    programs_completed: List[CertificateProgram]  # petition_bad

    @staticmethod
    def _fmt_money(val) -> str:
        """Форматирование денежного значения."""
        if not val:
            return ""
        s = str(val).strip().replace("\xa0", "").replace(" ", "")
        try:
            v = float(s)
            return f"{v:,.2f}".replace(",", " ")
        except (ValueError, TypeError):
            return s

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Certificate':
        programs_active = [
            CertificateProgram.from_dict(p)
            for p in (data.get("petition_good", []) or [])
        ]
        programs_completed = [
            CertificateProgram.from_dict(p)
            for p in (data.get("petition_bad", []) or [])
        ]

        return cls(
            number=str(data.get("number_cert", "") or ""),
            nominal=cls._fmt_money(data.get("rmc_nominal", "")),
            balance=cls._fmt_money(data.get("balance", "")),
            balance_start=cls._fmt_money(data.get("balance_start", "")),
            group_name=str(data.get("cert_group_name", "") or ""),
            territory=str(data.get("cert_territory", "") or ""),
            programs_active=programs_active,
            programs_completed=programs_completed
        )

    @property
    def all_programs(self) -> List[CertificateProgram]:
        return self.programs_active + self.programs_completed


@dataclass
class Achievements:
    """Дополнительное образование ученика."""
    directions: List[AchievementDirection]
    projects: List[Dict[str, Any]]
    gto_id: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Achievements':
        directions = [
            AchievementDirection.from_dict(d)
            for d in data.get("do_direction", [])
        ]
        return cls(
            directions=directions,
            projects=data.get("project_list", []),
            gto_id=data.get("gto_id", "")
        )


@dataclass
class Teacher:
    """Информация об учителе."""
    name: str
    subject: str
    user_id: int

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Teacher':
        # Пробуем получить полное ФИО из разных полей
        # person_str может быть в формате "Фамилия И.О." или "Фамилия Имя Отчество"
        name = (
            data.get("person_str", "") or
            data.get("fio", "") or
            data.get("full_name", "") or
            data.get("name", "")
        )
        return cls(
            name=name,
            subject=data.get("subject_qs", ""),
            user_id=data.get("user_id", 0)
        )


@dataclass
class SchoolGuide:
    """Информация о школе."""
    name: str
    address: str
    phone: str
    url: str
    teachers: List[Teacher]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SchoolGuide':
        teachers = [
            Teacher.from_dict(t)
            for t in data.get("teacher_list", [])
        ]
        return cls(
            name=data.get("name", ""),
            address=data.get("post_adress", ""),
            phone=data.get("tel_rec", ""),
            url=data.get("url", ""),
            teachers=teachers
        )


class RuobrClient:
    """
    Асинхронный клиент для Ruobr API.

    Использует AsyncRuobr из ruobr_api для нативных асинхронных запросов
    без блокировки потоков (asyncio.to_thread).

    Авторизация выполняется один раз при входе в контекстный менеджер.
    Последующие запросы переиспользуют сессию без повторного логина.
    """

    API_TIMEOUT = 30  # Таймаут API запросов в секундах

    def __init__(
        self,
        login: str,
        password: str,
        max_retries: int = 3,
        retry_delay: float = 1.0
    ):
        self._login = login
        self._password = password
        self._max_retries = max_retries
        self._retry_delay = retry_delay
        self._client: Optional[AsyncRuobr] = None

    async def __aenter__(self) -> 'RuobrClient':
        """
        Вход в контекст: создаёт AsyncRuobr и авторизуется один раз.
        Все последующие запросы в этом контексте пропускают повторную авторизацию.
        """
        self._client = AsyncRuobr(self._login, self._password)
        try:
            await asyncio.wait_for(
                self._client.get_user(),
                timeout=self.API_TIMEOUT
            )
        except AuthenticationException as e:
            self._client = None
            raise AuthenticationError(f"Authentication failed: {e}")
        except (httpx.TimeoutException, httpx.ConnectError) as e:
            self._client = None
            raise NetworkError(f"Auth network error: {e}")
        except Exception as e:
            self._client = None
            raise NetworkError(f"Auth failed: {e}")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Выход из контекста: освобождает клиент."""
        self._client = None

    def set_child(self, index: int) -> None:
        """Установка индекса текущего ребёнка."""
        if self._client:
            self._client.child = index

    async def _request_with_retry(
        self,
        method: str,
        endpoint: str,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Выполнение запроса с повторными попытками.
        Не блокирует потоки — использует нативный async.

        Args:
            method: HTTP метод.
            endpoint: URL endpoint.
            **kwargs: Параметры запроса.

        Returns:
            Ответ API в виде словаря.

        Raises:
            AuthenticationError: При ошибке аутентификации (без повтора).
            NetworkError: При сетевой ошибке после всех попыток.
            RuobrError: При ошибке API после всех попыток.
        """
        last_error = None

        for attempt in range(self._max_retries):
            try:
                coro = self._get_coroutine(endpoint, **kwargs)
                result = await asyncio.wait_for(coro, timeout=self.API_TIMEOUT)
                return result if isinstance(result, (dict, list)) else {}

            except asyncio.TimeoutError:
                last_error = NetworkError(f"Request timeout after {self.API_TIMEOUT}s")
                logger.warning(f"Request timeout (attempt {attempt + 1}/{self._max_retries})")
                if attempt < self._max_retries - 1:
                    delay = self._retry_delay * (2 ** attempt)
                    await asyncio.sleep(delay)

            except AuthenticationException as e:
                # Ошибка аутентификации — не повторяем
                raise AuthenticationError(str(e))

            except NoChildrenException as e:
                # Нет детей на аккаунте — не повторяем
                raise RuobrError(str(e))

            except NoSuccessException as e:
                # API вернул success=false — повторяем (может быть временной ошибкой)
                last_error = RuobrError(str(e))
                logger.warning(
                    f"API error (attempt {attempt + 1}/{self._max_retries}): {e}"
                )
                if attempt < self._max_retries - 1:
                    delay = self._retry_delay * (2 ** attempt)
                    await asyncio.sleep(delay)

            except (httpx.TimeoutException, httpx.ConnectError,
                    httpx.ReadError, httpx.WriteError) as e:
                # Сетевая ошибка на уровне httpx — повторяем
                last_error = NetworkError(f"Network error: {e}")
                logger.warning(
                    f"Network error (attempt {attempt + 1}/{self._max_retries}): {e}"
                )
                if attempt < self._max_retries - 1:
                    delay = self._retry_delay * (2 ** attempt)
                    await asyncio.sleep(delay)

            except Exception as e:
                # Неизвестная ошибка — логируем и прерываем повторы
                last_error = RuobrError(f"Unexpected API error: {e}")
                logger.error(f"Unexpected error during request: {e}")
                break

        raise last_error or NetworkError(
            f"Request failed after {self._max_retries} attempts"
        )

    def _get_coroutine(self, endpoint: str, **kwargs):
        """
        Возвращает корутину для соответствующего эндпоинта AsyncRuobr.
        Авторизация уже выполнена в __aenter__, поэтому каждый вызов
        делает только один HTTP-запрос к API.
        """
        if endpoint == "children":
            return self._client.get_children()
        elif endpoint == "food":
            return self._client.get_food_info()
        elif endpoint == "timetable":
            start = kwargs.get("start")
            end = kwargs.get("end")
            return self._client.get_timetable(
                start.strftime("%Y-%m-%d") if isinstance(start, date) else start,
                end.strftime("%Y-%m-%d") if isinstance(end, date) else end
            )
        elif endpoint == "classmates":
            return self._client.get_classmates()
        elif endpoint == "achievements":
            return self._client.get_achievements()
        elif endpoint == "certificate":
            return self._client.get_certificate()
        elif endpoint == "guide":
            return self._client.get_guide()
        else:
            raise RuobrError(f"Unknown endpoint: {endpoint}")

    async def get_children(self) -> List[Child]:
        """
        Получение списка детей.

        Returns:
            Список объектов Child.
        """
        result = await self._request_with_retry("GET", "children")

        if not isinstance(result, list):
            logger.warning(f"Unexpected children response type: {type(result)}")
            return []

        return [Child.from_dict(child) for child in result]

    async def get_food_info(self, child_id: Optional[int] = None) -> FoodInfo:
        """
        Получение информации о питании для текущего ребёнка.

        Args:
            child_id: Реальный ID ребёнка из API. Если не указан, используется текущий индекс.

        Returns:
            Объект FoodInfo.
        """
        result = await self._request_with_retry("GET", "food")

        effective_id = child_id if child_id is not None else (
            self._client.child if self._client else 0
        )
        return FoodInfo.from_dict(effective_id, result if isinstance(result, dict) else {})

    async def get_timetable(
        self,
        start: date,
        end: date
    ) -> List[Lesson]:
        """
        Получение расписания.

        Args:
            start: Начальная дата.
            end: Конечная дата.

        Returns:
            Список объектов Lesson.
        """
        result = await self._request_with_retry(
            "GET", "timetable", start=start, end=end
        )

        if not isinstance(result, list):
            logger.warning(f"Unexpected timetable response type: {type(result)}")
            return []

        return [Lesson.from_dict(lesson) for lesson in result]

    async def get_classmates(self) -> List[Classmate]:
        """
        Получение списка одноклассников.

        Returns:
            Список объектов Classmate.
        """
        result = await self._request_with_retry("GET", "classmates")

        if not isinstance(result, list):
            logger.warning(f"Unexpected classmates response type: {type(result)}")
            return []

        return [Classmate.from_dict(c) for c in result]

    async def get_achievements(self) -> Achievements:
        """
        Получение данных о дополнительном образовании.

        Returns:
            Объект Achievements с направлениями и программами.
        """
        result = await self._request_with_retry("GET", "achievements")

        if not isinstance(result, dict):
            logger.warning(f"Unexpected achievements response type: {type(result)}")
            return Achievements(directions=[], projects=[], gto_id="")

        # Логируем полную структуру для отладки
        import json
        logger.info(f"Achievements raw response: {json.dumps(result, ensure_ascii=False)[:3000]}")

        return Achievements.from_dict(result)

    async def get_certificate(self) -> Certificate:
        """
        Получение информации о сертификате ПФДО.

        Returns:
            Объект Certificate с программами и балансом.
        """
        result = await self._request_with_retry("GET", "certificate")

        if not isinstance(result, dict):
            logger.warning(f"Unexpected certificate response type: {type(result)}")
            return Certificate(number="", nominal="", balance="", balance_start="", group_name="", territory="", programs_active=[], programs_completed=[])

        # Логируем полную структуру для отладки
        import json
        logger.info(f"Certificate raw response: {json.dumps(result, ensure_ascii=False)[:3000]}")

        return Certificate.from_dict(result)

    async def get_guide(self) -> SchoolGuide:
        """
        Получение информации о школе.

        Returns:
            Объект SchoolGuide.
        """
        result = await self._request_with_retry("GET", "guide")

        if not isinstance(result, dict):
            logger.warning(f"Unexpected guide response type: {type(result)}")
            return SchoolGuide(name="", address="", phone="", url="", teachers=[])

        return SchoolGuide.from_dict(result)


from .cache import children_cache


async def get_children_async(login: str, password: str, *, use_cache: bool = True) -> List[Child]:
    """
    Удобная функция для получения списка детей.

    Кэширует результат на 24 часа (86400 сек). Кэш инвалидируется при:
    - set_login (новые учётные данные)
    - create_or_update_user с новым паролем

    С AsyncRuobr: 1 HTTP-вызов (авторизация через get_user).
    Данные о детях возвращаются из поля _children без дополнительного запроса.
    """
    cache_key = f"{login}:children"

    if use_cache:
        cached = children_cache.get(cache_key)
        if cached is not None:
            logger.debug(f"Children cache hit for {login}")
            return cached

    async with RuobrClient(login, password) as client:
        children = await client.get_children()

    # Сохраняем в кэш на 24 часа
    children_cache.set(cache_key, children, ttl=86400)
    logger.debug(f"Children fetched and cached for {login}: {len(children)} child(ren)")

    return children


async def get_food_for_children(
    login: str,
    password: str,
    children: List[Child]
) -> Dict[int, FoodInfo]:
    """
    Получение информации о питании для всех детей.

    Использует один клиент RuobrClient с единой авторизацией.
    Запросы для разных детей выполняются последовательно (одна авторизация).

    Args:
        login: Логин Ruobr.
        password: Пароль Ruobr.
        children: Список детей.

    Returns:
        Словарь {child_id: FoodInfo}.
    """
    food_info: Dict[int, FoodInfo] = {}

    try:
        async with RuobrClient(login, password) as client:
            for idx, child in enumerate(children):
                try:
                    client.set_child(idx)
                    food = await client.get_food_info(child_id=child.id)
                    food_info[child.id] = food
                except (AuthenticationError, NetworkError, RuobrError) as e:
                    logger.error(f"Error fetching food info for child {child.id}: {e}")

    except (AuthenticationError, NetworkError) as e:
        logger.error(f"Error creating client for food: {e}")
        return food_info

    return food_info


async def get_timetable_for_children(
    login: str,
    password: str,
    children: List[Child],
    start: date,
    end: date
) -> Dict[int, List[Lesson]]:
    """
    Получение расписания для всех детей.

    Использует один клиент RuobrClient с единой авторизацией.
    Запросы для разных детей выполняются последовательно (одна авторизация).

    Args:
        login: Логин Ruobr.
        password: Пароль Ruobr.
        children: Список детей.
        start: Начальная дата.
        end: Конечная дата.

    Returns:
        Словарь {child_id: [Lesson]}.
    """
    timetable: Dict[int, List[Lesson]] = {}

    try:
        async with RuobrClient(login, password) as client:
            for idx, child in enumerate(children):
                try:
                    client.set_child(idx)
                    lessons = await client.get_timetable(start, end)
                    timetable[child.id] = lessons
                except (AuthenticationError, NetworkError, RuobrError) as e:
                    logger.error(f"Error fetching timetable for child {child.id}: {e}")

    except (AuthenticationError, NetworkError) as e:
        logger.error(f"Error creating client for timetable: {e}")
        return timetable

    return timetable


async def get_classmates_for_child(
    login: str,
    password: str,
    child_index: int = 0
) -> List[Classmate]:
    """
    Получение списка одноклассников для ребёнка.

    Args:
        login: Логин Ruobr.
        password: Пароль Ruobr.
        child_index: Индекс ребёнка (0 по умолчанию).

    Returns:
        Список объектов Classmate.
    """
    async with RuobrClient(login, password) as client:
        client.set_child(child_index)
        return await client.get_classmates()


async def get_achievements_for_child(
    login: str,
    password: str,
    child_index: int = 0
) -> Achievements:
    """
    Получение данных о доп. образовании для ребёнка.

    Args:
        login: Логин Ruobr.
        password: Пароль Ruobr.
        child_index: Индекс ребёнка (0 по умолчанию).

    Returns:
        Объект Achievements.
    """
    async with RuobrClient(login, password) as client:
        client.set_child(child_index)
        return await client.get_achievements()


async def get_certificate_for_child(
    login: str,
    password: str,
    child_index: int = 0
) -> Certificate:
    """
    Получение сертификата ПФДО для ребёнка.

    Args:
        login: Логин Ruobr.
        password: Пароль Ruobr.
        child_index: Индекс ребёнка (0 по умолчанию).

    Returns:
        Объект Certificate.
    """
    async with RuobrClient(login, password) as client:
        client.set_child(child_index)
        return await client.get_certificate()


async def get_guide_for_child(
    login: str,
    password: str,
    child_index: int = 0
) -> SchoolGuide:
    """
    Получение информации о школе для ребёнка.

    Args:
        login: Логин Ruobr.
        password: Пароль Ruobr.
        child_index: Индекс ребёнка (0 по умолчанию).

    Returns:
        Объект SchoolGuide.
    """
    async with RuobrClient(login, password) as client:
        client.set_child(child_index)
        return await client.get_guide()
