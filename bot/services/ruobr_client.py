"""
Асинхронный клиент для Ruobr API.
Реализует неблокирующие запросы с повторными попытками и обработкой ошибок.
"""
import asyncio
import logging
from dataclasses import dataclass
from datetime import date, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional

import aiohttp
from aiohttp import ClientSession, ClientError, ClientResponseError

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
    group: str
    school: str
    
    @property
    def full_name(self) -> str:
        return f"{self.last_name} {self.first_name}".strip()
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Child':
        return cls(
            id=int(data.get("id", 0)),
            first_name=data.get("first_name", ""),
            last_name=data.get("last_name", ""),
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
        
        return cls(
            child_id=child_id,
            balance=balance,
            has_food=bool(data.get("balance")),
            visits=data.get("vizit", []) or []
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
    """Направление достижений."""
    direction: str
    count: int
    percent: int
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AchievementDirection':
        return cls(
            direction=data.get("direction_str", ""),
            count=data.get("cnt", 0),
            percent=data.get("percent_int", 0)
        )


@dataclass
class Achievements:
    """Достижения ученика."""
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
        return cls(
            name=data.get("person_str", ""),
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
    
    Оборачивает синхронную библиотеку ruobr_api в асинхронные вызовы
    через asyncio.to_thread для неблокирующей работы.
    """
    
    BASE_URL = "https://cabinet.ruobr.ru"
    
    def __init__(
        self,
        login: str,
        password: str,
        session: Optional[ClientSession] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0
    ):
        self._login = login
        self._password = password
        self._session = session
        self._own_session = session is None
        self._max_retries = max_retries
        self._retry_delay = retry_delay
        self._child_index = 0
    
    async def __aenter__(self) -> 'RuobrClient':
        if self._own_session:
            self._session = aiohttp.ClientSession(
                base_url=self.BASE_URL,
                timeout=aiohttp.ClientTimeout(total=30)
            )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._own_session and self._session:
            await self._session.close()
    
    def set_child(self, index: int) -> None:
        """Установка индекса текущего ребёнка."""
        self._child_index = index
    
    async def _request_with_retry(
        self,
        method: str,
        endpoint: str,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Выполнение запроса с повторными попытками.
        
        Args:
            method: HTTP метод.
            endpoint: URL endpoint.
            **kwargs: Параметры запроса.
            
        Returns:
            Ответ API в виде словаря.
            
        Raises:
            AuthenticationError: При ошибке аутентификации.
            NetworkError: При сетевой ошибке.
            RuobrError: При других ошибках API.
        """
        last_error = None
        
        for attempt in range(self._max_retries):
            try:
                # Используем синхронную библиотеку через to_thread
                result = await asyncio.to_thread(
                    self._sync_request,
                    method,
                    endpoint,
                    **kwargs
                )
                return result
            except AuthenticationError:
                raise  # Не повторяем при ошибке аутентификации
            except (NetworkError, ClientError) as e:
                last_error = e
                if attempt < self._max_retries - 1:
                    delay = self._retry_delay * (2 ** attempt)  # Exponential backoff
                    logger.warning(
                        f"Request failed (attempt {attempt + 1}/{self._max_retries}), "
                        f"retrying in {delay}s: {e}"
                    )
                    await asyncio.sleep(delay)
            except Exception as e:
                last_error = e
                logger.error(f"Unexpected error during request: {e}")
                break
        
        raise NetworkError(f"Request failed after {self._max_retries} attempts: {last_error}")
    
    def _sync_request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """
        Синхронный запрос через ruobr_api.
        Выполняется в отдельном потоке через to_thread.
        """
        from ruobr_api import Ruobr
        
        try:
            client = Ruobr(self._login, self._password)
            client.child = self._child_index
            
            if endpoint == "children":
                result = client.get_children()
            elif endpoint == "food":
                result = client.get_food_info()
            elif endpoint == "timetable":
                start = kwargs.get("start")
                end = kwargs.get("end")
                result = client.get_timetable(
                    start.strftime("%Y-%m-%d") if isinstance(start, date) else start,
                    end.strftime("%Y-%m-%d") if isinstance(end, date) else end
                )
            elif endpoint == "classmates":
                result = client.get_classmates()
            elif endpoint == "achievements":
                result = client.get_achievements()
            elif endpoint == "guide":
                result = client.get_guide()
            else:
                raise RuobrError(f"Unknown endpoint: {endpoint}")
            
            return result if isinstance(result, (dict, list)) else {}
            
        except Exception as e:
            error_str = str(e).lower()
            if "auth" in error_str or "login" in error_str or "password" in error_str:
                raise AuthenticationError(f"Authentication failed: {e}")
            if "network" in error_str or "connection" in error_str or "timeout" in error_str:
                raise NetworkError(f"Network error: {e}")
            raise RuobrError(f"API error: {e}")
    
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
    
    async def get_food_info(self) -> FoodInfo:
        """
        Получение информации о питании для текущего ребёнка.
        
        Returns:
            Объект FoodInfo.
        """
        result = await self._request_with_retry("GET", "food")
        
        # child_id будет установлен из контекста
        return FoodInfo.from_dict(self._child_index, result if isinstance(result, dict) else {})
    
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
        Получение достижений.
        
        Returns:
            Объект Achievements.
        """
        result = await self._request_with_retry("GET", "achievements")
        
        if not isinstance(result, dict):
            logger.warning(f"Unexpected achievements response type: {type(result)}")
            return Achievements(directions=[], projects=[], gto_id="")
        
        return Achievements.from_dict(result)
    
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


async def get_children_async(login: str, password: str) -> List[Child]:
    """Удобная функция для получения списка детей."""
    async with RuobrClient(login, password) as client:
        return await client.get_children()


async def get_food_for_children(
    login: str,
    password: str,
    children: List[Child]
) -> Dict[int, FoodInfo]:
    """
    Получение информации о питании для всех детей параллельно.
    
    Args:
        login: Логин Ruobr.
        password: Пароль Ruobr.
        children: Список детей.
        
    Returns:
        Словарь {child_id: FoodInfo}.
    """
    async def fetch_food(child: Child, index: int) -> tuple:
        async with RuobrClient(login, password) as client:
            client.set_child(index)
            food = await client.get_food_info()
            return child.id, food
    
    tasks = [fetch_food(child, idx) for idx, child in enumerate(children)]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    food_info = {}
    for result in results:
        if isinstance(result, Exception):
            logger.error(f"Error fetching food info: {result}")
        else:
            child_id, info = result
            food_info[child_id] = info
    
    return food_info


async def get_timetable_for_children(
    login: str,
    password: str,
    children: List[Child],
    start: date,
    end: date
) -> Dict[int, List[Lesson]]:
    """
    Получение расписания для всех детей параллельно.
    
    Args:
        login: Логин Ruobr.
        password: Пароль Ruobr.
        children: Список детей.
        start: Начальная дата.
        end: Конечная дата.
        
    Returns:
        Словарь {child_id: [Lesson]}.
    """
    async def fetch_timetable(child: Child, index: int) -> tuple:
        async with RuobrClient(login, password) as client:
            client.set_child(index)
            lessons = await client.get_timetable(start, end)
            return child.id, lessons
    
    tasks = [fetch_timetable(child, idx) for idx, child in enumerate(children)]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    timetable = {}
    for result in results:
        if isinstance(result, Exception):
            logger.error(f"Error fetching timetable: {result}")
        else:
            child_id, lessons = result
            timetable[child_id] = lessons
    
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
    Получение достижений для ребёнка.
    
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
