import asyncio
from datetime import datetime as dt
from typing import List, Dict, Any

from core.db import DB
from core.tcpclient import TcpClient
from utils.logger import get_logger

logger = get_logger("tcp_client")

# def create_buffer(post_json_data):
#     buffer = post_json_data.encode('utf-8')
#     buffer_with_byte = bytearray(4 + len(buffer))
#     struct.pack_into('<I', buffer_with_byte, 0, len(buffer))
#     buffer_with_byte[4:] = buffer
#     return bytes(buffer_with_byte)

def create_buffer(json_data: str) -> bytes:
    """
    Упаковывает JSON-строку в бинарный буфер с 4-байтовым заголовком (little-endian).

    :param json_data: данные в JSON-формате
    :return: бинарный пакет (длина + данные)
    """
    data = json_data.encode('utf-8')
    header = len(data).to_bytes(4, 'little')
    return header + data

def datetime_to_timestamp(date_time):
    time_object = dt.strptime(date_time,"%d.%m.%Y %H:%M:%S")
    #ts = int(round(dt.timestamp(time_object)))
    return time_object

def parse_datetime(date_str: str) -> dt:
    """
    Парсит строку формата "dd.MM.yyyy HH:mm:ss" в datetime.

    :param date_str: строка даты/времени
    :return: объект даты и времени
    """
    return dt.strptime(date_str, "%d.%m.%Y %H:%M:%S")

# def chunk_data(client: TcpClient, buff_size: int = 1024) -> bytes:
#     """
#     Синхронное получение данных от клиента.
#
#     Args:
#         client (TcpClient): TCP-клиент
#         buff_size (int): размер буфера чтения
#
#     Returns:
#         bytes: принятые данные
#     """
#     data = b""
#     while True:
#         part = client.receive(buff_size)
#         if not part:
#             break
#         data += part
#         if len(part) < buff_size:  # достигнут конец пакета
#             break
#     return data

async def chunk_data_async(client: TcpClient, timeout: int = 5) -> bytes:
    """
      Асинхронное получение одного пакета (заголовок + данные).

      :param client: TCP-клиент
      :param timeout:
      :return: принятый пакет
      """
    try:
        header = await asyncio.wait_for(client.receive_exactly(4), timeout=timeout)
        length = int.from_bytes(header, "little")
        payload = await asyncio.wait_for(client.receive_exactly(length), timeout=timeout)
        return header + payload
    except asyncio.TimeoutError:
        return b""  # ничего не получили → проверим shutdown_event

async def insert_event_to_db(db: DB, events: List[Dict[str, Any]]) -> List[str]:
    """
    Сохраняет события в БД.

    :param db: объект базы данных
    :param events: список событий (dict)
    :return: список ID вставленных событий
    """
    if not isinstance(events, list):
        logger.warning(f"insert_event_to_db: ожидаемый тип 'list', получен {type(events)}")
        return []

    results = []
    for event in events:
        if not isinstance(event, dict):
            logger.warning(f"Неверные данные о событии (не dict): {event}")
            continue

        ev_time = event.get('EvTime')
        ev_ap = event.get('EvAddr')
        ev_user = event.get('EvUser')
        ev_card = event.get('EvCard')
        ev_code = event.get('EvCode')

        if not ev_time or not ev_ap or ev_user is None:
            logger.warning(f"Отсутствуют обязательные поля в событии: {event}")
            continue

        ev_owner = ev_user if ev_user != 0 else None

        try:
            row = await db.fetch_row(
                """
                INSERT INTO public.pacs_event(created, ap_id, owner_id, card, code)
                VALUES($1, $2, $3, $4, $5)
                RETURNING id
                """,
                parse_datetime(ev_time), ev_ap, ev_owner, ev_card, ev_code
            )
            if row:
                results.append(str(row['id']))
        except Exception as e:
            logger.error(f"Failed to insert event {event}: {e}")

    return results

async def load_system_ap(db: DB, ap_list: List[Dict[str, Any]]):
    """
       Загружает список access points в БД.

       :param db: объект базы данных
       :param ap_list: список access points
       """
    if not isinstance(ap_list, list):
        logger.warning(f"load_system_ap: expected list, got {type(ap_list)}")
        return

    for ap in ap_list:
        try:
            system_id = ap["Id"]
            name = ap["Name"]
            await db.execute(
                """
                INSERT INTO public.pacs_accesspoint(system_id, name)
                VALUES($1, $2)
                ON CONFLICT (system_id)
                DO UPDATE SET name=$2
                """,
                system_id, name
            )
        except Exception as e:
            logger.error(f"Не удалось вставить/обновить AP {ap}: {e}")


async def load_system_card_owner(db: DB, user_list: List[Dict[str, Any]]):
    """
    Загружает список владельцев карт в БД.

    :param db: объект базы данных
    :param user_list: список пользователей
    """
    if not isinstance(user_list, list):
        logger.warning(f"load_system_card_owner: ожидаемый тип 'List', получен {type(user_list)}")
        return

    for user in user_list:
        try:
            system_id = user["Id"]
            firstname = user["FirstName"]
            secondname = user["SecondName"]
            lastname = user["LastName"]
            await db.execute(
                """
                INSERT INTO public.pacs_cardowner(system_id, firstname, secondname, lastname)
                VALUES ($1, $2, $3, $4) ON CONFLICT (system_id)
                   DO
                UPDATE SET firstname=$2, secondname=$3, lastname=$4
                """,
                system_id, firstname, secondname, lastname
            )
        except Exception as e:
            logger.error(f"Не удалось вставить/обновить пользователя {user}: {e}")