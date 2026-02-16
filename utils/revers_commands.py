import os
from datetime import datetime
from typing import Dict

from core.settings import settings


def _format_datetime(dt: datetime) -> str:
    """Форматирует дату в 'dd.mm.yyyy HH:MM:SS' (требуемый формат PACS)"""
    return dt.strftime("%d.%m.%Y %H:%M:%S")


def get_edit_card_command(
    event_id: str,
    card_number: str,
    start_date: datetime,
    end_date: datetime
) -> Dict:
    """Генерирует команду редактирования карты"""
    return {
        "Command": "editcard",
        "Id": event_id,
        "Version": settings.REVERS_VERSION,
        "Data": {
            "Id": settings.REVERS_DATA_ID,
            "CardNum": card_number,
            "TemplateId": settings.REVERS_TEMPLATE_ID,
            "StartDate": _format_datetime(start_date),
            "EndDate": _format_datetime(end_date),
            "Action": settings.REVERS_ACTION_ISSUE,
        },
    }


def get_add_card_command(
    event_id: str,
    card_number: int,
    start_date: datetime,
    end_date: datetime
) -> Dict:
    """Генерирует команду добавления карты"""
    return {
        "Command": "addcard",
        "Id": event_id,
        "Version": settings.REVERS_VERSION,
        "Data": {
            "Id": settings.REVERS_DATA_ID,
            "CardNum": card_number,
            "TemplateId": settings.REVERS_TEMPLATE_ID,
            "StartDate": _format_datetime(start_date),
            "EndDate": _format_datetime(end_date),
            "Action": settings.REVERS_ACTION_ISSUE,
        },
    }


def get_load_card_command(event_id: str, card_number: int) -> Dict:
    """Генерирует команду загрузки/блокировки карты (wdraw)"""
    return {
        "Command": "loadcard",
        "Id": event_id,
        "Version": settings.REVERS_VERSION,
        "Data": {
            "CardNum": card_number,
            "Action": settings.REVERS_ACTION_WITHDRAW,
        },
    }


def get_delete_card_command(event_id: str, card_number: int) -> Dict:
    """Генерирует команду удаления карты"""
    return {
        "Command": "delcard",
        "Id": event_id,
        "Version": settings.REVERS_VERSION,
        "CardNum": card_number,
    }

def get_state_card_command(event_id: str, card_number: int) -> Dict:
    """Генерирует команду статуса карты"""
    return {
        "Command": "cardstatelist",
        "Id": event_id,
        "Version": settings.REVERS_VERSION,
        "CardNum": [card_number],
    }

def get_photo_timestamp(dt: datetime) -> str:
    """Форматирует дату для имени файла фото: 'dd-mm-yyyy-HH-MM-SS'"""
    return dt.strftime("%d-%m-%Y-%H-%M-%S")