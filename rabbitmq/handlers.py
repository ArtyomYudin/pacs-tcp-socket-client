import asyncio
import json
import os
from datetime import datetime, timedelta

from utils.functions import create_buffer, calculate_card_number
from utils.logger import get_logger
from utils.revers_commands import get_load_card_command, get_delete_card_command, get_add_card_command

# from rabbitmq.schemas import Event
#
#
#
# # async def events_handler(message):
# #     logger.debug(f"📩 [events] {message.body.decode()}")
#
logger = get_logger(os.getenv("DEBUG_MODE", True))

async def rmq_handler(message, tcp_client, command_manager):
    # Текущее время как точка отсчёта
    request_tstamp = datetime.now()
    dt_start = request_tstamp - timedelta(hours=1)  # -1 час
    dt_end = request_tstamp + timedelta(hours=8)  # +8 часов

    message_body = json.loads(message.body.decode())

    event_id = int(message_body["event_id"])
    raw_card_number = message_body["card_number"]
    revers_card_number = calculate_card_number(raw_card_number)
    event_type = message_body["event_type"]


    match event_type:
        case "issue":
            logger.info(f"Добавление гостевой карты {raw_card_number}")

            add_cmd = get_add_card_command(
                event_id,
                revers_card_number,
                dt_start,
                dt_end
            )
            command_manager.add(
                event_id=event_id,
                card_number=revers_card_number,
                event_type=event_type,
                stage="addcard"
            )

            await tcp_client.send(create_buffer(json.dumps(add_cmd)))

        case "wdraw":
            logger.info(f"Удаление гостевой карты {raw_card_number}")

            command_manager.add(
                event_id=event_id,
                card_number=revers_card_number,
                event_type=event_type,
                stage="loadcard"
            )
            load_cmd = get_load_card_command(event_id, revers_card_number)
            delete_cmd = get_delete_card_command(event_id, revers_card_number)

            # запрет использования карты
            await tcp_client.send(create_buffer(json.dumps(load_cmd)))
            await asyncio.sleep(2)
            # удаление карты
            command_manager.update_stage(event_id, "delcard")
            await tcp_client.send(create_buffer(json.dumps(delete_cmd)))
