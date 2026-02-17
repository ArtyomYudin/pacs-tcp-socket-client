from datetime import datetime


class CommandManager:
    def __init__(self, logger):
        self._pending = {}
        self.logger = logger

    def add(self, event_id: int, card_number: int, event_type: str, stage: str):
        self._pending[event_id] = {
            "card_number": card_number,
            "event_type": event_type,
            "stage": stage,
            "created_at": datetime.now()
        }

    def get(self, event_id: int):
        return self._pending.get(event_id)

    def update_stage(self, event_id: int, stage: str):
        if event_id in self._pending:
            self._pending[event_id]["stage"] = stage

    def remove(self, event_id: int):
        if event_id in self._pending:
            self.logger.debug(f"Удаление ожидающей команды {event_id}")
            self._pending.pop(event_id, None)

    def all(self):
        return self._pending
