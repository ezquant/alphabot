from decimal import Decimal

class BaseStorage():

    def __init__(self, storage):
        self.storage = storage

    def create_signal(self):
        raise NotImplementedError("Subclasses should implement this!")

    def create_order(self):
        raise NotImplementedError("Subclasses should implement this!")

    def get_orders(self, status: list):
        raise NotImplementedError("Subclasses should implement this!")

    def update_order(self, _id: int, fills: list, status: str):
        raise NotImplementedError("Subclasses should implement this!")

    def create_candle(self,
                      time: int,
                      low: Decimal,
                      high: Decimal,
                      open: Decimal,
                      close: Decimal,
                      volume: Decimal,
                      interval: int):
        raise NotImplementedError("Subclasses should implement this!")
