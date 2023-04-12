
class BaseAdapter():
    def get_account(self, currency: str):
        raise NotImplementedError("Subclasses should implement this!")

    def get_order(self, order_id: int):
        raise NotImplementedError("Subclasses should implement this!")

    def get_fills(self, order_id: int):
        raise NotImplementedError("Subclasses should implement this!")

    def submit_orders(self, order: dict):  # TODO: Should be object, adapter should transform into dict.
        raise NotImplementedError("Subclasses should implement this!")
