from mongoengine import (StringField,
                         DateTimeField,
                         BooleanField,
                         DecimalField,
                         Document,
                         IntField,
                         LongField,
                         EmbeddedDocument,
                         EmbeddedDocumentListField
                         )


class Candle(Document):
    time = IntField(required=True)
    low = DecimalField(required=True)
    high = DecimalField(required=True)
    open = DecimalField(required=True)
    close = DecimalField(required=True)
    volume = DecimalField(required=True)
    interval = IntField(required=True)
    product_id = StringField(required=True)

    meta = {'collection': 'candles'}


class Fill(EmbeddedDocument):
    trade_id = StringField()
    product_id = StringField()
    price = StringField()
    size = StringField()
    order_id = StringField()
    created_at = DateTimeField()
    liquidity = StringField()
    fee = StringField()
    settled = BooleanField()
    side = StringField()


class Order(Document):
    order_id = StringField()
    price = StringField()
    size = StringField()
    side = StringField()
    created_at = DateTimeField()
    done_at = DateTimeField()
    status = StringField()
    type = StringField()
    product_id = StringField()
    fills = EmbeddedDocumentListField(Fill, default=[])

    meta = {'collection': 'orders', 'strict': False}
    