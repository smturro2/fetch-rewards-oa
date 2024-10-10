from sqlalchemy import (Float, DateTime)

from .utils import (convert_to_snake_case, parse_dates, parse_floats)
from .models import Receipt, User, Brand, Transaction

class RawDataParser:
    @staticmethod
    def parse(data, data_type):
        cases = {
            "receipts": RawDataParser.parse_receipts,
            "users": RawDataParser.parse_users,
            "brands": RawDataParser.parse_brands,
            "transactions": RawDataParser.parse_transactions
        }

        return cases[data_type](data)

    @staticmethod
    def parse_receipts(raw_data):
        raw_data = {convert_to_snake_case(k): v for k, v in raw_data.items()}
        raw_data["id"] = raw_data["id"]["$oid"]

        date_cols = [c.name for c in Receipt.__table__.c if isinstance(c.type, DateTime)]
        parse_dates(raw_data, date_cols, in_milliseconds=True)

        float_cols = [c.name for c in Receipt.__table__.c if isinstance(c.type, Float)]
        parse_floats(raw_data, float_cols)

        k = 'rewards_receipt_item_list'
        if k in raw_data:
            transactions = raw_data[k]
            for item in transactions:
                item["receipt_id"] = raw_data["id"]
                RawDataParser.parse_transactions(item)

        return raw_data

    @staticmethod
    def parse_users(raw_data):
        raw_data = {convert_to_snake_case(k): v for k, v in raw_data.items()}
        raw_data["id"] = raw_data["id"]["$oid"]

        date_cols = [c.name for c in User.__table__.c if isinstance(c.type, DateTime)]
        parse_dates(raw_data, date_cols, in_milliseconds=True)

        float_cols = [c.name for c in User.__table__.c if isinstance(c.type, Float)]
        parse_floats(raw_data, float_cols)

        return raw_data

    @staticmethod
    def parse_brands(raw_data):
        raw_data = {convert_to_snake_case(k): v for k, v in raw_data.items()}
        raw_data["id"] = raw_data["id"]["$oid"]

        if "cpg" in raw_data:
            raw_data["cpg_id"] = raw_data["cpg"]["$id"]["$oid"]
            raw_data["cpg_ref"] = raw_data["cpg"]["$ref"]
            del raw_data["cpg"]

        date_cols = [c.name for c in Brand.__table__.c if isinstance(c.type, DateTime)]
        parse_dates(raw_data, date_cols, in_milliseconds=True)

        float_cols = [c.name for c in Brand.__table__.c if isinstance(c.type, Float)]
        parse_floats(raw_data, float_cols)

        return raw_data

    @staticmethod
    def parse_transactions(raw_data):
        raw_data = {convert_to_snake_case(k): v for k, v in raw_data.items()}

        date_cols = [c.name for c in Transaction.__table__.c if isinstance(c.type, DateTime)]
        parse_dates(raw_data, date_cols, in_milliseconds=True)

        float_cols = [c.name for c in Transaction.__table__.c if isinstance(c.type, Float)]
        parse_floats(raw_data, float_cols)

        return raw_data
