import os
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging
from sqlalchemy import (Table, Column, String, Integer, Float,
                        Boolean, ForeignKey, DateTime, JSON,
                        create_engine, MetaData, func, desc,
                        extract, text, inspect)
from sqlalchemy.exc import NoSuchTableError, OperationalError
from sqlalchemy.orm import sessionmaker

from .models import (Transaction, Receipt, User, Brand)
from .data_parser import RawDataParser

load_dotenv()



class Database:
    def __init__(self):
        self.user = os.getenv("POSTGRES_USER")
        self.password = os.getenv("POSTGRES_PASSWORD")
        self.db = os.getenv("POSTGRES_DB")
        self.host = "localhost"
        self.port = "5432"
        self.tables = {
            'users': User,
            'brands': Brand,
            'receipts': Receipt,
            'transactions': Transaction,
        }

        # Establish connection
        self.engine = create_engine(f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}')
        self.Session = sessionmaker(bind=self.engine)

        # Create logger
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        if not self.check_connection():
            raise Exception("Failed to connect to the database")

        self._process_raw_data()

    def check_connection(self):
        # Check if the Docker container is up and the database is accessible
        try:
            with self.engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            self.logger.info("Connection successful")
        except OperationalError as e:
            self.logger.error(f"Connection failed: {e}")
            return False
        return True

    def table_exists(self, table_name):
        inspector = inspect(self.engine)
        return inspector.has_table(table_name)

    def _process_raw_data(self, path=f'../data'):
        # Ensure tables are created and populated
        tables_to_populate = [table_name for table_name in self.tables
                              if not self._create_table(table_name)]

        for data_name in tables_to_populate:
            if data_name == 'transactions':
                continue  # Transactions are inserted with receipts

            # Insert data
            num_new_entries = 0
            self.logger.info(f"Reading {data_name}.json")
            with open(os.path.join(path, f'{data_name}.json'), 'r') as file:
                for line in file:
                    raw_data = json.loads(line)
                    num_new_entries += self.insert_raw_data(data_name, raw_data)
            self.logger.info(f"inserted {num_new_entries} new entries into {data_name}")

    def insert_raw_data(self, table_name, raw_data):

        table_class = self.tables[table_name]
        parsed_data = RawDataParser.parse(raw_data, table_name)
        with self.Session() as session:
            try:
                # Skip duplicate users
                if table_name == 'users':
                    if session.query(table_class).filter_by(id=parsed_data['id']).first():
                        self.logger.info(f"User {parsed_data['id']} already exists, skipping.")
                        return 0

                # Remove transactions from receipts
                receipt_item_list = parsed_data.pop('rewards_receipt_item_list', None)

                # Insert data
                session.add(table_class(**parsed_data))
                session.commit()
                self.logger.info(f"Data inserted into {table_name} successfully.")

                # Insert transactions
                if receipt_item_list:
                    for item in receipt_item_list:
                        self.insert_raw_data('transactions', item)

            except Exception as e:
                session.rollback()
                self.logger.error(f"Failed to insert data into {table_name}")
                self.logger.error(json.dumps(raw_data, indent=2))
                raise e
            return 1

    def _create_table(self, table_name):
        table_class = self.tables[table_name]
        inspector = inspect(self.engine)

        if not inspector.has_table(table_name):
            try:
                # Create table
                table_class.__table__.create(self.engine, checkfirst=False)
                self.logger.info(f"Table {table_name} created successfully")
                return False
            except Exception as e:
                self.logger.error(f"Failed to create table: {table_name}")
                raise e

        # Check if the table has entries
        session = self.Session()
        count = session.query(table_class).count()
        session.close()
        return count > 0