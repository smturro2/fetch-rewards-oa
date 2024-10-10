import os
import ijson
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker
from sqlalchemy import (Table, Column, String, Integer, Float,
                        Boolean, ForeignKey, DateTime, JSON,
                        create_engine, MetaData, func, desc, extract)
from datetime import datetime, timedelta

class ConsumerDatabase:
    def __init__(self):
        self.user = os.getenv("POSTGRES_USER")
        self.password = os.getenv("POSTGRES_PASSWORD")
        self.db = os.getenv("POSTGRES_DB")
        self.host = "localhost"
        self.port = "5432"

        # Establish connection
        self.engine = create_engine(f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}')
        self.Session = sessionmaker(bind=self.engine)

        if not self.check_connection():
            raise Exception("Failed to connect to the database")

        self._init_tables()


    def check_connection(self):
        # Check if the Docker container is up and the database is accessible
        try:
            with self.engine.connect() as connection:
                connection.execute("SELECT 1")
            print("Connection successful")
        except OperationalError:
            print("Connection failed")
            return False
        return True

    from sqlalchemy import func, desc, extract
    from datetime import datetime

    def get_top_5_brands_recent_month(self):
        """Get the top 5 brands by receipts scanned for the most recent month."""
        with self.Session() as session:
            recent_month = datetime.now().month
            results = session.query(
                self.brands.c.name,
                func.count(self.receipts.c.id).label('receipt_count')
            ).join(
                self.receipts, self.brands.c.id == self.receipts.c.rewards_receipt_item_list['brand_id']
            ).filter(
                extract('month', self.receipts.c.date_scanned) == recent_month
            ).group_by(
                self.brands.c.name
            ).order_by(
                desc('receipt_count')
            ).limit(5).all()

            return results

    def compare_top_5_brands_recent_previous_month(self):
        """Compare the ranking of the top 5 brands by receipts scanned for the recent month and the previous month."""
        with self.Session() as session:
            current_month = datetime.now().month
            previous_month = (datetime.now() - timedelta(days=30)).month

            current_month_ranking = session.query(
                self.brands.c.name,
                func.count(self.receipts.c.id).label('receipt_count')
            ).join(
                self.receipts, self.brands.c.id == self.receipts.c.rewards_receipt_item_list['brand_id']
            ).filter(
                extract('month', self.receipts.c.date_scanned) == current_month
            ).group_by(
                self.brands.c.name
            ).order_by(
                desc('receipt_count')
            ).limit(5).all()

            previous_month_ranking = session.query(
                self.brands.c.name,
                func.count(self.receipts.c.id).label('receipt_count')
            ).join(
                self.receipts, self.brands.c.id == self.receipts.c.rewards_receipt_item_list['brand_id']
            ).filter(
                extract('month', self.receipts.c.date_scanned) == previous_month
            ).group_by(
                self.brands.c.name
            ).order_by(
                desc('receipt_count')
            ).limit(5).all()

            return current_month_ranking, previous_month_ranking

    def compare_average_spend(self):
        """Compare average spend from receipts with 'Accepted' or 'Rejected' status."""
        with self.Session() as session:
            accepted_spend = session.query(
                func.avg(self.receipts.c.total_spent).label('average_spend')
            ).filter(
                self.receipts.c.rewards_receipt_status == 'Accepted'
            ).scalar()

            rejected_spend = session.query(
                func.avg(self.receipts.c.total_spent).label('average_spend')
            ).filter(
                self.receipts.c.rewards_receipt_status == 'Rejected'
            ).scalar()

            return accepted_spend, rejected_spend

    def compare_total_items_purchased(self):
        """Compare total number of items purchased from receipts with 'Accepted' or 'Rejected' status."""
        with self.Session() as session:
            accepted_items = session.query(
                func.sum(self.receipts.c.purchased_item_count).label('total_items')
            ).filter(
                self.receipts.c.rewards_receipt_status == 'Accepted'
            ).scalar()

            rejected_items = session.query(
                func.sum(self.receipts.c.purchased_item_count).label('total_items')
            ).filter(
                self.receipts.c.rewards_receipt_status == 'Rejected'
            ).scalar()

            return accepted_items, rejected_items

    def brand_with_most_spend_recent_users(self):
        """Determine which brand has the most spend among users created within the past 6 months."""
        with self.Session() as session:
            six_months_ago = datetime.now() - timedelta(days=180)
            results = session.query(
                self.brands.c.name,
                func.sum(self.receipts.c.total_spent).label('total_spend')
            ).join(
                self.receipts, self.brands.c.id == self.receipts.c.rewards_receipt_item_list['brand_id']
            ).join(
                self.users, self.receipts.c.user_id == self.users.c.id
            ).filter(
                self.users.c.created_date >= six_months_ago
            ).group_by(
                self.brands.c.name
            ).order_by(
                desc('total_spend')
            ).first()

            return results

    def brand_with_most_transactions_recent_users(self):
        """Determine which brand has the most transactions among users created within the past 6 months."""
        with self.Session() as session:
            six_months_ago = datetime.now() - timedelta(days=180)
            results = session.query(
                self.brands.c.name,
                func.count(self.receipts.c.id).label('transaction_count')
            ).join(
                self.receipts, self.brands.c.id == self.receipts.c.rewards_receipt_item_list['brand_id']
            ).join(
                self.users, self.receipts.c.user_id == self.users.c.id
            ).filter(
                self.users.c.created_date >= six_months_ago
            ).group_by(
                self.brands.c.name
            ).order_by(
                desc('transaction_count')
            ).first()

            return results

    def insert_raw_data(self, table_name, raw_data):
        metadata = MetaData(bind=self.engine)

        cases = {
            'receipts': (self.receipts, self.parse_receipts),
            'users': (self.users, self.parse_users),
            'brands': (self.brands, self.parse_brands),
            'transactions': (self.transactions, self.parse_transactions),
        }

        table, parser = cases[table_name]
        parsed_data = parser(raw_data)

        with self.Session() as session:
            try:
                if table_name == 'users':
                    # Skip duplicate users
                    if session.query(table).filter_by(id=parsed_data['id']).first():
                        print(f"User {parsed_data['id']} already exists, skipping.")
                        return

                session.execute(table.insert(), parsed_data)
                session.commit()
                print(f"Data inserted into {table_name} successfully.")
            except Exception as e:
                session.rollback()
                print(f"Failed to insert data into {table_name}: {e}")

        @staticmethod
        def parse_receipts(raw_data):
            return {
                'id': raw_data['_id'],
                'bonus_points_earned': raw_data.get('bonus_points_earned', 0),
                'bonus_points_earned_reason': raw_data.get('bonus_points_earned_reason'),
                'create_date': raw_data.get('create_date'),
                'date_scanned': raw_data.get('date_scanned'),
                'finished_date': raw_data.get('finished_date'),
                'modify_date': raw_data.get('modify_date'),
                'points_awarded_date': raw_data.get('points_awarded_date'),
                'points_earned': raw_data.get('points_earned'),
                'purchase_date': raw_data.get('purchase_date'),
                'purchased_item_count': raw_data.get('purchased_item_count'),
                'rewards_receipt_item_list': raw_data.get('rewards_receipt_item_list'),
                'rewards_receipt_status': raw_data.get('rewards_receipt_status'),
                'total_spent': raw_data.get('total_spent'),
                'user_id': raw_data.get('user_id')
            }

        @staticmethod
        def parse_users(raw_data):
            return {
                'id': raw_data['_id'],
                'state': raw_data.get('state'),
                'created_date': raw_data.get('created_date'),
                'last_login': raw_data.get('last_login'),
                'role': raw_data.get('role', 'CONSUMER'),
                'active': raw_data.get('active', True)
            }

        @staticmethod
        def parse_brands(raw_data):
            return {
                'id': raw_data['_id'],
                'brand_code': raw_data.get('brand_code'),
                'barcode': raw_data.get('barcode'),
                'name': raw_data.get('name'),
                'category': raw_data.get('category'),
                'category_code': raw_data.get('category_code'),
                'cpg_id': raw_data.get('cpg_id'),
                'top_brand': raw_data.get('top_brand', False)
            }

        @staticmethod
        def parse_transactions(raw_data):
            return {
                'barcode': raw_data.get('barcode'),
                'brand_code': raw_data.get('brand_code'),
                'description': raw_data.get('description'),
                'item_price': raw_data.get('item_price'),
                'target_price': raw_data.get('target_price'),
                'price_after_coupon': raw_data.get('price_after_coupon'),
                'discounted_item_price': raw_data.get('discounted_item_price'),
                'final_price': raw_data.get('final_price'),
                'quantity_purchased': raw_data.get('quantity_purchased'),
                'needs_fetch_review': raw_data.get('needs_fetch_review'),
                'partner_item_id': raw_data.get('partner_item_id'),
                'prevent_target_gap_points': raw_data.get('prevent_target_gap_points'),
                'user_flagged_barcode': raw_data.get('user_flagged_barcode'),
                'user_flagged_new_item': raw_data.get('user_flagged_new_item'),
                'user_flagged_price': raw_data.get('user_flagged_price'),
                'user_flagged_quantity': raw_data.get('user_flagged_quantity'),
                'needs_fetch_review_reason': raw_data.get('needs_fetch_review_reason'),
                'points_earned': raw_data.get('points_earned'),
                'points_not_awarded_reason': raw_data.get('points_not_awarded_reason'),
                'points_payer_id': raw_data.get('points_payer_id'),
                'rewards_group': raw_data.get('rewards_group'),
                'rewards_product_partner_id': raw_data.get('rewards_product_partner_id'),
                'user_flagged_description': raw_data.get('user_flagged_description'),
                'metabrite_campaign_id': raw_data.get('metabrite_campaign_id'),
                'original_final_price': raw_data.get('original_final_price'),
                'original_meta_brite_barcode': raw_data.get('original_meta_brite_barcode'),
                'original_meta_brite_description': raw_data.get('original_meta_brite_description'),
                'original_meta_brite_quantity_purchased': raw_data.get('original_meta_brite_quantity_purchased'),
                'original_meta_brite_item_price': raw_data.get('original_meta_brite_item_price'),
                'original_receipt_item_text': raw_data.get('original_receipt_item_text'),
                'item_number': raw_data.get('item_number'),
                'competitive_product': raw_data.get('competitive_product'),
                'competitor_rewards_group': raw_data.get('competitor_rewards_group'),
                'deleted': raw_data.get('deleted', False)  # assuming default for 'deleted' is False
            }

    def _init_tables(self):
        metadata = MetaData(bind=self.engine)

        self.receipts = Table(
            'receipts', metadata,
            Column('id', String, primary_key=True),
            Column('bonus_points_earned', Integer, default=0),
            Column('bonus_points_earned_reason', String),
            Column('create_date', DateTime),
            Column('date_scanned', DateTime),
            Column('finished_date', DateTime),
            Column('modify_date', DateTime),
            Column('points_awarded_date', DateTime),
            Column('points_earned', Integer),
            Column('purchase_date', DateTime),
            Column('purchased_item_count', Integer),
            Column('rewards_receipt_item_list', JSON),
            Column('rewards_receipt_status', String),
            Column('total_spent', Float),
            Column('user_id', String, ForeignKey('users.id'))
        )

        self.users = Table(
            'users', metadata,
            Column('id', String, primary_key=True),
            Column('state', String),
            Column('created_date', DateTime),
            Column('last_login', DateTime),
            Column('role', String),
            Column('active', Boolean)
        )

        self.brands = Table(
            'brands', metadata,
            Column('id', String, primary_key=True),
            Column('brand_code', String),
            Column('barcode', String),
            Column('name', String),
            Column('category', String),
            Column('category_code', String),
            Column('cpg_id', String),
            Column('top_brand', Boolean)
        )

        self.transactions = Table(
            'transactions', metadata,
            Column('barcode', String),
            Column('brand_code', String),
            Column('description', String),
            Column('item_price', Float),
            Column('target_price', Float),
            Column('price_after_coupon', Float),
            Column('discounted_item_price', Float),
            Column('final_price', Float),
            Column('quantity_purchased', Integer),
            Column('needs_fetch_review', Boolean),
            Column('partner_item_id', String),
            Column('prevent_target_gap_points', Boolean),
            Column('user_flagged_barcode', String),
            Column('user_flagged_new_item', Boolean),
            Column('user_flagged_price', Boolean),
            Column('user_flagged_quantity', Integer),
            Column('needs_fetch_review_reason', String),
            Column('points_earned', Integer),
            Column('points_not_awarded_reason', String),
            Column('points_payer_id', String),
            Column('rewards_group', String),
            Column('rewards_product_partner_id', String),
            Column('user_flagged_description', String),
            Column('metabrite_campaign_id', String),
            Column('original_final_price', Float),
            Column('original_meta_brite_barcode', String),
            Column('original_meta_brite_description', String),
            Column('original_meta_brite_quantity_purchased', Integer),
            Column('original_meta_brite_item_price', Float),
            Column('original_receipt_item_text', String),
            Column('item_number', String),
            Column('competitive_product', Boolean),
            Column('competitor_rewards_group', String),
            Column('deleted', Boolean, default=False)
        )

        metadata.create_all(self.engine)

    def _process_raw_data(self):
        # Process and insert raw data from JSON files
        for f in ['brands', 'receipts', 'users']:
            with open(f'data/{f}.json', 'r') as file:
                print(f"Reading {f}")
                for raw_data in ijson.items(file, 'item'):
                    self.insert_raw_data(f, raw_data)

    @staticmethod
    def parse_receipts(raw_data):
        return {
            'id': raw_data['_id'],
            'bonus_points_earned': raw_data.get('bonus_points_earned', 0),
            'bonus_points_earned_reason': raw_data.get('bonus_points_earned_reason'),
            'create_date': raw_data.get('create_date'),
            'date_scanned': raw_data.get('date_scanned'),
            'finished_date': raw_data.get('finished_date'),
            'modify_date': raw_data.get('modify_date'),
            'points_awarded_date': raw_data.get('points_awarded_date'),
            'points_earned': raw_data.get('points_earned'),
            'purchase_date': raw_data.get('purchase_date'),
            'purchased_item_count': raw_data.get('purchased_item_count'),
            'rewards_receipt_item_list': raw_data.get('rewards_receipt_item_list'),
            'rewards_receipt_status': raw_data.get('rewards_receipt_status'),
            'total_spent': raw_data.get('total_spent'),
            'user_id': raw_data.get('user_id')
        }

    @staticmethod
    def parse_users(raw_data):
        return {
            'id': raw_data['_id'],
            'state': raw_data.get('state'),
            'created_date': raw_data.get('created_date'),
            'last_login': raw_data.get('last_login'),
            'role': raw_data.get('role', 'CONSUMER'),
            'active': raw_data.get('active', True)
        }

    @staticmethod
    def parse_brands(raw_data):
        return {
            'id': raw_data['_id'],
            'brand_code': raw_data.get('brand_code'),
            'barcode': raw_data.get('barcode'),
            'name': raw_data.get('name'),
            'category': raw_data.get('category'),
            'category_code': raw_data.get('category_code'),
            'cpg_id': raw_data.get('cpg_id'),
            'top_brand': raw_data.get('top_brand', False)
        }

    @staticmethod
    def parse_transactions(raw_data):
        return {
            'barcode': raw_data.get('barcode'),
            'brand_code': raw_data.get('brand_code'),
            'description': raw_data.get('description'),
            'item_price': raw_data.get('item_price'),
            'target_price': raw_data.get('target_price'),
            'price_after_coupon': raw_data.get('price_after_coupon'),
            'discounted_item_price': raw_data.get('discounted_item_price'),
            'final_price': raw_data.get('final_price'),
            'quantity_purchased': raw_data.get('quantity_purchased'),
            'needs_fetch_review': raw_data.get('needs_fetch_review'),
            'partner_item_id': raw_data.get('partner_item_id'),
            'prevent_target_gap_points': raw_data.get('prevent_target_gap_points'),
            'user_flagged_barcode': raw_data.get('user_flagged_barcode'),
            'user_flagged_new_item': raw_data.get('user_flagged_new_item'),
            'user_flagged_price': raw_data.get('user_flagged_price'),
            'user_flagged_quantity': raw_data.get('user_flagged_quantity'),
            'needs_fetch_review_reason': raw_data.get('needs_fetch_review_reason'),
            'points_earned': raw_data.get('points_earned'),
            'points_not_awarded_reason': raw_data.get('points_not_awarded_reason'),
            'points_payer_id': raw_data.get('points_payer_id'),
            'rewards_group': raw_data.get('rewards_group'),
            'rewards_product_partner_id': raw_data.get('rewards_product_partner_id'),
            'user_flagged_description': raw_data.get('user_flagged_description'),
            'metabrite_campaign_id': raw_data.get('metabrite_campaign_id'),
            'original_final_price': raw_data.get('original_final_price'),
            'original_meta_brite_barcode': raw_data.get('original_meta_brite_barcode'),
            'original_meta_brite_description': raw_data.get('original_meta_brite_description'),
            'original_meta_brite_quantity_purchased': raw_data.get('original_meta_brite_quantity_purchased'),
            'original_meta_brite_item_price': raw_data.get('original_meta_brite_item_price'),
            'original_receipt_item_text': raw_data.get('original_receipt_item_text'),
            'item_number': raw_data.get('item_number'),
            'competitive_product': raw_data.get('competitive_product'),
            'competitor_rewards_group': raw_data.get('competitor_rewards_group'),
            'deleted': raw_data.get('deleted', False)  # assuming default for 'deleted' is False
        }

