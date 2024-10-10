from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer, Float, Boolean, ForeignKey, DateTime, JSON

from .utils import convert_to_snake_case, parse_dates, parse_floats

Base = declarative_base()


class Receipt(Base):
    __tablename__ = 'receipts'
    id = Column(String, primary_key=True)
    bonus_points_earned = Column(Float, default=0)
    bonus_points_earned_reason = Column(String)
    create_date = Column(DateTime)
    date_scanned = Column(DateTime)
    finished_date = Column(DateTime)
    modify_date = Column(DateTime)
    points_awarded_date = Column(DateTime)
    points_earned = Column(Float)
    purchase_date = Column(DateTime)
    purchased_item_count = Column(Integer)
    rewards_receipt_status = Column(String)
    total_spent = Column(Float)
    user_id = Column(String,
                     #ForeignKey('users.id')  # Uncommented because we are missing users
              )

class User(Base):
    __tablename__ = 'users'
    id = Column(String, primary_key=True)
    state = Column(String)
    created_date = Column(DateTime)
    last_login = Column(DateTime)
    role = Column(String, default='CONSUMER')
    active = Column(Boolean)
    sign_up_source = Column(String)



class Brand(Base):
    __tablename__ = 'brands'
    id = Column(String, primary_key=True)
    brand_code = Column(String,
                        # unique=True
                        )
    barcode = Column(String)
    name = Column(String)
    category = Column(String)
    category_code = Column(String)
    cpg_id = Column(String)
    cpg_ref = Column(String)
    top_brand = Column(Boolean)


class Transaction(Base):
    __tablename__ = 'transactions'
    id = Column(Integer, primary_key=True, autoincrement=True)
    receipt_id = Column(String, ForeignKey('receipts.id'))
    barcode = Column(String)
    brand_code = Column(String,
                        # ForeignKey('brands.brand_code') Not unique brand_code
                        )
    description = Column(String)
    item_price = Column(Float)
    target_price = Column(Float)
    price_after_coupon = Column(Float)
    discounted_item_price = Column(Float)
    final_price = Column(Float)
    quantity_purchased = Column(Integer)
    needs_fetch_review = Column(Boolean)
    partner_item_id = Column(String)
    prevent_target_gap_points = Column(Boolean)
    user_flagged_barcode = Column(String)
    user_flagged_new_item = Column(Boolean)
    user_flagged_price = Column(Float)
    user_flagged_quantity = Column(Integer)
    needs_fetch_review_reason = Column(String)
    points_earned = Column(Float)
    points_not_awarded_reason = Column(String)
    points_payer_id = Column(String)
    rewards_group = Column(String)
    rewards_product_partner_id = Column(String)
    user_flagged_description = Column(String)
    metabrite_campaign_id = Column(String)
    original_final_price = Column(Float)
    original_meta_brite_barcode = Column(String)
    original_meta_brite_description = Column(String)
    original_meta_brite_quantity_purchased = Column(Integer)
    original_meta_brite_item_price = Column(Float)
    original_receipt_item_text = Column(String)
    item_number = Column(String)
    competitive_product = Column(Boolean)
    competitor_rewards_group = Column(String)
    deleted = Column(Boolean, default=False)

