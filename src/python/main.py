from database import ConsumerDatabase

db = ConsumerDatabase()

# Check connection to the database
db.check_connection()

db.get_top_5_brands_recent_month()
db.compare_top_5_brands_recent_previous_month()
db.compare_average_spend()
db.compare_total_items_purchased()
db.brand_with_most_spend_recent_users()
db.brand_with_most_transactions_recent_users()