### Receipts Data Schema

* **id** 
    * Desc: uuid for this receipt
    * Parsed from *\_id:**
    * is primary key
* **bonus_points_earned**
    * Desc: Number of bonus points that were awarded upon receipt completion
    * Nans filled with 0
* **bonus_points_earned_reason**
    * Desc: event that triggered bonus points
* **create_date**
    * Desc: The date that the event was created
* **date_scanned**
    * Desc: Date that the user scanned their receipt
* **finished_date**
    * Desc: Date that the receipt finished processing
* **modify_date**
    * Desc: The date the event was modified
* **points_awarded_date**
    * Desc: The date we awarded points for the transaction
* **points_earned**
    * Desc: The number of points earned for the receipt
* **purchase_date**
    * Desc: the date of the purchase
* **purchased_item_count**
    * Desc: Count of number of items on the receipt
* **rewards_receipt_item_list**
    * Desc: The items that were purchased on the receipt
* **rewards_receipt_status**
    * Desc: status of the receipt through receipt validation and processing
* **total_spent**
    * Desc: The total amount on the receipt
* **user_id**
    * Desc: string id back to the User collection for the user who scanned the receipt
    * Foreign key to users.id

### Users Data Schema
From my investigation this table contains many duplicate rows. Every duplicate user has the same row data, so when parsing the data, if the user is already in the database it is skipped.
* **id** 
    * Desc: user Id
    * Parsed from *\_id:**
    * is primary key
* **state**
    * Desc: state abbreviation
* **created_date**
    * Desc: when the user created their account
* **last_login**
    * Desc: last time the user was recorded logging in to the app
* **role**
    * Desc: constant value set to 'CONSUMER'
* **active**
    * Desc: indicates if the user is active; only Fetch will de\-activate an account with this flag

### Brand
* **id** 
    * Desc: brand uuid
    * Parsed from *\_id:**
    * is primary key
* **brand_code**
    * Desc: String that corresponds with the brand column in a partner product file
    * Desc: Brand name
* **barcode**
    * Desc: the barcode on the item
* **name**
* **category**
    * Desc: The category name for which the brand sells products in
* **category_code**
    * Desc: The category code that references a BrandCategory
* **cpg_id**
    * Desc: reference to CPG collection
* **top_brand**
    * Desc: Boolean indicator for whether the brand should be featured as a 'top brand'

### Transactions
* **barcode**
    * Desc: the barcode on the item
* **brand_code**
* **description**
* **item_price**
* **target_price**
* **price_after_coupon**
* **discounted_item_price**
* **final_price**
* **quantity_purchased**
* **needs_fetch_review**
* **partner_item_id**
* **prevent_target_gap_points**
* **user_flagged_barcode**
* **user_flagged_new_item**
* **user_flagged_price**
* **user_flagged_quantity**
* **needs_fetch_review_reason**
* **points_earned**
* **points_not_awarded_reason**
* **points_payer_id**
* **rewards_group**
* **rewards_product_partner_id**
* **user_flagged_description**
* **metabrite_campaign_id**
* **original_final_price**
* **original_meta_brite_barcode**
* **original_meta_brite_description**
* **original_meta_brite_quantity_purchased**
* **original_meta_brite_item_price**
* **original_receipt_item_text**
* **item_number**
* **competitive_product**
* **competitor_rewards_group**
* **deleted**