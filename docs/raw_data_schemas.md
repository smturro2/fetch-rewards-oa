
The Data
--------

### Receipts Data Schema

[Download
 receipts.json.gz](https://fetch-hiring.s3.amazonaws.com/analytics-engineer/ineeddata-data-modeling/receipts.json.gz)
* **\_id:** uuid for this receipt
* **bonusPointsEarned:** Number of bonus points that were awarded upon receipt completion
* **bonusPointsEarnedReason:** event that triggered bonus points
* **createDate:** The date that the event was created
* **dateScanned:** Date that the user scanned their receipt
* **finishedDate:** Date that the receipt finished processing
* **modifyDate:** The date the event was modified
* **pointsAwardedDate:** The date we awarded points for the transaction
* **pointsEarned:** The number of points earned for the receipt
* **purchaseDate:** the date of the purchase
* **purchasedItemCount:** Count of number of items on the receipt
* **rewardsReceiptItemList:** The items that were purchased on the receipt
* **rewardsReceiptStatus:** status of the receipt through receipt validation and processing
* **totalSpent:** The total amount on the receipt
* **userId:** string id back to the User collection for the user who scanned the receipt

### Users Data Schema

[Download
 users.json.gz](https://fetch-hiring.s3.amazonaws.com/analytics-engineer/ineeddata-data-modeling/users.json.gz)
* **\_id:** user Id
* **state:** state abbreviation
* **createdDate:** when the user created their account
* **lastLogin:** last time the user was recorded logging in to the app
* **role:** constant value set to 'CONSUMER'
* **active:** indicates if the user is active; only Fetch will de\-activate an account with this flag

### Brand Data Schema

[Download
 brands.json.gz](https://fetch-hiring.s3.amazonaws.com/analytics-engineer/ineeddata-data-modeling/brands.json.gz)
* **\_id:** brand uuid
* **barcode:** the barcode on the item
* **brandCode:** String that corresponds with the brand column in a partner product file
* **category:** The category name for which the brand sells products in
* **categoryCode:** The category code that references a BrandCategory
* **cpg:** reference to CPG collection
* **topBrand:** Boolean indicator for whether the brand should be featured as a 'top brand'
* **name:** Brand name