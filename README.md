# Fetch Rewards Online Assessment
This repo is a submission for Fetch Reward's Analytics Engineer Internship.
The rest of this readme will cover each question and 
provide an answer or show you where to find the answer.

see [instructions.md](docs/instructions.md) for the original instructions.

# To Fetch Rewards
There is a docker-compose file which runs a postgres database. If you run this container,
you will be able to run the code in the src folder. 
The Database class will connect to this database, create the tables, and insert the data.

# 1. Database Design
See _docs\database_design.md_

# 2. Queries
See _src\queries.ipynb_

# 3. Data Issues
See Issues in _src\data_exploration.ipynb_

# 4. Email to Stakeholder
 
```email
Hello,

I hope this email finds you well. Yesterday, I analyzed the data and looked at key metrics 
such as the percentage of missing values and the uniqueness of entries. 
Here are some potential issues I found:

- Half of the rows in the Users table are duplicated.
- User IDs in the Receipts table do not exist in the Users table.
- Stale data: The last user was created in 2021.
- Inconsistencies in the Brands table with many missing barcodes and brand codes.

I would like to gain a better understanding of the Brands data and how these tables will scale in the future. 
Do you have time in the next week or two for a 30-minute chat?

Thanks,
Scott Turro
```