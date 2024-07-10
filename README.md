## sg_de_challenge

Steps to run the code:

1.  Create a new Python environment using `python -m venv .venv`.
2.  Activate the new environment.
3.  Install dependencies with `pip install -r requirements.txt`.
4.  Execute the “challenge.py” file inside the "src" folder.

### Data Engineering in Production questions

> Using typical sales data as an example, how would you ensure that a data pipeline is kept  
> up to date with accurate data? What tools or processes might you use so that sales data is  
> updated daily?

I would use a replication service like AWS DMS to pull the latest version of the transactional tables in a Change Data Capture fashion,  
storing the tables in a relational database (like Postgres) or into AWS S3 and using a Data Lake approach from there.  
Then, I would use an orchestrator like Apache Airflow to load the required data into a Data Warehouse, like BigQuery or AWS Redshift.  
Using an orchestrator would be the best approach as it allows for the reprocessing of dates that the data pipeline needs to be re-run for, as it uses a logical date.

> Our sales and product data are constantly changing—returns can affect previous sales, and  
> pricing changes can affect product data tables, etc. How would you go about building a data  
> pipeline that is able to add new data while also changing or updating existing data that has  
> changed at the source system?

I would implement a dimensional model with Dimension and Fact tables, in the style of Kimball.  
Specifically, Type 2 Dimensions, to keep track of versions, and Fact tables with surrogate keys.  
I would use UPSERTS (INSERT or UPDATE) using a MERGE operation on the DW Database.  
For product returns, I would recommend inserting new Fact records to cancel previous amounts instead of updating the original records so the Accounting/Finance area has all the transactions. The same applies to chargebacks.  
Again, the data pipeline could be programmed in Airflow, using Python and SQL statements.  
The DBT framework could be leveraged for the dimensional modeling, more specifically, using the Snapshots feature to create and update the Dimension tables, and include source freshness and custom test on the data using plain SQL.