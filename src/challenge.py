
import logging
import json
import os
import shutil
import requests
import pandas as pd
import duckdb
from datetime import datetime

# Use a logger instead of simple print statements for better compatibility with orchestrators and logging services (like AWS CloudWatch)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

API_ROOT = "https://api.frankfurter.app/"

#TODO: Move "CURRENCIES" and "TO_CURRENCY" to a separate YAML file. That would allow to update the values without changing the code.
CURRENCIES = {
	"AUD": "Australian Dollar",
	"BGN": "Bulgarian Lev",
	"BRL": "Brazilian Real",
	"CAD": "Canadian Dollar",
	"CHF": "Swiss Franc",
	"CNY": "Chinese Renminbi Yuan",
	"CZK": "Czech Koruna",
	"DKK": "Danish Krone",
	"EUR": "Euro",
	"GBP": "British Pound",
	"HKD": "Hong Kong Dollar",
	"HUF": "Hungarian Forint",
	"IDR": "Indonesian Rupiah",
	"ILS": "Israeli New Sheqel",
	"INR": "Indian Rupee",
	"ISK": "Icelandic Króna",
	"JPY": "Japanese Yen",
	"KRW": "South Korean Won",
	"MXN": "Mexican Peso",
	"MYR": "Malaysian Ringgit",
	"NOK": "Norwegian Krone",
	"NZD": "New Zealand Dollar",
	"PHP": "Philippine Peso",
	"PLN": "Polish Złoty",
	"RON": "Romanian Leu",
	"SEK": "Swedish Krona",
	"SGD": "Singapore Dollar",
	"THB": "Thai Baht",
	"TRY": "Turkish Lira",
	"USD": "United States Dollar",
    "ZAR": "South African Rand"
}

TO_CURRENCY = "EUR"
# Remove the target currency because it cannot be converted to itself
FROM_CURRENCIES = [c for c in CURRENCIES.keys() if c != TO_CURRENCY]

def get_file_paths(directory):
    # get full path and basename for all files in directory
    files = []
    for entry in os.scandir(f'{directory}/'):
        if entry.is_file():
            files.append({"path": entry.path, "name": os.path.basename(entry.path)})
        else:
            # if the entry is a directory, recursively call get_file_paths
            files.extend(get_file_paths(entry.path))
    return files

def get_exchange_rates(api_root:str, date:str, from_currency:str, to_currency:str):
	''' 
	Returns the exchange rate between two currencies on a given date
	'''
	url = api_root + date + "?from=" + from_currency + "&to=" + to_currency
	r = requests.get(url)
	log.info(f'Requested url: {url} with HTTP status code {r.status_code}')

	#Extract and transform the data
	data = r.json()
	to_currency, to_amount = list(data['rates'].items())[0]
	output_data = {
		"date": data["date"],
		"from_currency": data["base"],
		"amount": data["amount"],
		"to_currency": to_currency,
		"exchange_rate": to_amount
	}
	return json.dumps(output_data)

def store_exchange_rates(api_response:str, path:str):
	''' 
	Takes the API response and stores the exchange rate between two currencies on a given date
	'''
	r = json.loads(api_response)
	ds = r["date"]
	from_currency = r["from_currency"]
	to_currency = r["to_currency"]
	
	#Use Pandas to convert to the required file format
	df = pd.json_normalize(r)
	#Add an ETL timestamp to know when the data was processed
	df = df.assign(updated_at=datetime.now())
	# I would use the parquet file format, but for simplicity's sake I'm using csv
	output_path = f'{path}/{from_currency}_{to_currency}_{ds}.csv'
	df.to_csv(output_path, index=False)

	log.info(f'Stored exchange rates for {from_currency} to {to_currency} on {ds} in {output_path}')


# The following code block performs a one-time export for the exchange rates for all the dates
dates = ['2024-02-12', '2024-02-13']

data_path = '../data'
rates_path = f'{data_path}/exchange_rates'

for ds in dates:
    # Create or replace a folder named after the date. Using the date allows for partitioning the data in
    # the future for improved performance (following Apache Hive folder structure and enabling the ability to push predicates down).
    dir = f'{rates_path}/{ds}'
    if os.path.exists(dir):
        shutil.rmtree(dir)
    os.makedirs(dir)
    for currency in FROM_CURRENCIES:
        try:
            r = get_exchange_rates(API_ROOT, ds, currency, TO_CURRENCY)
            store_exchange_rates(r, dir)
        except Exception as e:
            log.error(e)

# I'm going full "scripting" mode starting here; the following transformations could be refactored into function or classes with more time.

# Get ecommerce data
STORE_ENDPOINT = "https://fakestoreapi.com/products/category/women%27s%20clothing"
r = requests.get(STORE_ENDPOINT).json()
store_items = pd.DataFrame(r)

#Explode "rating" (dict to columns)
store_items = store_items.join(pd.DataFrame(store_items["rating"].tolist())[["rate", "count"]])
store_items.drop("rating", axis=1, inplace=True)

store_items.rename(columns={"price": "price_usd", "rate": "rating_rate", "count": "rating_count"}, inplace=True)

# Pull the exchange rates from stored files
rate_files = get_file_paths(rates_path)

# I'm pulling all the rates datasets, but in a real scenario I would only pull the required dates and currencies (for example, using AWS S3 and 
# the AWS Athena query engine over parquet files)
rates_df = pd.concat((pd.read_csv(f["path"]) for f in rate_files), ignore_index=True)

# Only keep USD rates for the required dates
rates_df = rates_df[(rates_df["date"].isin(dates)) & (rates_df["from_currency"] == "USD")]
# Drop unneeded columns
rates_df.drop(columns=['from_currency', 'amount', 'to_currency','updated_at'], inplace=True)
rates_df.rename(columns={'date': 'exchange_rate_date'}, inplace=True)

# Merge exchange rates with product data using a cross join
products_df = store_items.merge(rates_df, how='cross')
# Calculate price in EUROS
products_df["price_eur"] = products_df["price_usd"] * products_df["exchange_rate"]

# I'm using the DuckDB library to mimic a relational database here.
# Check the "DDL_products_big_query.sql" file for the actual table creation script for BigQuery.

# Create statement for the final table
ddl_query = """
        -- Drop the table if it exists
        DROP TABLE IF EXISTS top_products;

        -- Create the table with appropriate data types
        CREATE TABLE top_products (
            id INTEGER,
            title STRING,
            price_usd FLOAT,
            description STRING,
            category STRING,
            image STRING,
            rating_rate FLOAT,
            rating_count INTEGER,
            exchange_rate_date DATE,
            exchange_rate FLOAT,
            price_eur FLOAT
        );
    """
# I'm assuming that the required Top 5 products should be just five items, otherwise this would be a DENSE_RANK().
# In a real scenario this would be improved, for example, using other fields for cases where there are multiple items with the exact same rank.
top_products_query ="""
        INSERT INTO
            top_products
        SELECT
            id,
            title,
            price_usd,
            description,
            category,
            image,
            rating_rate,
            rating_count,
            exchange_rate_date,
            exchange_rate,
            price_eur
        FROM
            (
                SELECT
                    id,
                    title,
                    price_usd,
                    description,
                    category,
                    image,
                    rating_rate,
                    rating_count,
                    exchange_rate_date,
                    exchange_rate,
                    price_eur,
                    rank() OVER (
                        PARTITION BY exchange_rate_date
                        ORDER BY
                            rating_rate DESC
                    ) AS rank
                FROM
                    all_products
            )
        WHERE
            rank <= 5
    """

# Get the data from the "products_df" DataFrame into DuckDB
duckdb.sql("CREATE OR REPLACE TABLE all_products AS SELECT * FROM products_df")
# Create the "top_products" table
duckdb.sql(ddl_query)
# Insert the top products into the "top_products" table using the "top_products_query" query
duckdb.sql(top_products_query)
# Get the top products from the "top_products" table
top_products_df = duckdb.sql("SELECT * FROM top_products").to_df()
#Show the results
log.info("Top products dataframe:")

with pd.option_context('display.min_rows', 10, 'display.max_columns', 11):
     log.info(top_products_df)