-- Assuming the table is stored under a schema named 'dw'
-- Drop the table if it exists
DROP TABLE IF EXISTS dw.top_products;

-- Create the table with appropriate data types
CREATE TABLE dw.top_products (
  id INT64,
  title STRING,
  price_usd FLOAT64,
  description STRING,
  category STRING,
  image STRING,
  rating_rate FLOAT64,
  rating_count INT64,
  exchange_rate_date DATE,
  exchange_rate FLOAT64,
  price_eur FLOAT64
);
