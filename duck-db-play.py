from random import random
import pandas as pd
import numpy as np
import duckdb
import time

import os
from dotenv import load_dotenv

# test of in-memory database select as pandas dataframe
results = duckdb.sql("SELECT 42").df()
print(results)

# create connection to file database
con = duckdb.connect('demo-duck.db')

# recreate table
con.sql('DROP TABLE IF EXISTS numbers')
con.sql('CREATE TABLE numbers(val INTEGER)')

# insert 3 random integers
for i in range(3):
    val = int(random() * 1000)
    con.execute('INSERT INTO numbers (val) VALUES (?)', (val,))

# select all rows from table
con.sql('SELECT * FROM numbers').show()

# use pandas to create a dataframe with 100 million random integers
df = pd.DataFrame(np.random.randint(1, 100, size=100000000), columns=['val'])

# register the dataframe as a DuckDB view
con.register('random_ints_view', df)

con.sql('DROP TABLE IF EXISTS random_ints_table')

# create table by copying the view data
start = time.time()
con.sql('CREATE TABLE random_ints_table AS SELECT * FROM random_ints_view')
end = time.time()
print('Time to copy data: ', end - start)

# Select average from the filtered table. Measure the time it takes to execute the query.
start = time.time()
con.sql('SELECT avg(val) FROM random_ints_table where val % 2 == 0 LIMIT 10').show()
end = time.time()
print('Time to execute query: ', end - start)

# download NYV taxi data with command line or manually
# https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

# Can be done by the commands below (saved to data folder)
#  wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-04.parquet -P data
#  wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-05.parquet -P data

# show one row
con.sql("SELECT * FROM 'data/*2023*.parquet' LIMIT 1").show()

# select records where hour of tpep_pickup_datetime field is between 17 and 20
start = time.time()
con.sql("SELECT count(*) as peak_time_trips FROM 'data/*2023*.parquet' WHERE hour(tpep_pickup_datetime) BETWEEN 17 AND 20").show()
con.sql("SELECT count(*) as night_trips FROM 'data/*2023*.parquet' WHERE hour(tpep_pickup_datetime) = 23 or hour(tpep_pickup_datetime) <= 1 ").show()
end = time.time()
print('Time to execute queries: ', end - start)

# do some aggregations and join parquet data with csv data
start = time.time()
df = con.sql('''
SELECT
    hour(tpep_pickup_datetime), sum(passenger_count), avg(passenger_count), avg(trip_distance),
    avg(fare_amount), avg(tip_amount), avg(fare_amount - cs.amount) as final_amount_average, sum(cs.amount) as compensation
FROM 'data/*2023*.parquet' t JOIN 'data/compensation.csv' cs on hour(t.tpep_pickup_datetime) = cs.hour
GROUP BY hour(tpep_pickup_datetime)
ORDER BY hour(tpep_pickup_datetime);
''').df()
end = time.time()
print('Time to execute query: ', end - start)
print(df.sample(5))

# register dataframe as a DuckDB view
con.register('compensated_nyc_trips', df)

# upload data from DuckDB to S3 (can also be Google Cloud or other S3 API compatible service)

# install and activate httpfs plugin for DuckDB
con.sql('INSTALL httpfs')
con.sql('LOAD httpfs')

# load environment variables
load_dotenv()

# get S3 credentials from environment variables using dotenv
s3_bucket = os.getenv('S3_BUCKET')
s3_key = os.getenv('S3_KEY')
s3_secret = os.getenv('S3_SECRET')
s3_region = os.getenv('S3_REGION')

# set S3 credentials for DuckDB
con.sql(f"SET s3_access_key_id='{s3_key}'")
con.sql(f"SET s3_secret_access_key='{s3_secret}'")
con.sql(f"SET s3_region='{s3_region}'")

con.sql(f"COPY compensated_nyc_trips to 's3://{s3_bucket}/data/compensated_nyc_trips.parquet'")
print('Data copied to S3')