import duckdb
import time

import os
from dotenv import load_dotenv

# load environment variables
load_dotenv()

# get motherduck token from environment variable
token = os.getenv('MOTHERDUCK_TOKEN')

# connect to motherduck with token
con = duckdb.connect(f'md:?motherduck_token={token}')

# connect to motherduck without token, using browser-based authentication
# con = duckdb.connect(f'md:')

# show databases
con.sql('SHOW DATABASES').show()

# use default database named my_db
con.sql('use my_db')

# assuming the data is already loaded into the motherduck database
# test query timing
start = time.time()
df = con.sql('''
SELECT hour(tpep_pickup_datetime), sum(passenger_count), avg(passenger_count), avg(trip_distance), avg(fare_amount), avg(tip_amount)
FROM my_db.nyc_trips
GROUP BY hour(tpep_pickup_datetime)
ORDER BY hour(tpep_pickup_datetime);
''').df()
end = time.time()
print('Time to execute query: ', end - start)
print(df.sample(5))


# join cloud database with local data from csv file
start = time.time()
df = con.sql('''
SELECT
    hour(tpep_pickup_datetime), sum(passenger_count), avg(passenger_count), avg(trip_distance),
    avg(fare_amount), avg(tip_amount), avg(fare_amount - cs.amount) as final_amount_average, sum(cs.amount) as compensation
FROM my_db.nyc_trips t JOIN 'data/compensation.csv' cs on hour(t.tpep_pickup_datetime) = cs.hour
GROUP BY hour(tpep_pickup_datetime)
ORDER BY hour(tpep_pickup_datetime);
''').df()
end = time.time()
print('Time to execute query: ', end - start)
print(df.sample(5))

# register dataframe as a DuckDB view
con.register('compensated_nyc_trips', df)

# upload data from MotherDuck to S3 (can also be Google Cloud or other S3 API compatible service)
con.sql('INSTALL httpfs')
con.sql('LOAD httpfs')

# get S3 credentials from environment variables using dotenv
s3_bucket = os.getenv('S3_BUCKET')
s3_key = os.getenv('S3_KEY')
s3_secret = os.getenv('S3_SECRET')
s3_region = os.getenv('S3_REGION')

con.sql(f"SET s3_access_key_id='{s3_key}'")
con.sql(f"SET s3_secret_access_key='{s3_secret}'")
con.sql(f"SET s3_region='{s3_region}'")

con.sql(f"COPY compensated_nyc_trips to 's3://{s3_bucket}/data/compensated_nyc_trips.parquet'")
print('Data copied to S3')