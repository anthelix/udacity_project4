#!/usr/bin/python3

import psycopg2
from sql_queries import create_table_queries, drop_table_queries
from pathlib import Path
import pandas as pd

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

# connect to database
connect_str = 'host=postgres port=5432 dbname=bakery user=postgres password=postgres1234'
conn = psycopg2.connect(connect_str)
conn.autocommit = True
cursor = conn.cursor()

drop_tables(cursor, conn)
create_tables(cursor, conn)

# import data from parquet file
# try to import data from output/parquet file and load in database
data_dir = Path('output/users_table')
full_df = pd.concat(
    pd.read_parquet(parquet_file)
    for parquet_file in data_dir.glob('*.parquet')
)
full_df.to_csv('users_table.csv')

with open('users_table.csv', 'r') as f:
    next(f)  # Skip the header row.
    cursor.copy_from(
        f,
        'user_table',
        sep=',',
        columns=('user_id', 'first_name', 'last_name', 'gender', 'level')
    )
    conn.commit()


conn.close()