import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from cryptography.fernet import Fernet
from cryto import get_seed
import time
from datetime import timedelta

fernet = Fernet(get_seed())  # Used to decrypt the DB info
l_staging_tables = ["staging_events", "staging_songs"]
l_insert_tables = ["songplay_table_insert", "user_table_insert",
                   "song_table_insert", "artist_table_insert", "time_table_insert"]


def load_staging_tables(cur, conn):
    for i, query in enumerate(copy_table_queries):
        start = time.time()
        print("Starting --- to copy data into {}".format(
            l_staging_tables[i]))
        cur.execute(query)
        conn.commit()
        end = time.time()
        print("Completed! --- to load data into {} after {}".format(
            l_staging_tables[i], timedelta(seconds=end-start)))
    print("**Completed!** to Insert all data to staging tables!")


def insert_tables(cur, conn):
    for i, query in enumerate(insert_table_queries):
        start = time.time()
        print("Starting --- to copy data into {}".format(l_insert_tables[i]))
        cur.execute(query)
        conn.commit()
        end = time.time()
        print(
            "Completed! --- to load data into {} after {}".format(l_insert_tables[i], timedelta(seconds=end-start)))
    print("**Completed!** to Insert all tables!")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    db_host = fernet.decrypt(config['CLUSTER']["HOST"].encode()).decode()
    db_name = fernet.decrypt(config['CLUSTER']["DB_NAME"].encode()).decode()
    db_username = fernet.decrypt(
        config['CLUSTER']["DB_USER"].encode()).decode()
    db_password = fernet.decrypt(
        config['CLUSTER']["DB_PASSWORD"].encode()).decode()

    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
            db_host,
            db_name,
            db_username,
            db_password,
            config['CLUSTER']["DB_PORT"]
        ))

        cur = conn.cursor()
    except Exception as e:
        print(e)
    print("Connected!")

    load_staging_tables(cur, conn)
    print("-"*60)
    insert_tables(cur, conn)
    print("-"*60)
    conn.close()


if __name__ == "__main__":
    main()
