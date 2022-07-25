import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
from cryptography.fernet import Fernet
from cryto import get_seed

fernet = Fernet(get_seed())  # Used to decrypt the DB info


def drop_tables(cur, conn):
    """Drop all the tables inside Redshift Database

    Args:
        cur (cursor): Redshift DB Connection Cursor
        conn (Connection): RedshiftDB Connection
    """
    print("Staring --- to destroy all tables.")
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    print("Completed! --- destroy all tables.")


def create_tables(cur, conn):
    """Create all requried tables in the Redshift DB, Fact/Dim Tables and 2 Staging tables

    Args:
        cur (cursor): Redshift DB Connection Cursor
        conn (Connection): RedshiftDB Connection
    """
    print("Starting --- to create all tables.")
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    print("Completed! --- create all tables.")


def main():
    """
    Entry point of the python script
    """
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
    drop_tables(cur, conn)
    create_tables(cur, conn)
    print("-"*60)
    conn.close()


if __name__ == "__main__":
    main()
