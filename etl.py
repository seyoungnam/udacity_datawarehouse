import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Description: This function copies raw data (log and song data) from S3 and loads them into staging_events and staging_songs tables.

    Parameters:
        cur: the cursor object.
        conn: the connection obejct to the cluster on Redshift.

    Returns:
        None
    """
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(e)
    
    print("Staging tables have been successfully loaded.")


def insert_tables(cur, conn):
    """
    Description: This function loads data from staging tables into fact and dimension tables.

    Parameters:
        cur: the cursor object.
        conn: the connection obejct to the cluster on Redshift.

    Returns:
        None
    """
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(e)
    
    print("Fact and dimension tables have been successfully loaded.")


def main():
    """
    Description: The main function reads arguments from dwh.cfg, passes them to psycopg2.connect function to construct the connection to the database, loads S3 resources to staging tables, and inserts data into fact and dimension tables for analytics. 

    Parameters:
        config: Configparser object.
        host: Amazon Redshift cluster address.
        dbname: Database name.
        user: Username to access the database.
        password: Password to access the database.
        port: Database port number.
        conn: the connection obejct to the cluster on Redshift.
        cur: the cursor obejct.

    Returns:
        None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()