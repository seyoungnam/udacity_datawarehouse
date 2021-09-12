import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Description: This function drops any existing tables (fact, dimension, and staging tables) on Redshift.

    Parameters:
        cur: the cursor object.
        conn: the connection obejct to the cluster on Redshift.

    Returns:
        None
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(e)
    
    print("Existing tables have dropped.")


def create_tables(cur, conn):
    """
    Description: This function creates fact, dimension, and fact tables on Redshift.

    Parameters:
        cur: the cursor object.
        conn: the connection obejct to the cluster on Redshift.

    Returns:
        None
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(e)
    
    print("New tables have been created.")


def main():
    """
    Description: The main function reads arguments from dwh.cfg, passes them to psycopg2.connect function to construct the connection to the database, drops any existing tables, and creates new tables on the AWS Redshift. 

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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()