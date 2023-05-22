import psycopg2
import pandas as pd
from sqlalchemy import create_engine




def connect():
    global connection
    global engine
    global getConn
    connection = psycopg2.connect(
        user="bicicayb",
        password="sIrAKRSC",
        host="db-moviedata.cxqntpnwnjaq.eu-north-1.rds.amazonaws.com",
        port="5432",
        database="postgres",
    )
    engine = create_engine(
        "postgresql+psycopg2://bicicayb:sIrAKRSC@db-moviedata.cxqntpnwnjaq.eu-north-1.rds.amazonaws.com:5432/postgres"
    )

    getConn = engine.connect()

# Testing connectoin to DB
def testConnection():
    cursor = connection.cursor()
    try:
        print(connection.get_dsn_parameters(), "\n")
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print("You are connected to - ", record, "\n")
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to DB", error)
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            print("PostgreSQL connection is closed")


# todo take df as argument
def uploadMovieData(df):
    cursor = connection.cursor()
    try:
        df.to_sql("movies", engine, if_exists="append", index=False)
        print("Data uploaded")
    except (Exception, psycopg2.Error) as error:
        print("Error while uploading data", error)
    finally:
        if cursor:
            cursor.close()

def getMovieData():
    cursor = connection.cursor()
    df = pd.DataFrame()
    try:
        df = pd.read_sql_table("movies", getConn)
        print("Data downloaded")
    except (Exception, psycopg2.Error) as error:
        print("Error while downloading data", error)
    finally:
        if cursor:
            cursor.close()
    return df



def createMoviesTable(table_name):
    cursor = connection.cursor()    
    try:
        create_table_query = f"""CREATE TABLE IF NOT EXISTS {table_name}
              (id SERIAL PRIMARY KEY,
              title TEXT,
              budget BIGINT,
              genres TEXT,
              original_language TEXT,
              original_title TEXT,
              popularity FLOAT,
              production_companies TEXT,
              production_countries TEXT,
              release_date TEXT,
              revenue BIGINT,
              runtime INT,
              spoken_languages TEXT,
              status TEXT,
              vote_average FLOAT,
              vote_count INT,
              adult BOOLEAN); """
        cursor.execute(create_table_query)
        connection.commit()
        print("Table created")
    except (Exception, psycopg2.Error) as error:
        print("Error while creating table", error)
    finally:
        if cursor:
            cursor.close()

def dropMoviesTable():
    cursor = connection.cursor()    
    try:
        drop_table_query = """DROP TABLE movies"""
        cursor.execute(drop_table_query)
        connection.commit()
        print("Table dropped")
    except (Exception, psycopg2.Error) as error:
        print("Error while dropping table", error)
    finally:
        if cursor:
            cursor.close()


def shutdown():
    if connection:
        connection.close()
        print("PostgreSQL connection is closed")
