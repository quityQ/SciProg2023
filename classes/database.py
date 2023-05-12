import psycopg2
import pandas as pd
from sqlalchemy import create_engine

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

# Testing connectoin to DB
def testConnection():
    try:
        cursor = connection.cursor()
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
    try:
        df.to_sql("movies", engine, if_exists="append", index=False)
        print("Data uploaded")
    except (Exception, psycopg2.Error) as error:
        print("Error while uploading data", error)
    finally:
        if connection:
            connection.close()
            print("PostgreSQL connection is closed")


def createMoviesTable():
    try:
        cursor = connection.cursor()
        create_table_query = """CREATE TABLE IF NOT EXISTS movies
              (id SERIAL PRIMARY KEY,
              title TEXT,
              budget INT,
              genres TEXT,
              original_language TEXT,
              original_title TEXT,
              popularity FLOAT,
              production_companies TEXT,
              production_countries TEXT,
              release_date TEXT,
              revenue INT,
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
        if connection:
            connection.close()
            print("PostgreSQL connection is closed")

def dropMoviesTable():
    try:
        cursor = connection.cursor()
        cursor.open()
        drop_table_query = """DROP TABLE movies"""
        cursor.execute(drop_table_query)
        connection.commit()
        print("Table dropped")
    except (Exception, psycopg2.Error) as error:
        print("Error while dropping table", error)
    finally:
        if cursor:
            cursor.close()  
        if connection:
            connection.close()
            print("PostgreSQL connection is closed")