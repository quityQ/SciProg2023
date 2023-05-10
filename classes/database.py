import psycopg2
import pandas as pd


connection = psycopg2.connect(user="bicicayb",
                              password="sIrAKRSC",
                              host="db-moviedata.cxqntpnwnjaq.eu-north-1.rds.amazonaws.com",
                              port="5432",
                              database="postgres")

cursor = connection.cursor()

