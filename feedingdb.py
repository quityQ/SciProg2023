import tmdbsimple as tmdb
import psycopg2

tmdb.API_KEY = "60dde32875c0d3c5679496aba9fb3465"
tmdb.REQUESTS_TIMEOUT = 10

connection = psycopg2.connect(user="bicicayb",
                              password="sIrAKRSC",
                              host="db-moviedata.cxqntpnwnjaq.eu-north-1.rds.amazonaws.com",
                              port="5432",
                              database="postgres")

cursor = connection.cursor()

