import classes.movies as mov
import classes.database as db
import classes.datamanip as dm 

table_name = "all_movies"
chunksize = 10000
dfidlist = mov.downloadIDlist()
print("ID list downloaded")

numOfChunks = round(dfidlist.shape[0] // chunksize) + 1

print(f"There will be {numOfChunks} chunks")

print("Creating table")
db.createMoviesTable(table_name)
print("Table created")

for i in range(numOfChunks):
    print("Downloading movies")
    df = mov.downloadMoviesInChunks(dfidlist, chunksize)

    print("Cleaning data")
    df = dm.cleanup(df)
    print("Data cleaned")

    print("Uploading data")
    db.uploadMovieData(df, table_name)
    print("Data uploaded")