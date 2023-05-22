import classes.movies as mov
import classes.database as db
import classes.datamanip as dm 

table_name = input("Enter table name: ")
chunksize = eval(input("Enter chunksize: "))
startchunk = eval(input("Enter startchunk (0 if first time): "))
dfidlist = mov.downloadIDlist()
print("ID list downloaded")

numOfChunks = round(dfidlist.shape[0] // chunksize) + 1

print(f"There will be {numOfChunks} chunks")


db.connect()
print("Creating table")
db.createMoviesTable(table_name)
db.shutdown()

for i in range(numOfChunks):
    print("Downloading movies")
    df, startchunk = mov.downloadMoviesInChunks(dfidlist, chunksize, startchunk)

    print("Cleaning data")
    df = dm.cleanup(df)
    print("Data cleaned")
    db.connect()
    print("Uploading data")
    db.uploadMovieData(df, table_name)
    print("Data uploaded")
    db.shutdown()

