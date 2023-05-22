import classes.movies as mov
import classes.database as db
import classes.datamanip as dm 

table_name = "all_movies"



dfidlist = mov.downloadIDlist()
print("ID list downloaded")

print("Downloading movies")
df = mov.downloadMovies(dfidlist.shape[0], dfidlist)
print("Movies downloaded")

print("Creating table")
db.createMoviesTable(table_name)
print("Table created")

print("Cleaning data")
df = dm.cleanup(df)
print("Data cleaned")

print("Uploading data")
db.uploadMovieData(df, table_name)
print("Data uploaded")