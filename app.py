import classes.movies as mov
import classes.database as db
import classes.datamanip as dm 

table_name = "all_movies"


dfidlist = mov.downloadIDlist()

df = mov.downloadMovies(dfidlist.shape[0], dfidlist)
print("Movies downloaded")

db.createMoviesTable(table_name)
print("Table created")

df = dm.cleanup(df)
print("Data cleaned")

db.uploadMovieData(df)
print("Data uploaded")
