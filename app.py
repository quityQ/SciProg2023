

import classes.movies as mov
import classes.database as db
import classes.datamanip as dm 


df = mov.downloadMovies()
print("Movies downloaded")

db.createMoviesTable()
print("Table created")

dm.cleanup(df)
print("Data cleaned")

db.uploadMovieData(df)
print("Data uploaded")
