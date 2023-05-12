#Initialize project

import classes.movies as mov
import classes.database as db



df = mov.downloadMovies()
print("Movies downloaded")

db.createMoviesTable()
print("Table created")

db.uploadMovieData(df)
print("Data uploaded")
