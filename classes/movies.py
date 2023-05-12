import tmdbsimple as tmdb
import pandas as pd
import datetime

tmdb.API_KEY = "60dde32875c0d3c5679496aba9fb3465"
tmdb.REQUESTS_TIMEOUT = 10

day = str(datetime.datetime.now().day-1).zfill(2)
month = str(datetime.datetime.now().month).zfill(2)
year = str(datetime.datetime.now().year).zfill(4)

df = pd.DataFrame()
URL = f"http://files.tmdb.org/p/exports/movie_ids_{month}_{day}_{year}.json.gz"
dfidlist = pd.read_json(URL, compression="gzip", lines=True)

def downloadMovies():
    for i in dfidlist["id"]:
        movie = tmdb.Movies(i)
        response = movie.info()
        tempdf = pd.DataFrame(
            [
                [
                    movie.title,
                    movie.budget,
                    movie.genres,
                    movie.original_language,
                    movie.original_title,
                    movie.popularity,
                    movie.production_companies,
                    movie.production_countries,
                    movie.release_date,
                    movie.revenue,
                    movie.runtime,
                    movie.spoken_languages,
                    movie.status,
                    movie.vote_average,
                    movie.vote_count,
                    movie.adult,
                ]
            ],
            columns=[
                "title",
                "budget",
                "genres",
                "original_language",
                "original_title",
                "popularity",
                "production_companies",
                "production_countries",
                "release_date",
                "revenue",
                "runtime",
                "spoken_languages",
                "status",
                "vote_average",
                "vote_count",
                "adult",
            ],
        )
    df = pd.concat([df, tempdf], ignore_index=True)

    return df
