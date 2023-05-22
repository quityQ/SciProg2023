import tmdbsimple as tmdb
import pandas as pd
import datetime
from tqdm import tqdm

tmdb.API_KEY = "60dde32875c0d3c5679496aba9fb3465"
tmdb.REQUESTS_TIMEOUT = 10



day = str(datetime.datetime.now().day - 1).zfill(2)
month = str(datetime.datetime.now().month).zfill(2)
year = str(datetime.datetime.now().year).zfill(4)

URL = f"http://files.tmdb.org/p/exports/movie_ids_{month}_{day}_{year}.json.gz"

def downloadIDlist():  
    dfidlist = pd.read_json(URL, compression="gzip", lines=True)
    print(f"There are {dfidlist.shape[0]} movies in the list")
    return dfidlist


def downloadMovies(numberOfMovies, idlist):
    df = pd.DataFrame()
    for i in tqdm(idlist["id"].head(numberOfMovies), total=numberOfMovies):
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

def downloadMoviesInChunks(dfidlist, chunksize, startchunk=0):
    df = pd.DataFrame()
    chunkcount = 0
    numOfChunks = round(dfidlist.shape[0] // chunksize) + 1
    for i in range(0, dfidlist.shape[0], chunksize):
        if startchunk > chunkcount:
            chunkcount += 1
            print(f"skipping chunk {chunkcount}/{numOfChunks}")
        else:    
            chunk = dfidlist.iloc[i:i+chunksize]
            chunkcount += 1
            print(f"processing chunk {chunkcount}/{numOfChunks}")
            df = pd.concat([df, downloadMovies(chunk.shape[0], chunk)], ignore_index=True)
            return df, chunkcount
    
