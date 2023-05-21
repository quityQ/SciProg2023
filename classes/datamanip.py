import ast
import numpy as np

#Some entries in the Dataset are given in JSON format
#Here we extract the relevant parts of those lists (whatever name is given)
#Following columns may have JSON Format: genres, production_companies, production_countries, spoken_languages
def extractDict(input):
    outputList = []
    tempList = []
    input = str(input).replace("[","")
    input = str(input).replace("]","")
    input = str(input).replace('"','')
    if input == "":
        return 
    dictentry = input
    input = ast.literal_eval(dictentry)
    if type(input) == tuple:
        for i in input:
          tempList = i.items()
          for x in tempList:
            if "english_name" in x:
                outputList.append(x[1])
            elif "name" in x:
                outputList.append(x[1])
    else:
        tempList = input.items()
        for x in tempList:
            if "english_name" in x:
                outputList.append(x[1])
            elif "name" in x:
                outputList.append(x[1])
    return outputList

#cleanup the dataframe by replacing 0 with NaN and making lists prettier
def cleanup(df):
    df["budget"] = df["budget"].replace(0, np.nan)
    df["revenue"] = df["revenue"].replace(0, np.nan)
    df["vote_average"] = df["vote_average"].replace(0, np.nan)
    df["vote_count"] = df["vote_count"].replace(0,np.nan)
    df["genres"] = df["genres"].apply(extractDict)
    df["production_companies"] = df["production_companies"].apply(extractDict)
    df["production_countries"] = df["production_countries"].apply(extractDict)
    df["spoken_languages"] = df["spoken_languages"].apply(extractDict)
    return df
