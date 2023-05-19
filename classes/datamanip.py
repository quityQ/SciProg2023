import ast

#Some entries in the Dataset are given in JSON format
#Here we extract the relevant parts of those lists (whatever name is given)
#Following columns may have JSON Format: genres, production_companies, production_countries, spoken_languages
def extractDict(input):
    outputList = []
    tempList = []
    input = str(input).replace("[","")
    input = str(input).replace("]","")
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
