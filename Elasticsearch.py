import string
import threading
import time
import csv
import nltk
import pandas as pd
from nltk import *
from elasticsearch.helpers import bulk
from nltk.corpus import stopwords
from rake_nltk import Rake
from elasticsearch import Elasticsearch

ogData = pd.read_csv('wiki_movie_plots_deduped.csv', nrows=1000, encoding='utf-8')
swords = stopwords.words('english')

def processedData():  # reduce the dataset to a 1000 lines as per the requirements
    ogData.to_csv('moviePlots.csv',
                        index=False,
                        encoding='utf-8')
    print('csv created and opened')
    return

def stemming(data):
    nltkPS = PorterStemmer()
    return [nltkPS.stem(root) for root in data]

#didnt really use this because i was confused about how to approach the task with ES vs with Python
def csvConvertTxt():   #convert the csv data into string within a text file so it's readable
    ogCSV = ('moviePlots.csv') #can convert back to csv if needed
    newTxt = ('csvToText.txt')
    with open(newTxt, "w") as csvToText: #writes to the text file
        with open(ogCSV, "r") as csvFile: #using the csv data
            [csvToText.write(" ".join(row)+'\n') for row in csv.reader(csvFile)] #iterate through the rows pass to txt file
        csvToText.close()

def processingAndLemitrisation():
        # the preprocessing stage
        lematrizer = WordNetLemmatizer()
        # case folding
        dataToLower = text.lower()  # sets the text content to lower once the file is read as string
        tokenWords = nltk.word_tokenize(dataToLower)  # tokenization of the lower case text ^
        tokenPunctRemover = [char for char in tokenWords if char.isalpha()]  # all non alphabet chars removed
        # Lemmatisation process
        lematrisedTokens = [lematrizer.lemmatize(char) for char in tokenPunctRemover]
        return lematrisedTokens

def rakingTool(x, r):
    r.extract_keywords_from_text(x)
    return
 #
def preProcessing():
    csvText['OriginalD'] = csvText['Plot']
    csvText['temPlot'] = csvText['Plot'].apply(lambda x: rakingTool(x, raking))
    return

def normalisation(): #normalisation, tokenisation 
    csvText['Plot'] = csvText['Plot'].str.lower()  # movie plot data text lowered
    csvText['Plot'] = csvText['Plot'].apply(word_tokenize)  # movie plot data tokenized using nltk.tokenize lib

    csvText['Genre'] = csvText['Genre'].str.lower()
    csvText['Genre'] = csvText['Genre'].apply(word_tokenize)

    csvText['Origin/Ethnicity'] = csvText['Origin/Ethnicity'].str.lower()
    csvText['Origin/Ethnicity'] = csvText['Origin/Ethnicity'].apply(word_tokenize)

    csvText['Title'] = csvText['Title'].apply(word_tokenize)  # Title data tokenized by nltk

    csvText['Cast'] = csvText['Cast'].astype(str)  # cast data set read as string using pandas library
    csvText['Cast'] = csvText['Cast'].apply(word_tokenize)  # then is tokenized

    csvText['Release Year'] = csvText['Release Year'].astype(str)  # same process as cast, year number to str
    csvText['Release Year'] = csvText['Release Year'].apply(word_tokenize)  # then is tokenized

    csvText['Director'] = csvText['Director'].apply(word_tokenize)
    return

def stemTool():
    csvText['Plot'] = csvText['Plot'].apply(stemming)
    return
def stop_words():
    csvText['Plot'] = csvText['Plot'].apply(lambda word: [sword for sword in word if sword not in swords])
    csvText['Plot'].apply(lambda i: ' '.join(i))
    return

if __name__ == "__main__":
    es_connection = Elasticsearch('http://localhost:9200')  # connect elasticsearch python api
    es = Elasticsearch()
    #Call all of the functions and load the data
    processedData()#call the preprocessing function
    csvText = pd.read_csv('moviePlots.csv')
    raking = Rake()#setup rake
    preProcessing()
    normalisation()
    stemTool()
    stop_words() 
    indexer = 'wikimovieplots'
    es.indices.delete(index=indexer)
    print('pre-existing indicies removed')

    with open('moviePlots.csv') as csvData:
        reader = csv.DictReader(csvData)#reads through data and assigns values
        bulk(es, reader, index=indexer)# uploads to elasticsearch 
        print('Elasticsearch connection established.\nCsv data has been uploaded.')
        threading.Event().wait(5)# gives the system time to find the number of rows

        es.cat.count()
        rowsTotal = es.count(index=indexer, body={"query":
                                                      {"match_all": {}
                                                       }
                                                  })
        print(rowsTotal['count']) # counting the number of rows in the doc, should be 1000 pulled into the ES API
        print("Number of rows found: 1000")
        while True:
            try:
                search = input("Enter your Elasticsearch query: ")#input query via elasticsearch
                #searchs the fields of all the data based on user input 
                output = es.search(index=indexer, body={"query": {"multi_match": #multimatch will match user input
                                                                      {"query": search}#with all of the data fields
                                                                  }
                                                        })
                print('type for response["hits"]:', type(output['hits']))
                if output.get('hits') is not None \ #
                        and output['hits'].get('hits') is not None:
                                print(output['hits']['hits'])#so if theres any data mathced in any particular 
                                                             #column or row then the entire containing field will
                elif output.get('hits') is None \            #be printed out
                        and output['hits'].get('hits') is None:
                            print('No data was found, try again.')
                else:
                    if input == "quit":
                        quit()
            except(ValueError):
                print("Error has occurred, terminating program. Please restart")
                exit()
