# Import Dependencies
import pandas as pd
import requests
import json
from pprint import pprint
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import datetime
import tweepy
from twitterapi import consumer_key, consumer_secret, access_token, access_token_secret
import os.path
from pymongo import MongoClient
import pymongo
import sys

def grabber():
    # Import API key
    from newsapi import api_key

    #Establishing Search Terms
    target_twitter = '@katyperry'
    dt = datetime.datetime.now()
    #setting up empty dataframe for the tweets
    tweet_df = pd.DataFrame(columns=['Handle','Datetime', 'Text', 'Image','Name', 'Compound', 'Pos', 'Neg', 'Neu'])

    # Setup Tweepy API Authentication
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth, parser=tweepy.parsers.JSONParser())

    # Get target name and image info
    target_info = []

    # Get target name and image info
    tweets = api.user_timeline(target_twitter)
        
        # Loop through tweets and append to twitter_data dictionary
    target_name = tweets[0]['user']['name']
    target_image = tweets[0]['user']['profile_image_url_https']
    search_tweets = api.search_users(target_name)
    # List to hold twitter tweets
    twitter_info = []
    
    # Loop through tweets and append to twitter_data dictionary
    for tweet in search_tweets:
        try:
            twitter_dict = {}
            twitter_dict['text'] = tweet['status']['text']
            twitter_info.append(twitter_dict)
        except KeyError:
            twitter_dict['text'] = ''
            twitter_info.append(twitter_dict)

    analyzer = SentimentIntensityAnalyzer()

    for desc in twitter_info:
        senti = analyzer.polarity_scores(desc["text"])
        compound = senti["compound"]
        pos = senti["pos"]
        neu = senti["neu"]
        neg = senti["neg"]
        tweet_df = tweet_df.append({'Handle' : target_twitter,'Datetime' : dt,'Text': desc['text'], 'Image': target_image, 'Name': target_name,'Compound': compound, 'Pos': pos, 'Neg': neg, 'Neu': neu}, ignore_index=True)
        #Getting NewsAPI Articles
        url = "https://newsapi.org/v2/everything?q="
        query_url = url + target_name + '&language=en' + "&apiKey=" + api_key
        result = requests.get(query_url).json()
        news_df = pd.DataFrame(columns=['Handle','Datetime', 'Content', 'Title', 'Compound', 'Pos', 'Neg', 'Neu', 'URL'])

# Variables for holding sentiments

    for desc in result["articles"]:
        if desc["content"]:
            senti = analyzer.polarity_scores(desc["content"])
            compound = senti["compound"]
            pos = senti["pos"]
            neu = senti["neu"]
            neg = senti["neg"]
            news_df = news_df.append({'Handle' : target_twitter,'Datetime' : dt, 'Content': desc['content'], 'Title': desc['title'], 'Compound': compound, 'Pos': pos, 'Neg': neg, 'Neu': neu, 'URL': desc['url']}, ignore_index=True)

    news_df.to_csv('./data/news.csv')
    tweet_df.to_csv('./data/tweets.csv')


    
grabber()

myclient = pymongo.MongoClient("mongodb://localhost:27017/")

dblist = myclient.list_database_names()
if "popular" in dblist:
  client = MongoClient('localhost', 27017)
  client.drop_database('popular')
  mydb = myclient['popular']
  mycol = mydb['tweets']
  mycol2 = mydb['news']
  mycol3 = mydb['scores']
else:
    mydb = myclient['popular']
    mycol = mydb['tweets']
    mycol2 = mydb['news']
    mycol3 = mydb['scores']

def import_content(filepath):
    mng_client = pymongo.MongoClient('localhost', 27017)
    mng_db = mng_client['popular']
    collection_name = 'tweets'
    db_cm = mng_db[collection_name]
    cdir = os.path.dirname(__file__)
    file_res = os.path.join(cdir, filepath)
    data = pd.read_csv(file_res)
    data = data.apply(pd.to_numeric, errors='ignore')
    data_json = json.loads(data.to_json(orient='records'))
    db_cm.remove()
    db_cm.insert(data_json)
    
if __name__ == "__main__":
  filepath = './data/tweets.csv'
  import_content(filepath)

def second_import_content(filepath):
    mng_client = pymongo.MongoClient('localhost', 27017)
    mng_db = mng_client['popular']
    collection_name = 'news'
    db_cm = mng_db[collection_name]
    cdir = os.path.dirname(__file__)
    file_res = os.path.join(cdir, filepath)
    data = pd.read_csv(file_res)
    data = data.apply(pd.to_numeric, errors='ignore')
    data_json = json.loads(data.to_json(orient='records'))
    db_cm.remove()
    db_cm.insert(data_json)
    
if __name__ == "__main__":
  filepath = './data/news.csv'
  second_import_content(filepath)
