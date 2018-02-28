from tweepy import OAuthHandler
from tweepy import API
import sqlite3
from DBListener import DBListener
import json
import sys
import pandas as pd

cred = json.load(open('authentication/credentials.json'))
consumer_key = cred['consumer_key']
consumer_secret = cred['consumer_secret']

access_token = cred['access_token']
access_token_secret = cred['access_token_secret']


if __name__ == '__main__':

    db = sqlite3.connect('tweets.db')
    c = db.cursor()
    c.execute("SELECT * FROM tweets")

    rows = c.fetchall()

    tweets = pd.DataFrame(columns=['id', 'date', 'json', 'filter'], data=rows)
    tweets['date'] = pd.to_datetime(tweets['date'],format='%Y-%m-%d %H:%M:%S')
    print(tweets.head())
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = API(auth)

    tweet = api.get_status(tweets.id[0])
    print(tweet._json)