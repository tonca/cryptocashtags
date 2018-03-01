from tweepy import OAuthHandler
from tweepy import Stream
from DBListener import DBListener
import json
import sys

cred = json.load(open('authentication/credentials.json'))
consumer_key = cred['twitter']['consumer_key']
consumer_secret = cred['twitter']['consumer_secret']

access_token = cred['twitter']['access_token']
access_token_secret = cred['twitter']['access_token_secret']


if __name__ == '__main__':

    filter = sys.argv[1]

    l = DBListener(filter=filter)
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, l)
    
    stream.filter(track=[filter])
