import json
import sqlite3
import pandas as pd
import datetime as dt
from afinn import Afinn
import sys


def n_flwrs(data):
    return json.loads(data)['user']['followers_count']

def n_frnds(data):
    return json.loads(data)['user']['friends_count']

# Return the number of retweets of a retweet
def n_retweets(data):
    if is_retweeted(data):
        return int(json.loads(data)['retweeted_status']['retweet_count'])
    else:
        return 0

def is_retweeted(data):
    return 'retweeted_status' in json.loads(data)

def has_link(data):
    return len(json.loads(data)['entities']['urls'])>0

def has_mntns(data):
    return len(json.loads(data)['entities']['user_mentions'])>0

def sent_score(data):
    return afinn.score(json.loads(data)['text'])
# Volume of tweets authored by who has more than 1000 and
# less than 5000 followers
def feat2(group):
    return group.json.apply(lambda json : n_flwrs(json) > 1000 and n_flwrs(json) < 5000).sum()
# Volume of tweets authored by who has more than 5000 followers
def feat3(group):
    return group.json.apply(lambda json : n_flwrs(json) > 5000).sum()
# Volume of retweets
def feat4(group):
    return group.json.apply(lambda json : is_retweeted(json)).sum()
# Volume of retweets retweeted more than 5 times
def feat5(group):
    return group.json.apply(lambda json : n_retweets(json)>5).sum()
# Volume of retweets authored by who has more than 1000 followers
def feat6(group):
    return group.json.apply(lambda json : is_retweeted(json) and n_flwrs(json) > 1000).sum()
# Volume of retweets authored by who has more than 1000 followers
# and less than 1000 followings
def feat7(group):
    return group.json.apply(lambda json : is_retweeted(json) and n_flwrs(json) > 1000 and n_frnds(json) < 1000).sum()
# Volume of tweets containing links
def feat8(group):
    return group.json.apply(lambda json : has_link(json)).sum()
# Volume of positive tweets
def feat9(group):
    return group.json.apply(lambda json : sent_score(json)>0).sum()
# Volume of negative tweets
def feat10(group):
    return group.json.apply(lambda json : sent_score(json)<0).sum()
# Volume of neutral tweets
def feat11(group):
    return group.json.apply(lambda json : sent_score(json)==0).sum()
# Volume of positive tweet containing mentions (@)
def feat12(group):
    return group.json.apply(lambda json : sent_score(json)>0 and has_mntns(json)).sum()
# Volume of negative tweets containing mentions (@)
def feat13(group):
    return group.json.apply(lambda json : sent_score(json)==0 and has_mntns(json)).sum()

if __name__ == '__main__':

    time_resolution = sys.argv[1]

    db = sqlite3.connect('tweets.db')
    c = db.cursor()
    c.execute("SELECT * FROM tweets")

    rows = c.fetchall()

    tweets = pd.DataFrame(columns=['id', 'date', 'json', 'filter'], data=rows)
    tweets['date'] = pd.to_datetime(tweets['date'],format='%Y-%m-%d %H:%M:%S')

    afinn = Afinn()

    # This lets us process tweet grouped in time slots
    t_slots = tweets.groupby(pd.Grouper(freq=time_resolution, key='date'))
    varT = pd.DataFrame()

    # Computing varT
    varT['f1'] = t_slots.size()
    varT['f2'] = t_slots.apply(lambda x : feat2(x))
    varT['f3'] = t_slots.apply(lambda x : feat3(x))
    varT['f4'] = t_slots.apply(lambda x : feat4(x)) # needs to be verified
    varT['f5'] = t_slots.apply(lambda x : feat5(x))
    varT['f6'] = t_slots.apply(lambda x : feat6(x))
    varT['f7'] = t_slots.apply(lambda x : feat7(x))
    varT['f8'] = t_slots.apply(lambda x : feat8(x))
    varT['f9'] = t_slots.apply(lambda x : feat9(x))
    varT['f10'] = t_slots.apply(lambda x : feat10(x))
    varT['f11'] = t_slots.apply(lambda x : feat11(x))
    varT['f12'] = t_slots.apply(lambda x : feat12(x))
    varT['f13'] = t_slots.apply(lambda x : feat13(x))

    print(varT.tail())