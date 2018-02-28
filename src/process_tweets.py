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

def is_retweeted(data):
    return json.loads(data)['retweeted']

def has_link(data):
    return len(json.loads(data)['entities']['urls'])>0

def has_mntns(data):
    return len(json.loads(data)['entities']['user_mentions'])>0

def sent_score(data):
    return afinn.score(json.loads(data)['text'])

def feat2(group):
    return group.json.apply(lambda json : n_flwrs(json) > 1000 and n_flwrs(json) < 5000).sum()

def feat3(group):
    return group.json.apply(lambda json : n_flwrs(json) > 5000).sum()

def feat4(group):
    return group.json.apply(lambda json : is_retweeted(json)).sum()

def feat6(group):
    return group.json.apply(lambda json : is_retweeted(json) and n_flwrs(json) > 1000).sum()

def feat7(group):
    return group.json.apply(lambda json : is_retweeted(json) and n_flwrs(json) > 1000 and n_frnds(json) < 1000).sum()

def feat8(group):
    return group.json.apply(lambda json : has_link(json)).sum()

def feat9(group):
    return group.json.apply(lambda json : sent_score(json)>0).sum()

def feat10(group):
    return group.json.apply(lambda json : sent_score(json)<0).sum()

def feat11(group):
    return group.json.apply(lambda json : sent_score(json)==0).sum()

def feat12(group):
    return group.json.apply(lambda json : sent_score(json)>0 and has_mntns(json)).sum()

def feat13(group):
    return group.json.apply(lambda json : sent_score(json)==0 and has_mntns(json)).sum()

if __name__ == '__main__':

    time_resolution = sys.argv[1]

    db = sqlite3.connect('tweets.db')
    c = db.cursor()
    c.execute("SELECT * FROM tweets")

    rows = c.fetchall()

    afinn = Afinn()

    tweets = pd.DataFrame(columns=['id', 'date', 'json', 'filter'], data=rows)
    tweets['date'] = pd.to_datetime(tweets['date'],format='%Y-%m-%d %H:%M:%S')

    # This lets us process tweet grouped in time slots
    t_slots = tweets.groupby(pd.Grouper(freq=time_resolution, key='date'))
    features = pd.DataFrame()

    # Computing features
    features['f1'] = t_slots.size()
    features['f2'] = t_slots.apply(lambda x : feat2(x))
    features['f3'] = t_slots.apply(lambda x : feat3(x))
    features['f4'] = t_slots.apply(lambda x : feat4(x)) # needs to be verified
    # I still need to understand f5
    features['f6'] = t_slots.apply(lambda x : feat6(x))
    features['f7'] = t_slots.apply(lambda x : feat7(x))
    features['f8'] = t_slots.apply(lambda x : feat8(x))
    features['f9'] = t_slots.apply(lambda x : feat9(x))
    features['f10'] = t_slots.apply(lambda x : feat10(x))
    features['f11'] = t_slots.apply(lambda x : feat11(x))
    features['f12'] = t_slots.apply(lambda x : feat12(x))
    features['f13'] = t_slots.apply(lambda x : feat13(x))

    print(features.tail())