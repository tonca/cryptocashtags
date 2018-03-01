import json

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

def tweet_text(data):
    info = json.loads(data)
    if 'extended_tweet' in info:
        text = info['extended_tweet']['full_text']
    else:
        text = info['text']
    return text

def is_multiple_cashtag(tweet):
    info = json.loads(tweet)
    return len(info['entities']['symbols'])>1