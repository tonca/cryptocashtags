import json

# Number of followers
def n_flwrs(data):
    return json.loads(data)['user']['followers_count']

# Number of friends (following?)
def n_frnds(data):
    return json.loads(data)['user']['friends_count']

# Is the status a retweet
def is_retweet(data):
    return 'retweeted_status' in json.loads(data)

# Number of retweets of a retweeted status
def n_retweets(data):
    if is_retweet(data):
        return int(json.loads(data)['retweeted_status']['retweet_count'])
    else:
        return 0

# Does the status contain links?
def has_link(data):
    return len(json.loads(data)['entities']['urls'])>0

# Does the status contain links?
def has_mntns(data):
    return len(json.loads(data)['entities']['user_mentions'])>0

# The status text
def tweet_text(data):
    info = json.loads(data)
    if 'extended_tweet' in info:
        text = info['extended_tweet']['full_text']
    else:
        text = info['text']
    return text

# Are there more than one cashtag in the status
def is_multiple_cashtag(tweet):
    info = json.loads(tweet)
    return not len(info['entities']['symbols'])==1

def get_cashtag(tweet):
    info = json.loads(tweet)
    return info['entities']['symbols'][0]['text'].lower()

# The JSON string describing the symbols in a status text ()
def tweet_symbols(data):
    info = json.loads(data)
    return info['entities']['symbols']