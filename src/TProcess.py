import sqlite3
import pandas as pd
from datetime import timedelta, datetime
from afinn import Afinn
import sys
import tjson


def sentiment_score(data):
    return afinn.score(tjson.tweet_text(data))

# Volume of tweets authored by who has more than 1000 and
# less than 5000 followers
def feat2(group):
    return group.json.apply(lambda json : tjson.n_flwrs(json) > 1000 and tjson.n_flwrs(json) < 5000).sum()
# Volume of tweets authored by who has more than 5000 followers
def feat3(group):
    return group.json.apply(lambda json : tjson.n_flwrs(json) > 5000).sum()
# Volume of retweets
def feat4(group):
    return group.json.apply(lambda json : tjson.is_retweet(json)).sum()
# Volume of retweets retweeted more than 5 times
def feat5(group):
    return group.json.apply(lambda json : tjson.n_retweets(json)>5).sum()
# Volume of retweets authored by who has more than 1000 followers
def feat6(group):
    return group.json.apply(lambda json : tjson.is_retweet(json) and tjson.n_flwrs(json) > 1000).sum()
# Volume of retweets authored by who has more than 1000 followers
# and less than 1000 followings
def feat7(group):
    return group.json.apply(lambda json : tjson.is_retweet(json) and tjson.n_flwrs(json) > 1000 and tjson.n_frnds(json) < 1000).sum()
# Volume of tweets containing links
def feat8(group):
    return group.json.apply(lambda json : tjson.has_link(json)).sum()
# Volume of positive tweets
def feat9(group):
    return group.json.apply(lambda json : sentiment_score(json)>0).sum()
# Volume of negative tweets
def feat10(group):
    return group.json.apply(lambda json : sentiment_score(json)<0).sum()
# Volume of neutral tweets
def feat11(group):
    return group.json.apply(lambda json : sentiment_score(json)==0).sum()
# Volume of positive tweet containing mentions (@)
def feat12(group):
    return group.json.apply(lambda json : sentiment_score(json)>0 and tjson.has_mntns(json)).sum()
# Volume of negative tweets containing mentions (@)
def feat13(group):
    return group.json.apply(lambda json : sentiment_score(json)==0 and tjson.has_mntns(json)).sum()

if __name__ == '__main__':

    # Get the time resolution
    time_resolution = sys.argv[1]

    td = timedelta(hours=1)

    # Fetch data from db
    db = sqlite3.connect('data_collected/tweets-001.db')
    c = db.cursor()

    c.execute("SELECT min(date), max(date) FROM tweets")

    rows = c.fetchall()
    start = datetime.strptime(rows[0][0], '%Y-%m-%d %H:%M:%S').replace(second=0,minute=0)+td
    end = datetime.strptime(rows[0][1], '%Y-%m-%d %H:%M:%S').replace(second=0,minute=0)

    for i in range(int((end-start)/td)):

        slot_start = datetime.now()
        print("Fetching data...")
        c.execute("SELECT * FROM tweets WHERE date > '{}' AND date < '{}'".format(start+td*i,start+td*(i+1)))

        rows = c.fetchall()

        tweets = pd.DataFrame(columns=['id', 'date', 'json', 'filter'], data=rows)
        tweets['date'] = pd.to_datetime(tweets['date'],format='%Y-%m-%d %H:%M:%S')

        print("fetching time: "+str(datetime.now() - slot_start) )
        print("Processing data...")

        # Remove tweets with multiple cashtags
        tweets = tweets[tweets.json.apply(lambda x: not tjson.is_multiple_cashtag(x))]

        # Load the afinn class for sentiment analysis
        afinn = Afinn()

        varT = {}
        # Computing varT
        varT['f1'] = tweets.shape[0]
        varT['f2'] = feat2(tweets)
        varT['f3'] = feat3(tweets)
        varT['f4'] = feat4(tweets)
        varT['f5'] = feat5(tweets)
        varT['f6'] = feat6(tweets)
        varT['f7'] = feat7(tweets)
        varT['f8'] = feat8(tweets)
        varT['f9'] = feat9(tweets)
        varT['f10'] = feat10(tweets)
        varT['f11'] = feat11(tweets)
        varT['f12'] = feat12(tweets)
        varT['f13'] = feat13(tweets)

        print("processing time: "+str(datetime.now() - slot_start) )

        varT = pd.DataFrame(varT,index=[str(start+td*i)])
        print(varT.tail())
        varT.index = pd.to_datetime(varT.index)
        varT.to_sql('vart_{}'.format(time_resolution.lower()),db,index=True,if_exists='append')

        print("slot time elapsed: "+str(datetime.now() - slot_start) )
