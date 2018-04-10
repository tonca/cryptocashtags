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
    time_resolution = '1h'
    cashtags = ['btc','eth']

    td = timedelta(hours=1)

    # Fetch tweets from db
    db_tweets = sqlite3.connect('data_collected/tweets-001.db')
    c_tweets = db_tweets.cursor()

    db_vart = sqlite3.connect('vart.db')
    c_vart = db_vart.cursor()

    sql_create_vart = '''
        CREATE TABLE IF NOT EXISTS "vart_1h" (
        "tag" TEXT,
        "date" TIMESTAMP,
        "f1" INTEGER,
        "f10" INTEGER,
        "f11" INTEGER,
        "f12" INTEGER,
        "f13" INTEGER,
        "f2" INTEGER,
        "f3" INTEGER,
        "f4" INTEGER,
        "f5" INTEGER,
        "f6" INTEGER,
        "f7" INTEGER,
        "f8" INTEGER,
        "f9" INTEGER,
        PRIMARY KEY("date","tag"))
        '''

    c_vart.execute(sql_create_vart)

    # Subdivide time into slots
    c_tweets.execute("SELECT min(date), max(date) FROM tweets")
    rows = c_tweets.fetchall()
    start = datetime.strptime(rows[0][0], '%Y-%m-%d %H:%M:%S').replace(second=0,minute=0)+td
    end = datetime.strptime(rows[0][1], '%Y-%m-%d %H:%M:%S').replace(second=0,minute=0)

    # start from the last insertion
    c_vart.execute("SELECT max(date) FROM vart_{}".format(time_resolution))
    last_entry = c_vart.fetchall()[0][0]
   
    if not last_entry == None:
        last_entry_tm = datetime.strptime(last_entry, '%Y-%m-%d %H:%M:%S').replace(second=0,minute=0)+td

        if last_entry_tm > start and last_entry_tm < end:
            start = last_entry_tm


    for i in range(int((end-start)/td)):
        print('TIME SLOT: {} '.format(start+td*i))
        slot_start = datetime.now()

        varT_tags = []

        for tag in cashtags:

            print('TAG: {}'.format(tag))

            fetch_start = datetime.now()
            print("Fetching data...")
            c_tweets.execute("SELECT * FROM tweets WHERE date > '{}' AND date < '{}'".format(start+td*i,start+td*(i+1)))

            rows = c_tweets.fetchall()

            tweets = pd.DataFrame(columns=['id', 'date', 'json', 'filter'], data=rows)
            tweets['date'] = pd.to_datetime(tweets['date'],format='%Y-%m-%d %H:%M:%S')

            print("fetching time: "+str(datetime.now() - fetch_start) )

            print("Processing data...")

            proc_start = datetime.now()
            # Remove tweets with multiple cashtags
            tweets = tweets[tweets.json.apply(lambda x: not tjson.is_multiple_cashtag(x))]
            # Select a specific cashtag
            tweets = tweets[tweets.json.apply(lambda x: tjson.get_cashtag(x)) == tag]

            # Load the afinn class for sentiment analysis
            afinn = Afinn()

            varT_dict = {}
            varT_dict['tag'] = tag
            # Computing varT
            varT_dict['f1'] = tweets.shape[0]
            varT_dict['f2'] = feat2(tweets)
            varT_dict['f3'] = feat3(tweets)
            varT_dict['f4'] = feat4(tweets)
            varT_dict['f5'] = feat5(tweets)
            varT_dict['f6'] = feat6(tweets)
            varT_dict['f7'] = feat7(tweets)
            varT_dict['f8'] = feat8(tweets)
            # Sentiment analysis
            varT_dict['f9'] = feat9(tweets)
            varT_dict['f10'] = feat10(tweets)
            varT_dict['f11'] = feat11(tweets)
            varT_dict['f12'] = feat12(tweets)
            varT_dict['f13'] = feat13(tweets)

            varT_dict['date'] = pd.to_datetime(str(start+td*i))

            print("processing time: " + str(datetime.now() - proc_start) )

            varT_tags.append(varT_dict)
        

        varT = pd.DataFrame(varT_tags, index=range(len(cashtags)))
        print(varT.tail())
        varT.to_sql('vart_{}'.format(time_resolution.lower()),db_vart,index=False,if_exists='append')

        print("slot time elapsed: "+str(datetime.now() - slot_start) )


    db_tweets.close()
    db_vart.close()