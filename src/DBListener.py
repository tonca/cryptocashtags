import json
import sqlite3
import string
from datetime import datetime
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

class DBListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """
    def __init__(self, filter='$test'):
        self.db = sqlite3.connect('tweets.db')
        self.c = self.db.cursor()
        self.printable = list(string.printable)
        self.printable.remove("'")
        self.printable.remove('`')
        self.max_len = 0
        self.filter = filter


    def on_data(self, data):
        # Save data to the DB
        info = json.loads(data)
        json_str = json.dumps(info, separators=(',',':'))
        json_clean = ''.join(filter(lambda x: x in self.printable, json_str))

        id = info['id']
        date = datetime.strptime(info['created_at'],'%a %b %d %H:%M:%S %z %Y')
        print("Fetching tweet {}".format(id))
        print('Created at {}'.format(date))
        print('JSON length: {}'.format(len(json_clean)))
        self.max_len = max(self.max_len, len(json_clean))
        print('JSON maximum length: {}'.format(self.max_len))

        # SQL QUERY
        query = "INSERT OR IGNORE INTO tweets VALUES ('{id}', '{date}', '{json}', '{filter}')".format(
            id = id,
            date = date.strftime('%Y-%m-%d %H:%M:%S'),
            json = json_clean,
            filter = self.filter
        )
        # mette in coda la query
        self.c.execute(query)
        self.db.commit()

        return True

    def on_error(self, status):
        print("ERROR CODE: {}".format(status))
        if status == 420:
            #returning False in on_data disconnects the stream
            return False