import json
import sqlite3
import pandas as pd

db = sqlite3.connect('tweets.db')
c = db.cursor()
c.execute("SELECT * FROM tweets")
 
rows = c.fetchall()

tweets = pd.DataFrame(columns=['id', 'date', 'json', 'filter'], data=rows)

tweets['date'] = pd.DatetimeIndex(tweets['date'])

print(tweets.date.truncate(before='2018-02-26 15:05'))
print(tweets.date.truncate(after='2018-02-26 15:05'))
