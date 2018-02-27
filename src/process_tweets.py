import json
import sqlite3
import pandas as pd

db = sqlite3.connect('tweets.db')
c = db.cursor()
c.execute("SELECT * FROM tweets")
 
rows = c.fetchall()

df = pd.DataFrame(columns=['date', 'symbol', 'price', 'volume', 'market_cap'], data=rows)

print(df.head())