import time
import requests
import datetime
import sqlite3
import json
import telegram_send

cred = json.load(open('authentication/credentials.json'))
key = cred['alphavantage']

# there would be also tether (USDT) but it is not present in alphavantage, same for EOS, ADA, NEO
# ref https://coinmarketcap.com/currencies/volume/monthly/
symbols = [
    'BTC',
    'ETH',
    'XRP',
    'LTC',
    'BCH',
    'ETC'
]

# create db table
db = sqlite3.connect('tweets.db')
c = db.cursor()
sql_create_table = """ CREATE TABLE IF NOT EXISTS cryptos (
                        date DATETIME,
                        symbol VARCHAR(4),
                        price DECIMAL(16,8),
                        volume DECIMAL(16,8),
                        market_cap DECIMAL(19,8),
                        PRIMARY KEY(`date`, `symbol`)
                    ); """
c.execute(sql_create_table)

# ask for crypto values
while True:
    for s in symbols:
        print('{} - Requesting {} from alphavantage'.format(datetime.datetime.now(), s))
        try:
            req = requests.get('https://www.alphavantage.co/query?function=DIGITAL_CURRENCY_INTRADAY&symbol={}&market=USD&apikey={}'.format(s, key))
        except requests.exceptions.ConnectionError:
            print('## request ConnectionError at {}'.format(datetime.datetime.now()))
            telegram_send.send(['Error while requesting {}'.format(s)])
        except requests.exceptions.HTTPError:
            print('## request HttpError at {}'.format(datetime.datetime.now()))
            telegram_send.send(['Error while requesting {}'.format(s)])
        except requests.exceptions.Timeout:
            print('## request Timeout at {}'.format(datetime.datetime.now()))
            telegram_send.send(['Error while requesting {}'.format(s)])
        except:
            print('## request raised a generic error at {}'.format(datetime.datetime.now()))
            telegram_send.send(['Error while requesting {}'.format(s)])
        req = req.json()
        print('{} - Got {} from alphavantage'.format(datetime.datetime.now(), s))

        try:
            time_series = req['Time Series (Digital Currency Intraday)']

            for k,v in time_series.items():
                query = "INSERT OR IGNORE INTO cryptos VALUES ('{date}', '{symbol}', '{price}', '{volume}', '{market_cap}')".format(
                    date = k,
                    symbol = s,
                    price = v['1b. price (USD)'],
                    volume = v['2. volume'],
                    market_cap = v['3. market cap (USD)']
                )
                try:
                    c.execute(query)
                except sqlite3.Error as e:
                    print('## sql error occurred at {}: {}'.format(datetime.datetime.now(), e.args[0]))
                    telegram_send.send(['Sql error {}'.format(e.args[0])])
            print('{} - {} datapoints written to DB'.format(datetime.datetime.now(), s))
        except KeyError as e:
            print(e)
            print(json.dumps(req))
            telegram_send.send([s, json.dumps(req)])

        db.commit()
        time.sleep(10)
    time.sleep(60)