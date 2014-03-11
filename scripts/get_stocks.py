#!/usr/bin/env python3.3

import csv
import logging
import re
import requests
import sqlite3
import sys
import ystockquote
import datetime
import concurrent.futures
from io import StringIO
from pprint import pprint

logging.basicConfig(level = logging.DEBUG)
logger = logging.getLogger("investor")

def parse_int (data):
  try:
    if (data == "N/A"):
      return 0
    if data.endswith("T"):
      return int(data[:-1]) * 1000000000000
    if data.endswith("B"):
      return int(data[:-1]) * 1000000000
    if data.endswith("M"):
      return int(data[:-1]) * 1000000

    return int(data)
  except:
    logger.warning("Failed parsing int from %s", data)

def parse_float (data):
  try:
    if (data == "N/A"):
      return 0.0
    if data.endswith("T"):
      return float(data[:-1]) * 1000000000000
    if data.endswith("B"):
      return float(data[:-1]) * 1000000000
    if data.endswith("M"):
      return float(data[:-1]) * 1000000

    return float(data)
  except:
    logger.warning("Failed parsing float from %s", data)

class ose:
  id = 1
  name = 'ose'
  root_url = 'http://www.oslobors.no'
  stocks_list_url = root_url + '/ob_eng/markedsaktivitet/stockList?newt__list=OB-OSE'
  ticker_url = root_url + '/ob_eng/markedsaktivitet/stockOverview?newt__ticker='
  daily_values = root_url + '/markedsaktivitet/servlets/newt/tradesExcel-stock?exch=ose&newt_page=%2Fno%2Fportal%2Fose%2FstockGraph'

  def get_tickers(self):
    r = requests.get(self.stocks_list_url)
    assert(r.status_code == 200)
    tickers = [
      m.group(1) + ".OL" for m in re.finditer(re.escape(self.ticker_url) + "(\w*)", r.text)]
    tickers = set(tickers)
    return tickers

class ticker:
  id = -1
  name = ''
  def __init__(self, id, name):
    self.id = id
    self.name = name

# tables:
#  exchanges: id, name
#  tickers: id, exchange, name
class db:
  file = 'stocks.db'

  def open_connection(self):
    logger.debug("Opening connection to DB %s", self.file)
    self.conn = sqlite3.connect(self.file)
    self.cur = self.conn.cursor()

  def close_connection(self):
    logger.debug("Closing connection to DB %s", self.file)
    self.conn.commit()
    self.conn.close()

  def update_tickers(self, exchange, tickers):
    for ticker in tickers:
      self.cur.execute('select t.id, t.name from tickers t, exchanges e where t.exchange = e.id and t.name = ? and e.name = ?',
                       (ticker, exchange.name,))
      row = self.cur.fetchone()
      if row is None:
        logger.info("New ticker on %s[%s]: %s", exchange.name, exchange.id, ticker)
        self.conn.execute('insert into tickers(id, exchange, name) values (null, ?, ?);', (exchange.id, ticker))
      else:
        logger.debug("Ticker %s is already in the db", ticker)


  def update_data(self, symbol, data):
    id = self.get_id_for_symbol(symbol)
    for d in data:
      self.cur.execute('insert into data('
                       'ticker,'
                       'datum,'
                       'open,'
                       'close,'
                       'high,'
                       'low,'
                       'volume,'
                       'adj_close'
                       ') values (?, date(?), ?, ?, ?, ?, ?, ?)',
                       (
                         id,
                         d,
                         parse_float(data[d]['Open']),
                         parse_float(data[d]['Close']),
                         parse_float(data[d]['High']),
                         parse_float(data[d]['Low']),
                         parse_int(data[d]['Volume']),
                         parse_float(data[d]['Adj Close'])
                       ))

  def create_ticker(self, exchange, symbol, name, market_cap, tso, country, ipo_year, sector, industry, url):
    logger.info("Inserting ticker " + symbol)
    self.cur.execute('insert into tickers ('
                     'exchange,'
                     'symbol,'
                     'name,'
                     'market_cap,'
                     'tso,'
                     'country,'
                     'ipo_year,'
                     'sector,'
                     'industry,'
                     'url'
                     ') values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                     (
                       exchange,
                       symbol,
                       name,
                       market_cap,
                       tso,
                       country,
                       ipo_year,
                       sector,
                       industry,
                       url
                     ))
    return self.get_id_for_symbol(symbol)

  def create_ticker_info(self, id, datum, ticker_info):
    logger.info('Inserting ticker ' + str(id) + ' info')
    self.cur.execute('insert into ticker_info ('
                     'ticker,'
                     'datum,'
                     'avg_daily_volume,'
                     'book_value,'
                     'dividend_per_share,'
                     'dividend_yield,'
                     'earnings_per_share,'
                     'ebitda,'
                     'fifty_day_moving_avg,'
                     'fifty_two_week_high,'
                     'fifty_two_week_low,'
                     'market_cap,'
                     'price_book_ratio,'
                     'price_earnings_growth_ratio,'
                     'price_earnings_ratio,'
                     'price_sales_ratio,'
                     'short_ratio,'
                     'two_hundred_day_moving_avg,'
                     'volume'
                     ') values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                     (
                       id,
                       datum,
                       parse_int(ticker_info['avg_daily_volume']),
                       parse_float(ticker_info['book_value']),
                       parse_float(ticker_info['dividend_per_share']),
                       parse_float(ticker_info['dividend_yield']),
                       parse_float(ticker_info['earnings_per_share']),
                       ticker_info['ebitda'],
                       parse_float(ticker_info['fifty_day_moving_avg']),
                       parse_float(ticker_info['fifty_two_week_high']),
                       parse_float(ticker_info['fifty_two_week_low']),
                       parse_float(ticker_info['market_cap']),
                       parse_float(ticker_info['price_book_ratio']),
                       parse_float(ticker_info['price_earnings_growth_ratio']),
                       parse_float(ticker_info['price_earnings_ratio']),
                       parse_float(ticker_info['price_sales_ratio']),
                       parse_float(ticker_info['short_ratio']),
                       parse_float(ticker_info['two_hundred_day_moving_avg']),
                       parse_int(ticker_info['volume'])
                     ))

  def get_id_for_symbol(self, symbol):
    self.cur.execute('select id from tickers where symbol = ?', (symbol,))
    id = self.cur.fetchone()
    if id is not None:
      return id[0]
    return id

  def get_symbols(self):
    self.cur.execute('select symbol from tickers')
    symbols = []
    for s in self.cur.fetchall():
      symbols.append(s[0])
    return symbols

db = db()
db.open_connection()

nyse_csv = requests.get('http://www.nyse.com/indexes/nyaindex.csv')
if nyse_csv.status_code != 200:
  logger.error('Can not download symbols from NYSE %s', nyse_csv.status_code)
  exit()

nyse = StringIO(nyse_csv.text)
reader = csv.reader(nyse, delimiter=',')
i = 0
for row in reader:
  if i < 2:
    # Skip first two rows they are just headers
    i += 1
    continue
  # Row format: "Name", "Symbol", "Country", "ICB", "INDUS", "SUP SEC", "SEC", "SUB SEC"
  #               0        1          2        3       4         5        6        7
  # Note: Sector on NYSE and on NASDAQ have a bit different meaning
  if db.get_id_for_symbol(row[1]) is not None:
      # We already have this ticker in the DB
      logger.debug('Ticker %s already in DB', row[0])
      continue

  # Create ticker in the database
  id = db.create_ticker('nyse', row[1], row[0], 'N/A', 'N/A', row[2], 'N/A', row[4], row[6], 'N/A')

  logger.info('Created ticker id %d', id)

  # Get ticker detail information
  ticker_info = ystockquote.get_all(row[1])

  # Create ticker detail information
  db.create_ticker_info(id, datetime.datetime.now(), ticker_info)
  db.conn.commit()

tickers = []
with open('nasdaq.csv', 'r') as csvfile:
  reader = csv.reader(csvfile, delimiter=',')
  for row in reader:
    # Row format: "Symbol","Name","LastSale","MarketCap","ADR TSO","Country", "IPOyear","Sector","industry","Summary Quote",
    #                0        1       2           3          4         5        6         7             8      9
    if db.get_id_for_symbol(row[0]) is not None:
      # We already have this ticker in the DB
      logger.debug('Ticker %s already in DB', row[0])
      continue

    # Create ticker in the database
    id = db.create_ticker('nasdaq', row[0], row[1], row[3], row[4], row[5], row[6], row[7], row[8], row[9])

    logger.info('Created ticker id %d', id)

    # Get ticker detail information
    ticker_info = ystockquote.get_all(row[0])

    # Create ticker detail information
    db.create_ticker_info(id, datetime.datetime.now(), ticker_info)
    db.conn.commit()

def update_symbol(symbol):
  logger.info("Updating %s", symbol)
  try:
    return ystockquote.get_historical_prices(symbol, "2007-01-01", "2015-01-01")
  except:
    type, value, traceback = sys.exc_info()
    logger.warning("Getting %s from Yahoo failed %s %s %s", symbol, value, type, traceback)
    raise

with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
  # Update all tickers with the newest stock prices
  future_update = {executor.submit(update_symbol, symbol):
                    symbol for symbol in db.get_symbols()}
  for future in concurrent.futures.as_completed(future_update):
    symbol = future_update[future]
    try:
      data = future.result()
    except Exception as exc:
      logger.error('%s update generated an exception: %s' % (symbol, exc))
    else:
      logger.debug('Saving symbol %s update in DB', symbol)
      db.update_data(symbol, data)
      db.conn.commit()

db.close_connection()
