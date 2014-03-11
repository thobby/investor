drop table if exists tickers;
create table tickers (
  id integer,
  exchange text,
  symbol text,
  name text,
  market_cap real,
  tso text,
  country text,
  ipo_year integer,
  sector text,
  industry text,
  url text,
  primary key (id),
  unique (exchange, symbol));

drop table if exists ticker_info;
create table ticker_info (
  ticker integer,
  datum integer,
  avg_daily_volume integer,
  book_value real,
  dividend_per_share real,
  dividend_yield real,
  earnings_per_share real,
  ebitda text,
  fifty_day_moving_avg real,
  fifty_two_week_high real,
  fifty_two_week_low real,
  market_cap real,
  price_book_ratio real,
  price_earnings_growth_ratio real,
  price_earnings_ratio real,
  price_sales_ratio real,
  short_ratio real,
  two_hundred_day_moving_avg real,
  volume integer,
  unique (ticker, datum));

drop table if exists data;
create table data (ticker integer, datum integer, open real, close real, high real, low real, volume integer, adj_close real, unique (ticker, datum));

drop table if exists linear_regression;
create table linear_regression (ticker integer, gradient real, intercept real, r_value real, p_value real, std_err real, unique (ticker));

