import altair as alt
import datetime as dt
import logging
import matplotlib
import matplotlib.pyplot as plt
from matplotlib import style
import os
import pandas as pd
from pprint import pprint
import yfinance as yf

matplotlib.use('TkAgg')
style.use('ggplot')
logging.basicConfig(level=logging.INFO)


def get_data(symbol):

    fname = os.path.join('../data/', symbol + '.csv')
    if not os.path.exists(fname):
        return None

    df = pd.read_csv(fname, parse_dates=True, index_col=0)
    df['10ma'] = df['Close'].rolling(window=10, min_periods=0).mean()

    return df


def get_symbols(exchange):

    if exchange == 'NYSE':
        filename = '../exchanges/NYSE_symbols.csv'
    elif exchange == 'NASDAQ':
        filename = '../exchanges/NASDAQ_symbols.csv'
    else:
        raise Exception(f'Unknown exchange {exchange}')

    df = pd.read_csv(filename)

    return df


def analyze_symbol(df, symbol):

    def compute_delta(price, epoch):
        try:
            value = (price - df[df.index <= epoch]['10ma'][-1]) / price * 100.0
        except Exception as e:
            value = None

        return value

    if df is None:
        return None

    last_date = df.index[-1]
    last_price = df['10ma'][-1]
    ten_days_ago = last_date - dt.timedelta(days=10)
    twenty_days_ago = last_date - dt.timedelta(days=20)
    thirty_days_ago = last_date - dt.timedelta(days=30)
    sixty_days_ago = last_date - dt.timedelta(days=60)
    ninety_days_ago = last_date - dt.timedelta(days=90)
    oneeighty_days_ago = last_date - dt.timedelta(days=180)

    record = {
        'symbol': symbol,
        'latest_price': last_price,
        'latest_price_date': last_date,
        'min_price': df['10ma'].min(),
        'max_price': df['10ma'].max(),
        '10d_delta': compute_delta(last_price, ten_days_ago),
        '20d_delta': compute_delta(last_price, twenty_days_ago),
        '30d_delta': compute_delta(last_price, thirty_days_ago),
        '60d_delta': compute_delta(last_price, sixty_days_ago),
        '90d_delta': compute_delta(last_price, ninety_days_ago),
        '180d_delta': compute_delta(last_price, oneeighty_days_ago)
    }

    return record


def plot_symbol(df):

    ax1 = plt.subplot2grid((6, 1), (0, 0), rowspan=5, colspan=1)
    ax2 = plt.subplot2grid((6, 1), (5, 0), rowspan=1, colspan=1, sharex=ax1)

    ax1.plot(df.index, df['Close'])
    ax1.plot(df.index, df['10ma'])
    ax2.bar(df.index, df['Volume'])

    ax1.set_title(f'Symbol: {symbol}')
    ax1.set_ylabel('US Dollars')
    ax2.set_ylabel('Volume')
    plt.show()


def process_symbol(symbol, plot=False):
    df = get_data(symbol)
    record = analyze_symbol(df, symbol)
    logging.debug(pprint(record))
    if plot:
        plot_symbol(df)

    return record


def generate_pickle_file(exchange):

    symbols = list(get_symbols(exchange)['Symbol'])

    results = []
    for symbol in symbols:
        record = process_symbol(symbol)
        if record:
            results.append(record)

    results_df = pd.DataFrame(results)
    filename = os.path.join('../exchanges', exchange + '.pkl')
    results_df.to_pickle(filename)
    logging.info(
        f'Processed symbols from exchange {exchange}, wrote pickle file {filename}')


def process_exchange(exchange, sort_by='180d_delta', top_n=10):

    # Regenerate the pickle file to be safe.  We can address this in a different
    # way if needed later.
    filename = os.path.join('../exchanges', exchange + '.pkl')
    if os.path.exists(filename):
        os.remove(filename)

    generate_pickle_file(exchange)
    results_df = pd.read_pickle(filename)

    results_df.sort_values(by=sort_by, inplace=True, ascending=False)
    return results_df.iloc[0:top_n]


process_exchange('NYSE', sort_by='10d_delta', top_n=40)

df = get_data('DASH')
#chart = alt.Chart(df).mark_point().encode(x='Date:T', y='Close')
# chart.display()

data = pd.DataFrame({'col-1': list('CCCDDDEEE'),
                     'col-2': [2, 7, 4, 1, 2, 6, 8, 4, 7]})
chart = alt.Chart(data)
alt.Chart(data).mark_point().encode(
    x='col-1',
    y='col-2'
)
