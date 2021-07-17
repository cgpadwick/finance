import argparse
from datetime import date
import functools
import logging
from multiprocessing import Pool, cpu_count
import os
import pandas as pd
import yfinance as yf

logging.basicConfig(level=logging.INFO)

NUM_CPUS = cpu_count()


def download_symbol(symbol, startdate):

    obj = yf.Ticker(symbol)
    hist = obj.history(start=startdate, end=date.today(), debug=False)
    if hist.shape[0] > 0:
        fname = os.path.join('../data', symbol + '.csv')
        hist.to_csv(fname)
    return


def get_exchange_symbols(exchange):

    if exchange == 'NYSE':
        filename = '../exchanges/NYSE_symbols.csv'
    elif exchange == 'NASDAQ':
        filename = '../exchanges/NASDAQ_symbols.csv'
    else:
        raise Exception(f'Unknown exchange {exchange}')

    df = pd.read_csv(filename)
    return(list(df['Symbol']))


def main(exchange, startdate, nthreads):

    symbols = get_exchange_symbols(exchange)
    num_threads = NUM_CPUS
    if nthreads != -1:
        if nthreads > 0 and nthreads < NUM_CPUS:
            num_threads = nthreads
    pool = Pool(num_threads)

    logging.info(
        f'Downloading symbols from {exchange} with {num_threads} threads...')
    pool.map(functools.partial(download_symbol, startdate=startdate), symbols)
    logging.info('Done')


parser = argparse.ArgumentParser(description='Download stock data')
parser.add_argument('--exchange', type=str, choices=('NYSE', 'NASDAQ'),
                    default=None, help='exchange to download stock symbols for')
parser.add_argument('--startdate', default='2018-01-01',
                    help='starting date to download stock data for')
parser.add_argument('--threads', type=int, default=-1,
                    help='number of threads to use to download the data')

args = parser.parse_args()

if args.exchange is not None:
    main(args.exchange, args.startdate, args.threads)
else:
    for exchange in ['NYSE', 'NASDAQ']:
        main(exchange, args.startdate, args.threads)
