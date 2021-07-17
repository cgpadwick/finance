import altair as alt
import argparse
from analyze import *
import datapane as dp
import logging
import time
import yfinance as yf


def build_text(symbol):

    report_str = '***\n'
    report_str += f'## [{symbol}](https://finance.yahoo.com/quote/{symbol})\n'
    df = None
    # try:
    #     import pdb
    #     pdb.set_trace()
    #     obj = yf.Ticker(symbol)
    #     info = obj.info

    #     report_str += f"\n{info['shortName']}\n"
    #     df = pd.DataFrame.from_dict(info, orient='index', columns=['info'])

    # except Exception as e:
    #     logging.warn(f'Error getting data for symbol {symbol}')
    #     pass

    return report_str, df


def build_chart(df):

    alt.data_transformers.disable_max_rows()

    melt_df = df[['Date', 'Close', '10ma']].melt('Date')
    top = alt.Chart(melt_df).mark_line(point=True).encode(
        x='Date:T',
        y=alt.Y('value', axis=alt.Axis(title='US Dollars')),
        color='variable',
        tooltip=['value']
    ).interactive().properties(width=600, height=200)

    brush = alt.selection(type='interval', encodings=['x'])
    base = alt.Chart(df).mark_area().encode(
        x='Date:T',
        y='Close:Q'
    ).properties(
        width=600,
        height=200
    )

    upper = base.encode(
        alt.X('Date:T', scale=alt.Scale(domain=brush)),
        alt.Y('Close:Q'),
        tooltip=['Close']
    )

    lower = base.properties(
        height=60
    ).mark_area().encode(x='Date:T', y='Volume:Q', tooltip=['Volume']).add_selection(brush)

    chart = top & upper & lower
    return chart


def main(exchange, top_n, sort_by):

    results_df = process_exchange(exchange, sort_by=sort_by, top_n=top_n)
    header = f'# Momentum Report For {exchange}\n'
    header += f'## Top {top_n} Stonks sorted by {sort_by}\n'
    block_list = [dp.Text(header), dp.Table(results_df.set_index('symbol'))]
    for idx, row in results_df.iterrows():
        symbol = row['symbol']
        df = get_data(symbol)
        df.reset_index(inplace=True)
        report_str, data = build_text(symbol)
        block_list.append(dp.Text(report_str))
        block_list.append(dp.Plot(build_chart(df)))
        if data is not None:
            block_list.append(dp.Table(data))

    r = dp.Report(blocks=block_list)
    r.save(path=f'{exchange}.html')


parser = argparse.ArgumentParser(description='Build stock data report')
parser.add_argument('--exchange', type=str, choices=('NYSE', 'NASDAQ'),
                    default=None, help='exchange to build a report for')
parser.add_argument('--topn', type=int, default=20,
                    help='report on the top n stocks')
parser.add_argument('--sort_by', type=str, choices=('10d_delta', '20d_delta', '30d_delta',
                                                    '60d_delta', '90d_delta', '180d_delta'),
                    default='30d_delta', help='column to sort results by')
args = parser.parse_args()

if args.exchange is not None:
    main(args.exchange, args.topn, args.sort_by)
else:
    for exchange in ['NYSE', 'NASDAQ']:
        main(exchange, args.topn, args.sort_by)
