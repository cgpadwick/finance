#!/usr/bin/env python

import os
import json
import argparse
import pandas as pd
from glob import glob
from tqdm import tqdm


def add_ma(df, ma_windows=[5, 10, 30, 90]):
    """
    Calculates moving averages.
    """

    # Calculate moving averages for each specified window
    for window in ma_windows:
        col_name = f"MA_{window}"
        df[col_name] = df["Close"].rolling(window=window, min_periods=1).mean()

    return df


def load_single_stock(json_file):
    """
    Loads a single stock's JSON data, fills missing dates, and adds moving averages.

    Parameters:
        json_file (str): Path to the stock's JSON file.
        fill_method (str): Method to fill missing values. Default: 'ffill'.
        ma_windows (list): List of MA windows. Default: [5, 10, 30, 90].

    Returns:
        pd.DataFrame: Processed DataFrame for this stock.
    """
    symbol = os.path.splitext(os.path.basename(json_file))[0]  # e.g., 'AAPL'

    with open(json_file, "r") as f:
        data = json.load(f)

    df = pd.DataFrame(data)

    # Convert the timestamp to a date
    df["Date"] = pd.to_datetime(df["Date"], utc=True)
    df["Date"] = df["Date"].dt.date

    # Sort by Date
    df = df.sort_values("Date").reset_index(drop=True)
    df["Symbol"] = symbol  # Keep track of the symbol

    return df


def process_all_stocks(
    directory, fill_method="ffill", ma_windows=[5, 10, 30, 90], drop_cols=[]
):
    """
    Processes all stocks in a given directory:
      - Loads each JSON file
      - Fills missing dates
      - Computes moving averages
      - Concatenates everything into a single DataFrame
      - Drops unwanted columns if specified

    Parameters:
        directory (str): Path to the directory containing per-stock JSON files.
        fill_method (str): Method to fill missing data. Default: 'ffill'.
        ma_windows (list): List of MA windows. Default: [5, 10, 30, 90].
        drop_cols (list): List of columns to drop after merging. Default: [].

    Returns:
        pd.DataFrame: The combined DataFrame of all stocks.
    """

    all_dfs = []
    json_files = glob(os.path.join(directory, "*.json"))

    for json_file in tqdm(json_files):
        df_stock = load_single_stock(json_file)
        if not df_stock.empty:
            all_dfs.append(df_stock)

    # Some of the dataframes will be shorter because yfinance won't have all the data for them.
    # Filter by the start and end date.
    max_date = max([df["Date"].max() for df in all_dfs])
    min_date = min([df["Date"].min() for df in all_dfs])

    print(f"min date: {min_date}, max_date: {max_date}")

    filtered_dfs = []
    for df in all_dfs:
        keep = df["Date"].max() == max_date and df["Date"].min() == min_date
        if keep:
            filtered_dfs.append(df)

    print(
        f"After filtering json data by date, we keep {len(filtered_dfs)} datasets, or {int(len(filtered_dfs) / len(all_dfs) * 100)}% of the data"
    )

    assert len(filtered_dfs) > 0, "No data left after filtering"

    # Add moving averages
    for idx, df in enumerate(filtered_dfs):
        filtered_dfs[idx] = add_ma(df, ma_windows)

    # Combine all processed DataFrames
    combined_df = pd.concat(filtered_dfs, ignore_index=True)

    # Sort combined by Symbol, then Date
    combined_df.sort_values(by=["Symbol", "Date"], inplace=True)
    combined_df.reset_index(drop=True, inplace=True)

    # Optionally drop specified columns
    for col in drop_cols:
        if col in combined_df.columns:
            combined_df.drop(columns=[col], inplace=True)

    return combined_df


def main():
    parser = argparse.ArgumentParser(
        description="Process JSON stock data by filling missing dates and computing moving averages."
    )

    parser.add_argument(
        "--directory",
        type=str,
        default="../data/stock_data_json",
        help="Path to the directory containing JSON files.",
    )

    parser.add_argument(
        "--ma_windows",
        type=int,
        nargs="+",
        default=[5, 10, 30, 90],
        help="List of integers specifying moving average windows.",
    )

    parser.add_argument(
        "--drop_cols",
        nargs="*",
        default=["Dividends", "Stock Splits", "Capital Gains"],
        help="List of column names to drop from the final DataFrame.",
    )

    parser.add_argument(
        "--outputpkl",
        required=True,
        default=None,
        help="pathname to write out dataframe to (in pickle format)",
    )

    args = parser.parse_args()

    combined_df = process_all_stocks(
        directory=args.directory,
        ma_windows=args.ma_windows,
        drop_cols=args.drop_cols,
    )

    print(
        "Combined DataFrame with missing dates filled and specified moving averages calculated:"
    )
    print(combined_df.head(20))  # Show first 20 rows

    combined_df.to_pickle(args.outputpkl)


if __name__ == "__main__":
    main()
