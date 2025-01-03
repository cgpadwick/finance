#!/usr/bin/env python

import os
import json
import argparse
import pandas as pd
from glob import glob


def fill_missing_dates_and_add_ma(
    df, symbol, fill_method="ffill", ma_windows=[5, 10, 30, 90]
):
    """
    Fills missing business dates for a given stock's DataFrame and calculates moving averages.

    Parameters:
        df (pd.DataFrame): DataFrame containing columns ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', ...].
        symbol (str): The stock symbol to process.
        fill_method (str): Method to fill missing values (e.g., 'ffill', 'bfill'). Default: 'ffill'.
        ma_windows (list): List of integers specifying the moving average windows. Default: [5, 10, 30, 90].

    Returns:
        pd.DataFrame: The updated DataFrame with no missing business dates and added moving averages.
    """
    # Ensure 'Date' is in datetime format (forcing UTC for consistency)
    df["Date"] = pd.to_datetime(df["Date"], utc=True)

    # Sort by Date
    df = df.sort_values("Date").reset_index(drop=True)

    # Set Date as index for reindexing
    df.set_index("Date", inplace=True)

    # Generate all business dates from min to max date
    all_dates = pd.date_range(start=df.index.min(), end=df.index.max(), freq="B")

    # Reindex to fill missing business dates
    df = df.reindex(all_dates)
    df["Symbol"] = symbol  # Keep track of the symbol
    df.index.name = "Date"

    # Fill missing values using the specified method (default: ffill)
    if fill_method == "ffill":
        df.ffill(inplace=True)
    elif fill_method == "bfill":
        df.bfill(inplace=True)
    else:
        raise Exception(f"Unsupported fill method: {fill_method}")

    # Calculate moving averages for each specified window
    for window in ma_windows:
        col_name = f"MA_{window}"
        df[col_name] = df["Close"].rolling(window=window, min_periods=1).mean()

    # Reset index to restore 'Date' as a column
    df.reset_index(inplace=True)

    return df


def process_single_stock(json_file, fill_method="ffill", ma_windows=[5, 10, 30, 90]):
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

    # (Optional) check for required columns
    # required_cols = {'Date','Open','High','Low','Close','Volume'}
    # if not required_cols.issubset(df.columns):
    #     print(f"Skipping {symbol} due to missing columns.")
    #     return pd.DataFrame()

    processed_df = fill_missing_dates_and_add_ma(
        df, symbol, fill_method=fill_method, ma_windows=ma_windows
    )
    return processed_df


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

    for json_file in json_files:
        df_stock = process_single_stock(
            json_file, fill_method=fill_method, ma_windows=ma_windows
        )
        if not df_stock.empty:
            all_dfs.append(df_stock)

    # Combine all processed DataFrames
    if all_dfs:
        combined_df = pd.concat(all_dfs, ignore_index=True)

        # Sort combined by Symbol, then Date
        combined_df.sort_values(by=["Symbol", "Date"], inplace=True)
        combined_df.reset_index(drop=True, inplace=True)

        # Optionally drop specified columns
        for col in drop_cols:
            if col in combined_df.columns:
                combined_df.drop(columns=[col], inplace=True)

        return combined_df
    else:
        return pd.DataFrame()  # Return empty if no valid data


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
        "--fill_method",
        type=str,
        default="ffill",
        help="Method to fill missing values (e.g., 'ffill', 'bfill').",
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
        fill_method=args.fill_method,
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
