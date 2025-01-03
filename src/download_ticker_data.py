import os
import pandas as pd
import yfinance as yf
from tqdm import tqdm
import json
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import argparse
import random

# -------------------- Configuration -------------------- #


def setup_logging(log_file):
    """
    Set up logging configuration.
    Logs will be written to both the specified log file and the console.
    """
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s %(levelname)s:%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Configure the root logger to also output to console without disrupting tqdm
    console = logging.StreamHandler()
    console.setLevel(logging.CRITICAL)
    formatter = logging.Formatter("%(asctime)s %(levelname)s:%(message)s")
    console.setFormatter(formatter)
    logging.getLogger("").addHandler(console)


def load_symbols(csv_path, subsample=None, seed=None):
    """
    Load stock symbols from a CSV file.

    Parameters:
        csv_path (str): Path to the CSV file.

    Returns:
        list: List of stock symbols.
    """
    try:
        df = pd.read_csv(csv_path)
        if "Symbol" not in df.columns:
            logging.error(f"'Symbol' column not found in {csv_path}.")
            return []
        symbols = df["Symbol"].dropna().unique().tolist()
        logging.info(f"Loaded {len(symbols)} symbols from {csv_path}.")
        if subsample is not None:
            random.seed(seed)
            nsymbols = int(len(symbols) * subsample)
            if nsymbols == 0:
                raise Exception(
                    f"No symbols after subsample fraction {subsample} - pick a larger fraction"
                )
            logging.info(
                f"Subsampling symbols to fraction {subsample} of the symbols - {nsymbols} after subsample"
            )
            symbols = random.choices(symbols, k=nsymbols)
        return symbols
    except FileNotFoundError:
        logging.error(f"File {csv_path} not found.")
        return []
    except Exception as e:
        logging.error(f"Error loading symbols: {e}")
        return []


def create_output_dir(directory):
    """
    Create the output directory if it doesn't exist.

    Parameters:
        directory (str): Path to the directory.
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
        logging.info(f"Created directory: {directory}")
    else:
        logging.info(f"Output directory already exists: {directory}")


def download_stock_data(symbol, start, end, max_retries, retry_delay):
    """
    Download historical stock data for a given symbol.

    Parameters:
        symbol (str): Stock ticker symbol.
        start (str): Start date (YYYY-MM-DD).
        end (str): End date (YYYY-MM-DD).
        max_retries (int): Maximum number of retries for failed downloads.
        retry_delay (int): Delay between retries in seconds.

    Returns:
        pd.DataFrame or None: Historical stock data or None if failed.
    """
    for attempt in range(1, max_retries + 1):
        try:
            stock = yf.Ticker(symbol)
            df = stock.history(start=start, end=end)
            if df.empty:
                logging.warning(
                    f"No data found for {symbol} between {start} and {end}."
                )
                return None
            return df
        except Exception as e:
            logging.error(
                f"Error downloading {symbol} (Attempt {attempt}/{max_retries}): {e}"
            )
            if attempt < max_retries:
                logging.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logging.error(
                    f"Failed to download data for {symbol} after {max_retries} attempts."
                )
                return None


def save_to_json(symbol, df, directory):
    """
    Save the stock data DataFrame to a JSON file.

    Parameters:
        symbol (str): Stock ticker symbol.
        df (pd.DataFrame): Historical stock data.
        directory (str): Directory to save the JSON file.
    """
    try:
        # Reset index to include date in JSON
        df_reset = df.reset_index()
        # Convert to dictionary
        data_dict = df_reset.to_dict(orient="records")
        # Define file path
        file_path = os.path.join(directory, f"{symbol}.json")
        # Write JSON file
        with open(file_path, "w") as json_file:
            json.dump(data_dict, json_file, indent=4, default=str)
        logging.info(f"Saved data for {symbol} to {file_path}.")
    except Exception as e:
        # Use tqdm.write to avoid interfering with the progress bar
        # tqdm.write(f"Error saving JSON for {symbol}: {e}")
        logging.error(f"Error saving JSON for {symbol}: {e}")


def validate_data(df, symbol):
    """
    Validate the downloaded data.

    Parameters:
        df (pd.DataFrame): DataFrame containing stock data.
        symbol (str): Stock ticker symbol.

    Returns:
        bool: True if data is valid, False otherwise.
    """
    required_columns = {"Open", "High", "Low", "Close", "Volume"}
    if not required_columns.issubset(df.columns):
        # tqdm.write(f"Data for {symbol} is missing required columns.")
        logging.error(f"Data for {symbol} is missing required columns.")
        return False
    if df.empty:
        # tqdm.write(f"Data for {symbol} is empty.")
        logging.error(f"Data for {symbol} is empty.")
        return False
    return True


def download_and_save(
    symbol, start, end, directory, max_retries, retry_delay, download_delay
):
    """
    Download and save stock data if not already present.

    Parameters:
        symbol (str): Stock ticker symbol.
        start (str): Start date.
        end (str): End date.
        directory (str): Output directory.
        max_retries (int): Maximum number of retries for failed downloads.
        retry_delay (int): Delay between retries in seconds.
        download_delay (float): Delay between downloads in seconds.
    """
    file_path = os.path.join(directory, f"{symbol}.json")
    if os.path.exists(file_path):
        logging.info(f"Data for {symbol} already exists. Skipping.")
        return

    df = download_stock_data(symbol, start, end, max_retries, retry_delay)
    if df is not None and validate_data(df, symbol):
        save_to_json(symbol, df, directory)
    else:
        # tqdm.write(f"Data for {symbol} is invalid or incomplete.")
        logging.warning(f"Data for {symbol} is invalid or incomplete.")
    time.sleep(download_delay)  # Throttle requests


def parse_arguments():
    """
    Parse command-line arguments using argparse.

    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description="Download historical stock data using yfinance."
    )
    parser.add_argument(
        "--csv",
        type=str,
        default="nasdaq_symbols.csv",
        help="Path to the CSV file containing stock symbols (default: nasdaq_symbols.csv).",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="stock_data_json",
        help="Directory to save JSON files (default: stock_data_json).",
    )
    parser.add_argument(
        "--start",
        type=str,
        default="2020-01-01",
        help="Start date for historical data in YYYY-MM-DD format (default: 2020-01-01).",
    )
    parser.add_argument(
        "--end",
        type=str,
        default="2023-01-01",
        help="End date for historical data in YYYY-MM-DD format (default: 2023-01-01).",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=5,
        help="Number of parallel threads for downloading (default: 5).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.1,
        help="Delay between downloads in seconds to prevent rate limiting (default: 0.1).",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Maximum number of retries for failed downloads (default: 3).",
    )
    parser.add_argument(
        "--retry-delay",
        type=int,
        default=5,
        help="Delay between retries in seconds (default: 5).",
    )
    parser.add_argument(
        "--subsample",
        type=float,
        default=None,
        help="Download data for subsample fraction of the symbols (0.0 to 1.0).",
    )
    parser.add_argument(
        "--seed", type=int, default=42, help="seed passed to library random"
    )
    return parser.parse_args()


def main():
    """
    Main function to execute the downloader with command-line arguments.
    """

    # Setup logging
    setup_logging("download.log")

    args = parse_arguments()

    logging.info("Starting stock data downloader with the following parameters:")
    logging.info(f"CSV File Path: {args.csv}")
    logging.info(f"Output Directory: {args.output}")
    logging.info(f"Date Range: {args.start} to {args.end}")
    logging.info(f"Number of Threads: {args.threads}")
    logging.info(f"Download Delay: {args.delay} seconds")
    logging.info(f"Max Retries: {args.retries}")
    logging.info(f"Retry Delay: {args.retry_delay} seconds")
    if args.subsample is not None:
        logging.info(f"Subsample fraction: {args.subsample}")

    # Load symbols
    symbols = load_symbols(args.csv, args.subsample, args.seed)
    if not symbols:
        logging.error("No symbols to process. Exiting.")
        return

    # Create output directory
    create_output_dir(args.output)

    # Initialize ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        # Submit download tasks
        futures = [
            executor.submit(
                download_and_save,
                symbol,
                args.start,
                args.end,
                args.output,
                args.retries,
                args.retry_delay,
                args.delay,
            )
            for symbol in symbols
        ]

        # Use tqdm to display progress
        for _ in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Downloading Stocks",
            unit="symbol",
        ):
            pass  # Results are handled within download_and_save

    logging.info("Stock data downloader completed.")


if __name__ == "__main__":
    main()
