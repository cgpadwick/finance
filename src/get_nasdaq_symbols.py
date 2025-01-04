import ftplib
import pandas as pd
from io import BytesIO, StringIO


def download_nasdaq_listed():
    # FTP server details
    ftp_host = "ftp.nasdaqtrader.com"
    ftp_directory = "Symboldirectory"
    filename = "nasdaqlisted.txt"

    # Connect to FTP server
    ftp = ftplib.FTP(ftp_host)
    ftp.login()  # Anonymous login
    ftp.cwd(ftp_directory)

    # Retrieve the file as bytes
    buffer = BytesIO()
    ftp.retrbinary(f"RETR {filename}", buffer.write)
    ftp.quit()

    # Decode bytes to string
    data = buffer.getvalue().decode("utf-8")

    # Read the data into a pandas DataFrame
    df = pd.read_csv(StringIO(data), sep="|")

    # Remove the last row if it's a summary or empty
    if "File Creation Time" in df.iloc[-1, 0]:
        df = df[:-1]

    return df


if __name__ == "__main__":
    nasdaq_df = download_nasdaq_listed()
    print(nasdaq_df.head())
    nasdaq_df.to_csv("nasdaq_symbols.csv", index=False)
    print("Wrote file nasdaq_symbols.csv")
