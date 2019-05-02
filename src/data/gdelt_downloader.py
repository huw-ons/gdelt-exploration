import time
import gdelt
import pandas as pd
import numpy as np
import argparse


def download_data(start_date, end_date, filename, chunks=1):
    dates = pd.date_range(start_date, end_date).to_list()
    dates = [str(x)[:10] for x in dates]

    gd = gdelt.gdelt(version=2.0)
    i = 0

    for date_chunk in np.array_split(dates, chunks):
        i = i + 1
        print(f"Fetching {date_chunk[0]} to {date_chunk[-1]}")
        results = gd.Search(list(date_chunk), table='gkg', coverage=True)
        results.dropna(subset=["V2Locations"], inplace=True)
        results = results[
            results["V2Locations"].str.contains("United Kingdom|UK|GB")
        ]

        results = results.set_index("GKGRECORDID")

        if chunks > 1:
            results.to_csv(f"~/GDELT/data/raw/{filename}_{i}.csv")

        else:
            results.to_csv(f"~/GDELT/data/raw/{filename}.csv")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("start_date", help="Start date in format YYYY-MM-DD")
    parser.add_argument("end_date", help="End date in format YYYY-MM-DD")
    parser.add_argument("filename", help="File name to save data as (no .csv extension needed")
    parser.add_argument("-c", "--chunks", nargs="?", default=1, help="Optional chunk size", type=int)

    args = parser.parse_args()

    start = time.time()
    download_data(args.start_date, args.end_date, args.filename, args.chunks)
    end = time.time()
    print(f"Time taken to process: {end-start}")