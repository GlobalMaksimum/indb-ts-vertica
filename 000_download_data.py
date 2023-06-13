"""Download data from amazon s3 bucket to local directory
"""
from datetime import datetime
from pathlib import Path

import arrow
import pandas as pd
from tqdm import tqdm

start = datetime(2013, 7, 1, 0, 0)
end = datetime(2016, 12, 1, 0, 0)

for r in tqdm(
    arrow.Arrow.span_range("month", start, end),
    desc="Downloading monthly data",
    unit="month",
):
    s0, _ = r
    target_parquet = Path("data/nyc-trip") / f"{s0.format('YYYYMM')}.parquet"

    if not target_parquet.exists():
        # print()
        df = pd.read_csv(
            f"https://s3.amazonaws.com/tripdata/{s0.format('YYYYMM')}-citibike-tripdata.zip"
        )

        df.to_parquet(target_parquet)

# print(len(df))

# print(df)
