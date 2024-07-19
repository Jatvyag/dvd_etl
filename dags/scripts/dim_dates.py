import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


start_date = '2022-01-01'
end_date = '2025-12-31'
unknown_date = '1900-01-01'

dim_date = pd.date_range(start=start_date, end=end_date, freq='D').to_series(name='date_id').to_frame()
unknown_frame = pd.date_range(start=unknown_date, end=unknown_date, freq='D').to_series(name='date_id').to_frame()
dim_date = pd.concat([unknown_frame, dim_date])

dim_date['fdate'] = dim_date['date_id']
dim_date['date_id'] = dim_date['date_id'].astype(str).str.replace('-','').astype(int)

dim_date['year'] = dim_date['fdate'].dt.year
dim_date['month'] = dim_date['fdate'].dt.month
dim_date['day'] = dim_date['fdate'].dt.day
dim_date['dow'] = dim_date['fdate'].dt.dayofweek
dim_date['dow_name'] = dim_date['fdate'].dt.day_name(locale='en_US.utf8')
dim_date['month_name'] = dim_date['fdate'].dt.month_name(locale='en_US.utf8')
dim_date['quarter'] = dim_date['fdate'].dt.quarter
dim_date['y-m'] = dim_date.year.astype(str) + '-' + dim_date.month.astype(str)
dim_date['y-q'] = dim_date.year.astype(str) + '-' + dim_date.quarter.astype(str)

dim_date.to_csv('staging/dim_dates.csv', index=False)

print(os.getcwd())