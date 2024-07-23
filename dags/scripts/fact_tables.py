import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

rental_df = pd.read_csv(
    'source/rental.csv',
    sep='\t', 
    header=None, 
    names=[
        'rental_id', 
        'rental_date', 
        'inventory_id', 
        'customer_id', 
        'return_date', 
        'staff_id', 
    ],
    usecols=[0,1,2,3,4,5]
)

inventory_df = pd.read_csv(
    'source/inventory.csv',
    sep='\t', 
    header=None, 
    names=['inventory_id', 'film_id', 'store_id'], 
    usecols=[0,1,2]
)

rental = rental_df.merge(inventory_df, how='left', on='inventory_id')
rental.drop(['store_id'], inplace=True, axis=1)

# Save file to csv.
rental.to_csv('staging/fact_rental.csv', index=False)


def create_payment(num):
    df = pd.read_csv(
    f'source/payment_p2022_0{num}.csv',
    sep='\t', 
    header=None, 
    names=[
        'payment_id', 
        'customer_id', 
        'staff_id', 
        'rental_id', 
        'amount', 
        'payment_date', 
    ],
    )
    return df

payment = create_payment(1)
for i in range(2,8):
    payment = pd.concat([payment, create_payment(i)])

payment = payment.merge(rental[['inventory_id', 'film_id', 'rental_id']], how='left', on='rental_id')
payment = payment[[
    'payment_id', 'customer_id', 'staff_id', 'rental_id', 'inventory_id', 'film_id', 
    'amount', 'payment_date']]

# Save file to csv.
payment.to_csv('staging/fact_payment.csv', index=False)