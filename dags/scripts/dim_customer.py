import pandas as pd

# Download country list.
country_df = pd.read_csv(
    'source/country.csv',
    sep='\t', 
    header=None, 
    names=['country_id', 'country'], 
    usecols=[0,1]
    )
# Download city list.
city_df = pd.read_csv(
    'source/city.csv', 
    sep='\t', 
    header=None, 
    names=['city_id','city','country_id'], 
    usecols=[0,1,2])
# Download addresses list.
address_df = pd.read_csv(
    'source/address.csv', 
    sep='\t', 
    header=None, 
    names=['address_id','address','address2','district','city_id','zip','phone'], 
    usecols=[0,1,2,3,4,5,6]
    )
# Merge countries and cities.
city_country = city_df.merge(country_df, how='left', on='country_id')
city_country.drop('country_id', axis=1, inplace=True)
# Merge cities and addresses.
addresses = address_df.merge(city_country, how='left', on='city_id')
addresses.drop('city_id', axis=1, inplace=True)
addresses = addresses.astype({'zip':'Int64','phone':'Int64'})
# Change number notation.
pd.options.display.float_format = '{:.0f}'.format
# Addresses 1-2 belongs to stores, addresses 3-4 belongs to staff, so, we remove them from this dimention.
addresses.drop([0,1,2,3], inplace=True)
# There are only 3 rows without district, so, we replace it with 'No district' value.
addresses['district'].fillna('No district', inplace=True)
# The address2 has no value, so, we drop this column.
addresses.drop('address2', axis=1, inplace=True)
# Change the order of the columns.
addresses = addresses[['address_id', 'address', 'district', 'city', 'country', 'zip', 'phone']]

# The dimension that we got only needed in customer dimension, so, we need to marge them.
customer_df = pd.read_csv(
    'source/customer.csv', 
    sep='\t', 
    header=None, 
    names=['customer_id', 'store_id', 'first_name', 'last_name', 'email', 'address_id', 'activebool', 'create_date', 'active'], 
    usecols=[0,1,2,3,4,5,6,7,9]
    )

customer = customer_df.merge(addresses, how='left', on='address_id')
customer.drop('address_id', axis=1, inplace=True)

# The column 'activebool' has only True values, and 'create_date' only '2022-02-14'. So, we deleted them.
customer.drop(['activebool', 'create_date'], axis=1, inplace=True)

# Save file to csv.
customer.to_csv('staging/dim_customer.csv', index=False)
