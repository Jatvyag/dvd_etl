import pandas as pd

store_df = pd.read_csv(
    'source/store.csv',
    sep='\t', 
    header=None, 
    names=['store_id', 'manager_staff_id', 'address_id'], 
    usecols=[0,1,2]
)
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
# Download addresses list for stores.
address_df = pd.read_csv(
    'source/address.csv', 
    sep='\t', 
    header=None, 
    names=['address_id','address','district','city_id'], 
    usecols=[0,1,3,4],
    nrows=2,
    )
# Merge countries and cities.
city_country = city_df.merge(country_df, how='left', on='country_id')
city_country.drop('country_id', axis=1, inplace=True)
# Merge cities and addresses.
addresses = address_df.merge(city_country, how='left', on='city_id')
addresses.drop('city_id', axis=1, inplace=True)
# Merge store and it address.
store = store_df.merge(addresses, how='left', on='address_id')
store.drop('address_id', axis=1, inplace=True)

# Save file to csv.
store.to_csv('staging/dim_store.csv', index=False)

staff_df = pd.read_csv(
    'source/staff.csv',
    sep='\t', 
    header=None, 
    names=['staff_id', 'first_name', 'last_name', 'address_id', 'email', 'store_id'], 
    usecols=[0,1,2,3,4,5]
)
# Download addresses list for staff.
address_staff_df = pd.read_csv(
    'source/address.csv', 
    sep='\t', 
    header=None, 
    names=['address_id','address','district','city_id'], 
    usecols=[0,1,3,4],
    skiprows=2,
    nrows=2,
    )
# Merge cities and addresses.
address_staff = address_staff_df.merge(city_country, how='left', on='city_id')
address_staff.drop('city_id', axis=1, inplace=True)
# Merge staff and their address.
staff = staff_df.merge(address_staff, how='left', on='address_id')
staff.drop('address_id', axis=1, inplace=True)
staff = staff[['staff_id', 'first_name', 'last_name', 'email', 'address', 'district', 'city', 'country', 'store_id']]

# Save file to csv.
staff.to_csv('staging/dim_staff.csv', index=False)
