import pandas as pd
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator

def df_city_country():
    """ Create df city-country which used it two functions. """
    country_df = pd.read_csv(
        'source/country.csv',  
        sep='\t', 
        header=None, 
        names=['country_id', 'country'], 
        usecols=[0,1]
        )
    city_df = pd.read_csv(
        'source/city.csv',  
        sep='\t', 
        header=None, 
        names=['city_id','city','country_id'], 
        usecols=[0,1,2])
    city_country = city_df.merge(country_df, how='left', on='country_id')  # Merge countries and cities.
    city_country.drop('country_id', axis=1, inplace=True)
    return city_country

def df_rental():
    """ Create df rental which used it two functions. """
    # The fact table about renting.
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
    rental = rental_df.merge(inventory_df, how='left', on='inventory_id')  # Merge data about renting and inventory.
    rental.drop(['store_id'], inplace=True, axis=1)
    return rental

def create_dim_dates():
    """ Create dates dimension from scratch. """
    start_date = '2022-01-01'  # Create the first date of dimension.
    end_date = '2025-12-31'  # Create the last date of dimension (leave possibility for DF extension).
    unknown_date = '1900-01-01'   # Create id for unknown date.
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
    dim_date.to_csv('staging/dim_dates.csv', index=False)  # Save file to csv.

def create_dim_inventory():
    """ Create dimension for inventory. """
    # Create inventory.
    store_df = pd.read_csv(
        'source/store.csv',  
        sep='\t', 
        header=None, 
        names=['store_id', 'manager_staff_id', 'address_id'], 
        usecols=[0,1,2]
    )
    address_df = pd.read_csv(
        'source/address.csv',  
        sep='\t', 
        header=None, 
        names=['address_id','address','district','city_id'], 
        usecols=[0,1,3,4],
        nrows=2,
        )
    addresses = address_df.merge(df_city_country(), how='left', on='city_id')  # Merge cities and addresses.
    addresses.drop('city_id', axis=1, inplace=True)
    store = store_df.merge(addresses, how='left', on='address_id')  # Merge store and it address.
    store.drop('address_id', axis=1, inplace=True)
    inventory_df = pd.read_csv(
        'source/inventory.csv',  
        sep='\t', 
        header=None, 
        names=['inventory_id', 'film_id', 'store_id'], 
        usecols=[0,1,2]
    )
    inventory = inventory_df.merge(store, how='left', on='store_id')  # Merge inventory data and stores.
    inventory.drop('store_id', axis=1, inplace=True)
    inventory.to_csv('staging/dim_inventory.csv', index=False)  # Save file to csv.

def create_dim_staff():
    """ Create dimension for staff. """
    staff_df = pd.read_csv(
        'source/staff.csv',  # 
        sep='\t', 
        header=None, 
        names=['staff_id', 'first_name', 'last_name', 'address_id', 'email', 'store_id'], 
        usecols=[0,1,2,3,4,5]
    )
    address_staff_df = pd.read_csv(
        'source/address.csv', 
        sep='\t', 
        header=None, 
        names=['address_id','address','district','city_id'], 
        usecols=[0,1,3,4],
        skiprows=2,
        nrows=2,
        )
    address_staff = address_staff_df.merge(df_city_country(), how='left', on='city_id')  # Merge cities and addresses.
    address_staff.drop('city_id', axis=1, inplace=True)
    staff = staff_df.merge(address_staff, how='left', on='address_id')  # Merge staff and their address.
    staff.drop('address_id', axis=1, inplace=True)
    staff = staff[['staff_id', 'first_name', 'last_name', 'email', 'address', 'district', 'city', 'country', 'store_id']]
    staff.to_csv('staging/dim_staff.csv', index=False)  # Save file to csv.

def create_dim_customer():
    """ Create dimension for customers. """
    country_df = pd.read_csv(
        'source/country.csv',
        sep='\t', 
        header=None, 
        names=['country_id', 'country'], 
        usecols=[0,1]
        )
    city_df = pd.read_csv(
        'source/city.csv', 
        sep='\t', 
        header=None, 
        names=['city_id','city','country_id'], 
        usecols=[0,1,2])
    address_df = pd.read_csv(
        'source/address.csv', 
        sep='\t', 
        header=None, 
        names=['address_id','address','address2','district','city_id','zip','phone'], 
        usecols=[0,1,2,3,4,5,6]
        )
    city_country = city_df.merge(country_df, how='left', on='country_id')  # Merge countries and cities.
    city_country.drop('country_id', axis=1, inplace=True)
    addresses = address_df.merge(city_country, how='left', on='city_id')  # Merge cities and addresses.
    addresses.drop('city_id', axis=1, inplace=True)
    addresses = addresses.astype({'zip':'Int64','phone':'Int64'})  # Change number notation.
    addresses.drop([0,1,2,3], inplace=True)  # There are only 3 rows without district, so, we replace it with 'No district' value.    
    addresses['district'].fillna('No district', inplace=True)  # The address2 has no value, so, we drop this column.
    addresses.drop('address2', axis=1, inplace=True)
    addresses = addresses[['address_id', 'address', 'district', 'city', 'country', 'zip', 'phone']]  # Change the order of the columns.
    customer_df = pd.read_csv(
        'source/customer.csv', 
        sep='\t', 
        header=None, 
        names=['customer_id', 'store_id', 'first_name', 'last_name', 'email', 'address_id', 'activebool', 'create_date', 'active'], 
        usecols=[0,1,2,3,4,5,6,7,9]
        )
    customer = customer_df.merge(addresses, how='left', on='address_id')  # Merge data about customers and their address.
    customer.drop('address_id', axis=1, inplace=True)
    customer.drop(['activebool', 'create_date'], axis=1, inplace=True)  # The column 'activebool' has only True values, and 'create_date' only '2022-02-14'. So, we deleted them.
    customer.to_csv('staging/dim_customer.csv', index=False)  # Save file to csv.

def create_dim_films():
    """ Create dimension for films. """
    film_df = pd.read_csv(
        'source/film.csv',
        sep='\t', 
        header=None, 
        names=[
            'film_id', 
            'title', 
            'description', 
            'release_year', 
            'language_id', 
            'rental_duration', 
            'rental_rate',
            'length',
            'replacement_cost',
            'rating',
            'special_features',
            'fulltext'
        ],
        usecols=[0,1,2,3,4,6,7,8,9,10,12,13]  # Excluding information about original language (it empty).
    )
    actor_df = pd.read_csv(
        'source/actor.csv',
        sep='\t', 
        header=None, 
        names=['actor_id', 'actor_fname', 'actor_lname'],
        usecols=[0,1,2],
    )    
    actor_df['actor'] = actor_df['actor_fname'].str.title() + ' ' + actor_df['actor_lname'].str.title()  # Combine actor's first and last names.
    actor_df.drop(['actor_fname', 'actor_lname'], inplace=True, axis=1)
    unknown_actor = pd.DataFrame({'actor_id':[-1], 'actor':['UNKWNON NAME']})    
    actor_df = pd.concat([unknown_actor, actor_df])  # Create values for unknown actor names.
    category_df = pd.read_csv(
        'source/category.csv',
        sep='\t', 
        header=None, 
        names=['category_id', 'category'],
        usecols=[0,1],
    )
    film_actor_df = pd.read_csv(
        'source/film_actor.csv',
        sep='\t', 
        header=None, 
        names=['actor_id', 'film_id'],
        usecols=[0,1],
    )
    film_category_df = pd.read_csv(
        'source/film_category.csv',
        sep='\t', 
        header=None, 
        names=['film_id', 'category_id'],
        usecols=[0,1],
    )
    language_df = pd.read_csv(
        'source/language.csv',
        sep='\t', 
        header=None, 
        names=['language_id', 'language'],
        usecols=[0,1],
    )
    film = film_df.merge(film_category_df, how='left', on='film_id').merge(category_df, how='left', on='category_id')  # Merge films with categories.
    film = film.merge(language_df, how='left', on='language_id')
    film_actor = film_actor_df.merge(actor_df, how='left', on='actor_id')  # Merge actors (in nested format).
    film_actor.drop(['actor_id'], axis=1, inplace=True)
    film_actor = film_actor.groupby(['film_id'])['actor'].apply(set).reset_index()

    film = film.merge(film_actor, how='left', on='film_id')  # Merge films with actors.
    film.fillna(value={'actor':-1}, inplace=True)  # Provide information for unknown actors.
    film.drop(['category_id', 'language_id'], axis=1, inplace=True)
    film = film[[
        'film_id', 'title', 'description', 'category', 'language', 'rating', 
        'release_year', 'rental_duration', 'actor',
        'special_features', 'fulltext', 'length',
        'rental_rate', 'replacement_cost',     
        ]]  # Reorder columns.
    film.to_csv('staging/dim_films.csv', index=False)  # Save file to csv.

def create_fact_rent():
    """ Create fact table about renting and payments. """
    df_rental().to_csv('staging/fact_rental.csv', index=False)  # Save file to csv.

def create_fact_payment():
    """ Create fact table about payments. """
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
    payment = payment.merge(df_rental()[['inventory_id', 'film_id', 'rental_id']], how='left', on='rental_id')
    payment = payment[[
        'payment_id', 'customer_id', 'staff_id', 'rental_id', 'inventory_id', 'film_id', 
        'amount', 'payment_date']]
    payment.to_csv('staging/fact_payment.csv', index=False)  # Save file to csv.

default_args = {
'owner': '{{ var.value.owner }}',
'email': '{{ var.value.email }}',
'start_date': datetime(2024, 7, 16),
'depends_on_past': False,
'email_on_failure': False,
'email_on_retry': False,
'retries': 3,
'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='create_fact_and_dims_files',
    default_args=default_args,
    description='Pipeline to create necessary for DWH flat files.',
    start_date=datetime(2024, 7, 16),
    schedule_interval='@once',
    catchup=False,
    concurrency=10,
    tags=['ingesting'],
) as dag:
    container_name='postgres_cont'
    
    dim_dates = PythonOperator(
        task_id='dim_dates',
        python_callable=create_dim_dates,
    )

    dim_inventory = PythonOperator(
        task_id='dim_inventory',
        python_callable=create_dim_inventory,
    )

    dim_staff = PythonOperator(
        task_id='dim_staff',
        python_callable=create_dim_staff,
    )

    dim_customer = PythonOperator(
        task_id='dim_customer',
        python_callable=create_dim_customer,
    )

    dim_films = PythonOperator(
        task_id='dim_films',
        python_callable=create_dim_films,
    )

    facts_rental = PythonOperator(
        task_id='facts_rental',
        python_callable=create_fact_rent,
    )

    facts_payment = PythonOperator(
        task_id='facts_payment',
        python_callable=create_fact_payment,
    )


dim_dates >> dim_inventory >> dim_staff >> dim_customer >> dim_films >> facts_rental >> facts_payment
