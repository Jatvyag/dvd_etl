import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Download information about films, excluding information about original language.
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
    usecols=[0,1,2,3,4,6,7,8,9,10,12,13]
)

actor_df = pd.read_csv(
    'source/actor.csv',
    sep='\t', 
    header=None, 
    names=['actor_id', 'actor_fname', 'actor_lname'],
    usecols=[0,1,2],
)

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

film = film_df.merge(film_category_df, how='left', on='film_id').merge(category_df, how='left', on='category_id')
film = film.merge(film_actor_df, how='left', on='film_id').merge(actor_df, how='left', on='actor_id')
film = film.merge(language_df, how='left', on='language_id')
film.drop(['category_id', 'actor_id', 'language_id'], axis=1, inplace=True)
film = film[[
    'film_id', 'title', 'description', 'category', 'language', 'rating', 
    'release_year', 'rental_duration', 'actor_fname', 'actor_lname',
    'special_features', 'fulltext', 'length',
    'rental_rate', 'replacement_cost',     
    ]]

# Save file to csv.
film.to_csv('staging/dim_films.csv', index=False)