import pandas as pd

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

# Combine actor's first and last names.
actor_df['actor'] = actor_df['actor_fname'].str.title() + ' ' + actor_df['actor_lname'].str.title()
actor_df.drop(['actor_fname', 'actor_lname'], inplace=True, axis=1)
unknown_actor = pd.DataFrame({'actor_id':[-1], 'actor':['UNKWNON NAME']})
# Create values for unknown actor names.
actor_df = pd.concat([unknown_actor, actor_df])

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
film = film.merge(language_df, how='left', on='language_id')
film_actor = film_actor_df.merge(actor_df, how='left', on='actor_id')
film_actor.drop(['actor_id'], axis=1, inplace=True)
film_actor = film_actor.groupby(['film_id'])['actor'].apply(set).reset_index()

film = film.merge(film_actor, how='left', on='film_id')
film.fillna(value={'actor':-1}, inplace=True)
film.drop(['category_id', 'language_id'], axis=1, inplace=True)

film = film[[
    'film_id', 'title', 'description', 'category', 'language', 'rating', 
    'release_year', 'rental_duration', 'actor',
    'special_features', 'fulltext', 'length',
    'rental_rate', 'replacement_cost',     
    ]]

# Save file to csv.
film.to_csv('staging/dim_films.csv', index=False)
