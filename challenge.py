def movies_ETL(wiki_data, kaggle_data, ratings_data):
    print('Lets start!')
    print('Importing dependencies')
    import json
    import pandas as pd
    import numpy as np
    import re
    from sqlalchemy import create_engine
    from config import db_password
    import time

    #Ask for directions
    print('To make this work, there are few things to get done firts:')
    file_dir=input('Provide path to files directory: ')
    input('Create a config.py file on file path and on first line write db_password=''Your Passord Here''.\nDone?')
    input('Create a new database on Postgres and name as movie_data. Done?')

    try:
        with open(f'{file_dir}/{wiki_data}.json', mode='r') as file:
            wiki_movies_raw=json.load(file)
    except FileNotFoundError as fnf_error:
        print(fnf_error)
        print('Please correct file or directoryd and start again')
        exit

    try:
        kaggle_metadata=pd.read_csv(f'{file_dir}/{kaggle_data}.csv', low_memory=False)
    except FileNotFoundError as fnf_error:
        print(fnf_error)
        print('Please correct file or directoryd and start again')
        exit
    
    try:
        ratings=pd.read_csv(f'{file_dir}/{ratings_data}.csv')
    except FileNotFoundError as fnf_error:
        print(fnf_error)
        print('Please correct file or directoryd and start again')
        exit

    print('Analysing Wiki_data')
    try:
        #Filter only for movies (no series) that contain Director(by) and IMDB link
        wiki_movies= [movie for movie in wiki_movies_raw
                     if ('Director' in movie or 'Directed by' in movie)
                     and 'imdb_link' in movie
                     and 'No. of episodes' not in movie]
        # Get DataFrame for wiki_movies
        wiki_movies_df=pd.DataFrame(wiki_movies)

        #v_1: Create a function to aggregate all the alternative titles into one
        def clean_movie(movie):
            movie = dict(movie) #create a non-destructive copy
            alt_titles = {}
            for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                        'Hangul','Hebrew','Hepburn','Japanese','Literally',
                        'Mandarin','McCune–Reischauer','Original title','Polish',
                        'Revised Romanization','Romanized','Russian',
                        'Simplified','Traditional','Yiddish']:
                if key in movie:
                    alt_titles[key] = movie[key]
                    movie.pop(key)
            if len(alt_titles) > 0:
                movie['alt_titles'] = alt_titles

            #Reame columns
            def change_column_name(old_name, new_name):
                if old_name in movie:
                    movie[new_name]=movie.pop(old_name)
            change_column_name('Adaptation by', 'Writer(s)')
            change_column_name('Country of origin', 'Country')
            change_column_name('Directed by', 'Director')
            change_column_name('Distributed by', 'Distributor')
            change_column_name('Edited by', 'Editor(s)')
            change_column_name('Length', 'Running time')
            change_column_name('Original release', 'Release date')
            change_column_name('Music by', 'Composer(s)')
            change_column_name('Produced by', 'Producer(s)')
            change_column_name('Producer', 'Producer(s)')
            change_column_name('Productioncompanies ', 'Production company(s)')
            change_column_name('Productioncompany ', 'Production company(s)')
            change_column_name('Released', 'Release Date')
            change_column_name('Release Date', 'Release date')
            change_column_name('Screen story by', 'Writer(s)')
            change_column_name('Screenplay by', 'Writer(s)')
            change_column_name('Story by', 'Writer(s)')
            change_column_name('Theme music composer', 'Composer(s)')
            change_column_name('Written by', 'Writer(s)')

            return movie

        #Apply clean_movie to wiki_movies
        clean_movies=[clean_movie(movie) for movie in wiki_movies]
        #Create dataframe
        wiki_movies_df=pd.DataFrame(clean_movies)
        #Extract the IMDB ID from 'imdb_link'
        wiki_movies_df['imdb_id']=wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
        wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)
        # Keep only the columns that contain 90% of the total entries
        wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df)*0.9]
        wiki_movies_df=wiki_movies_df[wiki_columns_to_keep]

    except:
        print('Wiki_data not loaded correctly')
        pass
        
    print('Analysing Box Office data')
    try:
        #Clean and correct data on Box Office to be numeric
        box_office=wiki_movies_df['Box office'].dropna()
        #Join data 
        box_office=box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)
        #Define variables
        form_one=r'\$\s*\d+\.?\d*\s*[mb]illi?on'
        form_two=r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'
        box_office=box_office.str.replace(r'\$.*[-—–](?![a-z])' , '$', regex=True)
        matches_form_one=box_office.str.contains(form_one, flags=re.IGNORECASE)
        matches_form_two=box_office.str.contains(form_two, flags=re.IGNORECASE)
        box_office[~ matches_form_one & ~ matches_form_two]
        #Create funtion to change numbers format
        def parse_dollars(s):
            # if s is not a string, return NaN
            if type(s) != str:
                return np.nan

            # if input is of the form $###.# million
            if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):
                # remove dollar sign and " million"
                s=re.sub('\$|\s|[a-zA-Z]','', s)
                # convert to float and multiply by a million
                value=float(s) *10**6
                # return value
                return value
            # if input is of the form $###.# billion
            elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):
                # remove dollar sign and " billion"
                s=re.sub('\$|\s|[a-zA-Z]','', s)
                # convert to float and multiply by a billion
                value=float(s) *10**9
                # return value
                return value
            # if input is of the form $###,###,###
            elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):
                # remove dollar sign and commas
                s=re.sub('\$|,', '', s)
                # convert to float
                value=float(s)
                # return value
                return value
            # otherwise, return NaN
            else:
                return np.nan
        #Apply parse_dolars to new column box_office
        wiki_movies_df['box_office']=box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
        #Remove the Box office column
        wiki_movies_df.drop('Box office', axis=1, inplace=True)

    except:
        print('Box orffice data not loaded correctly')
        pass        
        
    #Budget data analysis
    try:
        print('Analysing Budget Data')
        budget=wiki_movies_df['Budget'].dropna()
        #Convert lists to strings
        budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)
        budget=budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
        #Redefine variables
        form_one=r'\$\s*\d+\.?\d*\s*[mb]illi?on'
        form_two=r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'
        budget=budget.str.replace(r'\$.*[-—–](?![a-z])' , '$', regex=True)
        matches_form_one = budget.str.contains(form_one, flags=re.IGNORECASE)
        matches_form_two = budget.str.contains(form_two, flags=re.IGNORECASE)
        budget = budget.str.replace(r'\[\d+\]\s*', '')
        budget[~matches_form_one & ~matches_form_two]

        wiki_movies_df['budget']=budget.str.extract(f'({form_one}|{form_two})', flags=re.I).apply(parse_dollars)
        wiki_movies_df.drop('Budget', axis=1,inplace=True)

    except:
        print('Budget data not loaded correctly')
        pass
        
    #Release date data analysis
    try:
        print('Analysing Release Date Data')
        #Covert lists to strings
        release_date=wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x)==list else x)
        date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
        date_form_two = r'\d{4}.[01]\d.[123]\d'
        date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
        date_form_four = r'\d{4}'
        release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})',flags=re.I)
        wiki_movies_df['release_date']=pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)

    except:
        print('Release date data not loaded correctly')
        pass        
        
    #Running Time data analysis
    try:
        print('Analysing Running Time Data')
        #Convert lists to string
        running_time=wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x)==list else x)
        running_time_extract=running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
        running_time_extract=running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
        wiki_movies_df['running_time']=running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2]==0 else row[2], axis=1)
        wiki_movies_df.drop('Running time', axis=1, inplace=True)

    except:
        print('Running time data not loaded correctly')
        pass        
        
    #Kaggle data analysis
    try:
        print('Analysing Kaggle Data')
        #Clean the Kaggle Data
        kaggle_metadata=kaggle_metadata[kaggle_metadata['adult']=='False'].drop('adult', axis='columns')
        kaggle_metadata['video']=kaggle_metadata['video']=='True'
        kaggle_metadata['budget']=kaggle_metadata['budget'].astype(int)
        kaggle_metadata['id']=pd.to_numeric(kaggle_metadata['id'], errors='raise')
        kaggle_metadata['popularity']=pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
        kaggle_metadata['release_date']=pd.to_datetime(kaggle_metadata['release_date'])
 
    except:
        print('Kaggle data not loaded correctly')
        pass
        
    #Ratings data analysis
    try:
        print('Ratings Data Analysis')
        ratings['timestamp']=pd.to_datetime(ratings['timestamp'], unit='s')

    except:
        print('Ratings data not loaded correctly')
        pass        
        
    #Merge Wiki and kaggle dataframes
    try:
        print('Merge dataframes')
        movies_df=pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])
        #Drop know wrong point
        movies_df = movies_df.drop(movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date_kaggle'] < '1965-01-01')].index)
        #Drop the columns from wiki
        movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)
        #Create function to deal with the merged data
        def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
            df[kaggle_column] = df.apply(
                lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column]
                , axis=1)
            df.drop(columns=wiki_column, inplace=True)

        #Merge data
        fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
        fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
        fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')

        # Reorganize columns
        movies_df = movies_df.loc[:, ['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                            'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                            'genres','original_language','overview','spoken_languages','Country',
                            'production_companies','production_countries','Distributor',
                            'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                            ]]

        #Rename columns
        movies_df.rename({'id':'kaggle_id',
                        'title_kaggle':'title',
                        'url':'wikipedia_url',
                        'budget_kaggle':'budget',
                        'release_date_kaggle':'release_date',
                        'Country':'country',
                        'Distributor':'distributor',
                        'Producer(s)':'producers',
                        'Director':'director',
                        'Starring':'starring',
                        'Cinematography':'cinematography',
                        'Editor(s)':'editors',
                        'Writer(s)':'writers',
                        'Composer(s)':'composers',
                        'Based on':'based_on'
                        }, axis='columns', inplace=True)

    except:
        print('Merge data not loaded correctly')
        raise
        pass        
        
    # Transform and Merge Rating Data
    try:
        #Organize data of ratings
        rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count() \
                        .rename({'userId':'count'}, axis=1) \
                        .pivot(index='movieId',columns='rating', values='count')
        #rename the columns to start with rating_
        rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
        #Merge ratings data to movies_df
        movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')
        #Fill null values with zeros
        movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
        # Create the Database Engine
        db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"
        engine = create_engine(db_string)

    except:
        print('Merge ratings data not loaded correctly')
        raise
        pass        
        
    # Import the Movie Data
    print('Importing to SQL movie_data')
    try:
        movies_df.to_sql(name='movies', con=engine)
    except ValueError as ve:
        print(ve)
        replace=input('Do you want to replace data? (yes/no)')
        if replace=='yes':
            movies_df.to_sql(name='movies', con=engine, if_exists='replace')
            print('Data replaced!')
        else:
            print('We are done for today!')
            exit

    # Import the Ratings Data into chunks
    print('Importing to SQL ratings data! Beaware, this will take time! Enjoy a coffee (or two)!')
    rows_imported = 0
    # get the start_time from time.time()
    start_time = time.time()
    for data in pd.read_csv(f'{file_dir}/ratings.csv', chunksize=1000000):
        print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
        data.to_sql(name='ratings', con=engine, if_exists='replace')
        rows_imported += len(data)

        # add elapsed time to final print out
        print(f'Done. {time.time() - start_time} total seconds elapsed')

    print('Congratulations!')
