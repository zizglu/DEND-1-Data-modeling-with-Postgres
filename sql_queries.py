'''
    Data Source Name for the psycopg2 driver used almost everywhere in the project
'''
DSN_STUDENT = "host=127.0.0.1 dbname=studentdb user=student password=student"    # used to create the sparkify db
DSN_SPARKIFY = "host=127.0.0.1 dbname=sparkifydb user=student password=student"  # used to manipulate sparkifydb content


'''
    db script drop & create sparkifydb
'''
DB_SCRIPT = ["DROP DATABASE IF EXISTS sparkifydb",\
             "CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0"]


'''
    Drop Tables : each variables below is used to wipe the content of the db in create_tables.py
'''
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop     = "DROP TABLE IF EXISTS users"
song_table_drop     = "DROP TABLE IF EXISTS songs"
artist_table_drop   = "DROP TABLE IF EXISTS artists"
time_table_drop     = "DROP TABLE IF EXISTS times"


'''
    Create Tables : each variables below are used by create_tables.py to initialize the DB
'''

# Fact Table : songplay is strongly related to users & times table because of there same source 
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
 ( songplay_id SERIAL PRIMARY KEY
 , time_id INT NOT NULL
 , user_id INT NOT NULL
 , level TEXT
 , song_id TEXT
 , artist_id TEXT
 , session_id INT
 , location TEXT
 , user_agent TEXT 
 , FOREIGN KEY (time_id) REFERENCES times (time_id)
 , FOREIGN KEY (user_id) REFERENCES users (user_id));
""")

# Dimension Tables : users - this query doesn't manage duplicate
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
 ( user_id INT PRIMARY KEY
 , first_name TEXT
 , last_name TEXT
 , gender TEXT
 , level TEXT);
""")

# Dimension Tables : times
time_table_create = (""" 
CREATE TABLE IF NOT EXISTS times 
 (time_id SERIAL PRIMARY KEY
 , start_time TIMESTAMP NOT NULL
 , hour INT NOT NULL
 , day INT NOT NULL
 , week INT NOT NULL
 , month INT NOT NULL
 , year INT NOT NULL
 , weekday INT NOT NULL );
""")

# Dimension Tables : artists
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists 
 ( artist_id TEXT PRIMARY KEY
 , name TEXT
 , location TEXT
 , lattitude NUMERIC
 , longitude NUMERIC);
""")

# Dimension Tables : songs has to be always related to an artist
song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
 ( song_id TEXT PRIMARY KEY
 , title TEXT
 , artist_id TEXT NOT NULL
 , year INT
 , duration NUMERIC);
""")



'''
    Insert queries : each variables below are used in the ETL pipeline to do its load jobs
'''

# insert in songplays table
songplayson_table_insert = ("""
INSERT INTO songplays
    ( songplay_id
    , time_id
    , user_id
    , level
    , song_id
    , artist_id
    , session_id
    , location
    , user_agent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
""")

# insert in users table - this query manage upsert on user_id
user_table_insert = ("""
INSERT INTO users 
    ( user_id
    , first_name
    , last_name
    , gender
    , level)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT(user_id) 
    DO UPDATE set level=EXCLUDED.level;
""")

'''
    Insert in artists table
    I create intermediate variables to play with morgify & COPY in etl.ipynb
'''
song_table_insert_fields = "( song_id, title, artist_id, year, duration) "
song_table_insert_params = "(%s, %s, %s, %s, %s) "

song_table_insert_light = ("INSERT INTO songs " \
                           +song_table_insert_fields\
                           +"VALUES ")

song_table_insert = song_table_insert_light +" "+ song_table_insert_params


'''
artist_table_insert_fields = "( artist_id, name, location, lattitude, longitude ) "
artist_table_insert_params = "(%s, %s, %s, %s, %s) "

artist_table_insert_light = ("INSERT INTO artists " \
                           +artist_table_insert_fields\
                           +"VALUES ")
artist_table_insert = artist_table_insert_light +" "+ artist_table_insert_params
'''

# insert in artists table
artist_table_insert = ("""
INSERT INTO artists
    ( artist_id
    , name
    , location
    , lattitude
    , longitude )
    VALUES (%s, %s, %s, %s, %s)
""")


# insert in times table
time_table_insert = ("""
INSERT INTO  times
    (time_id
    , start_time
    , log_hour
    , log_day
    , log_week
    , log_month
    , log_year
    , log_weekday )
    VALUES (%s , %s, %s, %s, %s, %s, %s, %s)
""")


# This query help to find artist & songs ids based on textual data
song_select = ("""
SELECT 
       songs.song_id
     , artists.artist_id
FROM 
    songs 
INNER JOIN 
    artists ON (artists.artist_id = songs.artist_id)
WHERE 
        artists.name = %s
    and songs.title = %s 
    and songs.duration = %s 
""")

'''
    list to facilitate create_table.py job
    order is important in create_table_queries because of foreign keys
'''
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
create_table_queries = [artist_table_create, song_table_create, time_table_create , user_table_create , songplay_table_create ]


'''
    this query help finding where the application is the most used
    it will return 3 column : location, # of songplays, percentage of songplays
    in ordre to work, you to use it like this query_count_songplay_by_location.format(tot_songplay=6820)
    
    Parameters (like ;-)
    ----------  
        tot_songplay - int the totale number of songplays in the db
    Returns
    -------
        list(location, songplays_counter, songplays_percent) : 
'''
query_count_songplay_by_location ="""
SELECT 
     songplays.location as location
    , COUNT(songplays.songplay_id) as songplay_counter
    , COUNT(songplays.songplay_id)::decimal / {tot_songplay} * 100 as songplays_percent
FROM 
    songplays
GROUP BY
    songplays.location 
ORDER BY 
    songplay_counter DESC
"""


'''
    this query will help finding most active users

    Returns
    -------
        list(user_id, user_fullname, songplays_counter)
'''
query_count_user_listen ="""
SELECT 
     users.user_id
    ,users.first_name ||' '|| users.last_name as user_fullname
    ,COUNT( songplays.user_id ) as songplays_counter
FROM 
    songplays
INNER JOIN 
    users ON users.user_id = songplays.user_id
GROUP BY 
    users.user_id
ORDER BY 
      songplays_counter DESC
    , user_fullname    
"""

'''
    this query show peak hours

    Returns
    -------
        list(hour, songplays_counter)
'''
query_songplay_by_hour ="""
SELECT
      times.hour as hour
    , COUNT( songplays.songplay_id ) as songplays_counter
FROM 
    songplays
INNER JOIN 
    times ON times.time_id = songplays.time_id
GROUP BY 
    times.hour
ORDER BY 
    times.hour
"""