import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import io

def get_files( root_directory , file_search_query = '*' ):
    '''
        lookup for json file in root_directory and return a list of full file path
        
        Parameters
        ----------
            root_directory : str - filepath
            file_search_query : str - a query string for files, will be appended to each founded folder
        Returns
        -------
            list(str) : a list of full path on each files
        Raise
        -----
            ValueError if filepath is empty  
    '''
        
    if( root_directory == "" ):
        raise ValueError('filepath must be defined')
        
    all_files = []
    #iterate over the folder tree structure
    for root, dirs, files in os.walk( root_directory ):
        #search json files on each folder
        files = glob.glob( os.path.join( root , file_search_query ) )
        for f in files :
            #concatenate result in the list
            all_files.append(os.path.abspath(f))
    
    return all_files


def create_dataframe_from_jsonfiles(root_directory):
    '''
        Concatenante all json files founded in root_directory
        Drop duplicate directly
        Parameters
        ----------       
            root_directory str - the folder to import in a dataframe
        Returns
        -------        
            a Pandas dataframe
    '''
    
    #get all files
    files = get_files(root_directory , '*.json' )

    #load each files in a data frame
    df_from_each_json = ( pd.read_json( f , lines = True ) for f in files )

    #concatenate each dataframe in one
    df_concatenated = pd.concat( df_from_each_json , ignore_index = True )

    #drop duplicates
    return df_concatenated.drop_duplicates(inplace=False) 

        
def clean_dataframe_for_export( df , force_empty_string_to_null = True , null_string = 'NULL' , sep='\t' , sep_replace_with=''):
    '''
       Prepare (&uniformise) the DF for CSV exportation
       Replace NaN or empty cell with NULL 
        
        Parameters
        ----------       
            df (Dataframe) - the Pandas dataframe
            force_empty_string_to_null (bool) - by default empty strings are nullified
            null_string (str) - the string used to represent NULL (by def)
            sep (str) - to separator choosed in CSV file..
            sep_replace_with (str) - .. to replce with
        Returns
        -------        
            a Pandas dataframe
    '''   
    #NaN becomes NULL
    df = df.fillna(null_string)
    
    #replace all empty cells with NULL
    if( force_empty_string_to_null ): df = df.replace('', null_string)
    
    #avoid usage of tab in all cells...
    df = df.replace(sep, sep_replace_with)
    
    return df


def lookup_song_and_artist( params , cur , query ):
    '''
        >> Function to apply in a dataframe that came from a Log Dataset <<
        Based on params[], query the DB for artist & song ID
        Parameters
        ----------
            It is tricky to use to because params must be well ordered
            params[0] : str - artist name
            params[1] : str - song title
            params[2] : decimal - length of songs
            cur : cursor - the psycopg cursor used to trigger the query
            query : str - the sql query to execute with params (to be passed bay apply(args=) )
        Returns
        -------
            list(songid, artistid) : 
                - a list of related entity db primary key
                - 'NULL', 'NULL' if nothing found
                - 'Error' , 'exception error' in case of exceptions
    '''
    #concretise params
    artist = params[0]
    song = params[1]
    length = params[2]
        
    
    try:
        # query the db
        cur.execute( query , (artist, song , length))
        results = cur.fetchone()
    except psycopg2.Error as e:
        # catch the error and return an empty result
        results = 'Error' , e 
        
    if results:
        #ok we have a match or an error
        songid, artistid = results
    else:
        #instead we nulls
        songid, artistid = 'NULL', 'NULL'
        
    return ( songid, artistid )  


def copy_df_to_db( cur , df , tablename , table_columns , with_index=False , separator='\t'):
    '''
        Manage the df copy to db. 
        This functions manage the creation of the CSV to be imported
        
        Parameters
        ----------       
            cur - cursor to the database where the copy_from will be performed
            df - Pandas dataframe to export to SQL
            tablename - table where Pandas data will be imported
            table_columns (str) - list of the sql columns in tablename
            with_index=False - do we export the df index as first column ?
            separator='\t' - separator 
    '''
    # create a buffer for CSV infos
    buffer = io.StringIO()
    
    #serialize the dataframe into the buffer (no header at all)
    df.to_csv( buffer , index=with_index , header=False, sep=separator)
    
    # move the pointer at the start of the stream to do another iteration
    buffer.seek(0)
 
    try:
        cur.copy_from(buffer, tablename , sep=separator , columns=table_columns , null='NULL' )
    except psycopg2.Error as e:
        print('Error while processing table{} : {}'.format(tablename , e ))   
        
def process_song_files(cur, filepath):
    '''
        ETL on Song Dataset
        
        Parameters
        ----------  
            cur (cursor) - allow execution of SQL queries in the database
            filepath (str) - place where json song files resides
    '''
    
    ### EXTRACT ###
    df_songs_json = create_dataframe_from_jsonfiles(filepath)
    
    ### TRANSFORM ###
    #extract songs infos and load them to the DB
    df_songs = df_songs_json[['song_id','title','artist_id','year','duration']].copy()
    df_songs = clean_dataframe_for_export(df_songs)
    
    
    #extract artists infos and load them to the DB
    df_artists = df_songs_json[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].copy()
    df_artists.drop_duplicates(inplace=True)    
    df_artists = clean_dataframe_for_export(df_artists)

    ### LOAD ###
    copy_df_to_db(cur, df_artists, 'artists',( 'artist_id', 'name', 'location', 'lattitude', 'longitude' ) )
    copy_df_to_db(cur, df_songs, 'songs', ( 'song_id', 'title', 'artist_id', 'year', 'duration') )    


def process_log_files(cur, filepath):
    '''
        ETL on Log Dataset
        
        Parameters
        ----------  
            cur (cursor) - allow execution of SQL queries in the database
            filepath (str) - place where json song files resides
    '''
    
    ### EXTRACT ###
    #load log's
    df_logs_json = create_dataframe_from_jsonfiles('data/log_data')
    #filter on NextSong logs entries
    df_NextSong = df_logs_json[ df_logs_json['page'] == 'NextSong' ].copy()
    
    ### TRANSFORM ###
    
    df_NextSong = clean_dataframe_for_export(df_NextSong)
    
    #convert the ts column to a datetime column
    df_NextSong['start_time'] = pd.to_datetime( df_NextSong['ts'], unit='ms')
    df_NextSong['start_time'] = df_NextSong['start_time'].astype('datetime64[s]')

    #append column needed for times table
    df_NextSong['hour'] = df_NextSong['start_time'].dt.hour
    df_NextSong['day'] = df_NextSong['start_time'].dt.day
    df_NextSong['week'] = df_NextSong['start_time'].dt.week
    df_NextSong['month'] = df_NextSong['start_time'].dt.month
    df_NextSong['year'] = df_NextSong['start_time'].dt.year
    df_NextSong['weekday'] = df_NextSong['start_time'].dt.weekday

    #avoid duplicate caused by type mismatch
    df_NextSong['userId'] = pd.to_numeric( df_NextSong['userId'])


    ## prepare the dataframes for import
    
    df_times = df_NextSong[['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']]
    
    
    df_users = df_NextSong[['userId', 'firstName', 'lastName', 'gender', 'level']].copy()
    #clean duplicates on userID
    df_users.drop_duplicates( inplace=True, keep='last' , subset=['userId', 'firstName', 'lastName', 'gender'])
    
    
    df_songplay = df_NextSong[['userId', 'level', 'sessionId',  'location', 'userAgent','artist','song','length']].copy()
    
    # keep the 1:1 relation beetween times & songplay table
    df_songplay['time_id'] = df_songplay.index
    
    #lookup artist & song IDs
    df_songplay['songid'],  df_songplay['artistid'] = zip(*df_songplay[['artist','song' , 'length' ]].apply( lookup_song_and_artist , axis=1 , args=(cur, song_select,)))
    #rearrange the column
    df_songplay = df_songplay[['time_id','userId','level','songid', 'artistid','sessionId','location','userAgent']]

    
    ### LOAD ###
    copy_df_to_db(cur, df_times, 'times', ( 'time_id', 'start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday') , with_index=True )
    copy_df_to_db(cur, df_users, 'users', ('user_id', 'first_name', 'last_name', 'gender', 'level') )
    copy_df_to_db(cur, df_songplay, 'songplays', ('songplay_id', 'time_id', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent'), with_index=True )
    

def main():
    '''
        ETL lauching point
    '''    
    conn = None
    cur = None

    try:
        conn = psycopg2.connect( DSN_SPARKIFY )
        conn.autocommit = True
        cur = conn.cursor()
    except psycopg2.Error as e:
        print('Error while initializing the db connection : {}'.format( e ))   

    print( '*********** Processing song_data.....')
    process_song_files(cur, 'data/song_data')
    
    print( '*********** Processing log_data.....')
    process_log_files(cur, 'data/log_data')

    if( conn ):conn.close()
    print( '*********** Processing terminated.....')
    

if __name__ == "__main__":
    main()