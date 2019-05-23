""" 
    This file contains the functions to run the ETL job to populate the songplays, users, songs, artists and time tables. 
"""
import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """  
  
    This function processes each song file in the filepath and inserts records into the song and artists tables.
    
    Parameters: 
    curr: Cursor variable with the currently connected DB
    filepath: The top level directory under which all the files are listed
    
    
    Returns: 
    None
    """
    # open song file
    df = df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id','artist_id','year','duration']].values.tolist()[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data =  df[['artist_id','artist_name','artist_location', 'artist_latitude', 'artist_longitude']].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)

def process_log_file(cur, filepath):
    """  
  
    This function processes each log file in the filepath, filters records for the NextSong action 
    and inserts records into the time, user and songplays tables.
    
    
    Parameters: 
    curr: Cursor variable with the currently connected DB
    filepath: The top level directory under which all the files are listed
    
    
    Returns: 
    None
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page=='NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = [t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.dayofweek]
    column_labels = ('timestamp','hour','day','week','month','year','weekday')
    dictionary = dict(zip(column_labels, time_data))
    time_df = pd.DataFrame.from_dict(dictionary)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
            
        timestamp= pd.to_datetime(row.ts, unit='ms')
        # insert songplay record
        songplay_data = (timestamp, row.userId,row.level,songid,artistid,row.sessionId,row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)



def process_data(cur, conn, filepath, func):
    """  
  
    This function obtains all the JSON files from the directory and all subdirectories specified 
    in the filepath parameter. It then iterates over the files and then executes the function specified
    by the func parameter which performs the task to store the data for the fileset passed in. The connection
    is subsequently closed.
  
    Parameters: 
    conn: The connection variable to the DB
    curr: Cursor variable with the currently connected DB
    filepath: The top level directory under which all the files are listed
    func: The function to be executed on the files specified by the filepath
    
    Returns: 
    None
    """    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """ 
    The function to runs the main function on this module. 
  
    This main function first connects to the sparkifydb and then runs the process_data
    method twice, once to process the song files and then to process the log files to ETL
    the song and log data to the respective tables.
  
    Parameters: 
    None
    
    Returns: 
    None
    """      
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()