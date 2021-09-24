import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def process_song_file(cur, filepath):
    """
       Description: Read song metadata from a JSON `filepath` file and insert the data into
       song and artist tables based on different columns.

       Arguments:
            cur : psycopg2.cursor
                cursor obtained from active session to execute PostgreSQL commands.
            filepath : str or path object
                path to the song file.
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
        Description: Read logs data from a JSON `filepath` file, filter the logs to include
        only useractivity that has level == 'NextSong', and insert the data into
        users, time, and songplay tables.

        Arguments:
            cur : psycopg2.cursor
                cursor obtained from active session to execute PostgreSQL commands.
            filepath : str or path object
                path to the song file.
    """
    # open log file
    df = pd.read_json(filepath, lines= True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    t = df.copy()
    
    # insert time data records
    time_data = (t.ts, t.ts.dt.hour, t.ts.dt.day, t.ts.dt.dayofweek, t.ts.dt.month, t.ts.dt.year, t.ts.dt.weekday)
    column_labels =  ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(columns = column_labels)

    for index, column_label in enumerate(column_labels):
        time_df[column_label] = time_data[index]

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

        # insert songplay record
        songplay_data =  (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):

    """
        Description: This is the final procedure of the script.
            1. First we get all the files matching extension from the directory.
            2. Gets the number of the total files found.
            3. Then it iterates over the files and finally all the data is processed to our connection.

        Arguments:
            cur : psycopg2.cursor
                cursor obtained from active session to execute PostgreSQL commands.
            filepath : str or path object
                path to the song file.
            conn:
                connection to the database
            func:
                the function that is being used to process the data
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()