                                                          # Needed Modules

import googleapiclient.discovery 
from pprint import pprint
import pandas as pd
import pymongo as pm
import streamlit as st
import pyodbc

                                                         # API service Connection
    
api_key = 'AIzaSyC8C6Sx1i25VJzDzDe-XdxRrA76kUpOpwQ'
api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey = api_key)

                                                        # Getting Channel Details    

def channel_info_here(channel_id):
    request = youtube.channels().list(
                      part = "snippet,contentDetails,statistics",
                      id = channel_id
    )
    Channel_Response = request.execute()
    
    for i in Channel_Response["items"]:
        data  = dict(Channel_Name = i ["snippet"]["title"],
                    Channel_Id = i ["id"],
                    Subscribers = i ["statistics"]["subscriberCount"],
                    Views = i ["statistics"]["viewCount"],
                    Total_Videos = i ["statistics"]["videoCount"],
                    Playlist_Id = i ["contentDetails"]["relatedPlaylists"]["uploads"],
                    Channel_Desription = i ["snippet"]["description"])
    return data

                                                        # Getting Video Id Details    
def video_ids_here(channel_id):
    
    video_ids = []
    response = youtube.channels().list(id = channel_id,
                                      part = "contentDetails").execute()
    Playlist_id =  response['items'][0]['contentDetails']["relatedPlaylists"]["uploads"]

    next_page_token = None


    while True:
        response1 = youtube.playlistItems().list(
                                              part = 'snippet',
                                              playlistId = Playlist_id,
                                              maxResults = 50,
                                              pageToken = next_page_token).execute()

        for i in range (len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response1.get("nextPageToken")

        if next_page_token is None:
            break
    return video_ids

                                                          # Getting Videos Details    


def video_details_here(video_id):
    video_info = []
    for i in video_id:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id = i 
        )
        response = request.execute()

        for inner in response["items"]:
            data = dict(Channel_Name = inner ['snippet']['channelTitle'],
                        Channel_Id = inner ['snippet']['channelId'],
                        Video_Id = inner ['id'],
                        Title = inner ['snippet']['title'],
                        Tags = inner['snippet'].get('tage'),
                        Description = inner['snippet'].get('description'),
                        Published_Date = inner['snippet']['publishedAt'],
                        Duration = inner ['contentDetails']['duration'],
                        Views = inner['statistics'].get('viewCount'),
                        Likes= inner['statistics'].get('likeCount'),
                        Comments = inner ['statistics'].get('commentCount'),
                        Favorite_Count = inner ['statistics']['favoriteCount'],
                        Definition = inner ['contentDetails']['definition'],
                        Caption_Status = inner ['contentDetails']['caption']
                       )
            video_info.append(data)
            
    return video_info

                                                            # Getting Comment Details    


def comment_details_here(video_ids):
    comment_datas = []
    try:
        for i in video_ids:
            request = youtube.commentThreads().list(
                part = "snippet",
                videoId = i ,
                maxResults = 100
            )
            response = request.execute()

            for i in response['items']:
                data = dict(Comment_Id = i ['snippet']['topLevelComment']['id'],
                            Comment_Text = i ['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author = i ['snippet']['topLevelComment'] ['snippet']['authorDisplayName'],
                            Comment_Published = i ['snippet']['topLevelComment'] ['snippet']['publishedAt'],
                            Video_Id = i ['snippet']['topLevelComment']['snippet']['videoId'])

                comment_datas.append(data)
    except:
        pass
    
    return comment_datas


                                                         # Getting Playlist Details    

        


def playlist_ids_here(channel_id):

    All_data = []
    next_page_token = None

    while True:

        request = youtube.playlists().list(
                part = "snippet,contentDetails",
                channelId = channel_id,
                maxResults = 50,
                pageToken = next_page_token

            )
        response = request.execute()

        for i in response['items']:
            data = dict(Palylist_id = i['id'],
                       Title = i ['snippet']['title'],
                       Channel_Id = i['snippet']['channelId'],
                       Channel_Name = i ['snippet']['channelTitle'],
                       PublishedAt = i['snippet']['publishedAt'],
                       Video_Count = i ['contentDetails']['itemCount'])
            All_data.append(data)

        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break
            
    return All_data


                                                        # upload to mongoDB

client = pm.MongoClient('mongodb://localhost:27017')
DataBase = client["Youtube_data"]

def channel_details(channel_id):
    ch_details = channel_info_here(channel_id)
    pl_details = playlist_ids_here(channel_id)
    vi_ids = video_ids_here(channel_id)
    vi_details = video_details_here(vi_ids)
    com_details = comment_details_here(vi_ids)
    
    collection1 = DataBase["channel_details"]
    collection1.insert_one({'channel_information':ch_details,'playlist_information':pl_details,
                           'video_information': vi_details,'comment_information':com_details})
    
    return "upload completed successfully"

            
                                                            # Microsoft SQL Server
                                              # Creating Table for Channel and Insert The Datas


def channels_table():
    
    connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                                'SERVER=SMARTSATHISH;'
                                'DATABASE=youtube_data;'
                                'UID=sa;'
                                'PWD=admin9')
    cursor = connection.cursor()
    
    drop_query = '''drop table if exists Channels'''
    cursor.execute(drop_query)
    connection.commit()


    try:
        create_table_query = """
            CREATE TABLE Channels (
                Channel_Name varchar(100),
                Channel_Id char(100) primary key,
                Subscribers bigint,
                Views bigint,
                Total_Videos int,
                Playlist_Id varchar(80),
                Channel_Desription text
                )
    """
        cursor.execute(create_table_query)
        connection.commit()

    except:
        print("already created")


    ch_list = []

    DataBase = client['Youtube_data']
    collection1 = DataBase['channel_details']

    for ch_data in collection1.find({},{'_id':0,'channel_information':1}):
        ch_list.append(ch_data['channel_information'])
        
    DataF = pd.DataFrame(ch_list)

    for index, row in DataF.iterrows():
        insert_query = '''
            INSERT INTO Channels(
             Channel_Name,
             Channel_Id,
             Subscribers,
             Views,
             Total_Videos,
             Playlist_Id,
             Channel_Desription)

             VALUES (?, ?, ?, ?, ?, ?, ?)'''

        values = (row['Channel_Name'],
                  row['Channel_Id'],
                  row['Subscribers'],
                  row['Views'],
                  row['Total_Videos'],
                  row['Playlist_Id'],
                  row['Channel_Desription'])

        try:
            cursor.execute(insert_query, values)
            connection.commit()
        except:
            print('Channel values is already inserted')
            

            
                                            # Creating Table for Playlists and Insert The Datas

            
                
def playlist_table():

    connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                                'SERVER=SMARTSATHISH;'
                                'DATABASE=youtube_data;'
                                'UID=sa;'
                                'PWD=admin9')
    cursor = connection.cursor()

    drop_query = '''drop table if exists playlists'''
    cursor.execute(drop_query)
    connection.commit()


    create_playlists_query = """
        CREATE TABLE playlists (
            Palylist_id varchar(100) primary key,
            Title varchar(100),
            Channel_Id varchar(100),
            Channel_Name varchar(100),
            PublishedAt varchar(100),
            Video_Count int
            )
    """
    cursor.execute(create_playlists_query)
    connection.commit()
    
    
    pl_list = []
    DataBase = client['Youtube_data']
    collection1 = DataBase['channel_details']

    for pl_data in collection1.find({},{'_id':0,'playlist_information':1}):
        for i in range(len(pl_data['playlist_information'])):
            pl_list.append(pl_data['playlist_information'][i])
    DataF1 = pd.DataFrame(pl_list)
    
    for index, row in DataF1.iterrows():
            insert_query = '''
                INSERT INTO playlists(
                 Palylist_Id,
                 Title,
                 Channel_Id,
                 Channel_Name,
                 PublishedAt,
                 Video_Count)

                 VALUES (?, ?, ?, ?, ?, ?)'''

            values = (row['Palylist_id'], row['Title'], row['Channel_Id'], row['Channel_Name'], row['PublishedAt'], row['Video_Count'])

            cursor.execute(insert_query, values)
            connection.commit()

            
            
                                         # Creating Table for Videos and Insert The Datas

            
            
def videos_table():

    connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                                'SERVER=SMARTSATHISH;'
                                'DATABASE=youtube_data;'
                                'UID=sa;'
                                'PWD=admin9')
    cursor = connection.cursor()

    drop_query = '''drop table if exists videos'''
    cursor.execute(drop_query)
    connection.commit()


    create_Videos_query = """
        create table videos (Channel_Name varchar (300),
                                Channel_Id varchar (100),
                                Video_Id varchar (30) primary key,
                                Title varchar (150),
                                Description text,
                                Published_Date varchar(100),
                                Duration varchar(100),
                                Views bigint,
                                Likes bigint,
                                Comments int,
                                Favorite_Count int,
                                Definition varchar(20),
                                Caption_Status varchar(50)
                                )
    """


    cursor.execute(create_Videos_query)
    connection.commit()


    vi_list = []
    DataBase = client['Youtube_data']
    collection1 = DataBase['channel_details']

    for vi_data in collection1.find({},{'_id':0,'video_information':1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])
    DataF2 = pd.DataFrame(vi_list)
    
    
    for index, row in DataF2.iterrows():

        insert_query = '''
            INSERT INTO videos(
                Channel_Name,
                Channel_Id,
                Video_Id ,
                Title,
                Description,
                Published_Date,
                Duration,
                Views,
                Likes,
                Comments,
                Favorite_Count,
                Definition,
                Caption_Status
                )

                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

        values = (row['Channel_Name'],
                  row['Channel_Id'],
                  row['Video_Id'],
                  row['Title'],
                  row['Description'],
                  row['Published_Date'],
                  row['Duration'],
                  row['Views'],
                  row['Likes'],
                  row['Comments'],
                  row['Favorite_Count'],
                  row['Definition'],
                  row['Caption_Status']
                 )

        cursor.execute(insert_query, values)
        connection.commit()

        
        
                                         # Creating Table for Comments and Insert The Datas

        
        
def comments_table():

    connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                                'SERVER=SMARTSATHISH;'
                                'DATABASE=youtube_data;'
                                'UID=sa;'
                                'PWD=admin9')
    cursor = connection.cursor()

    drop_query = '''drop table if exists comments'''
    cursor.execute(drop_query)
    connection.commit()


    create_comments_query = """
        CREATE TABLE comments (
            Comment_Id varchar(100) primary key,
            Comment_Text text,
            Comment_Author varchar(150),
            Comment_Published varchar(100),
            Video_Id varchar(100)
            )
    """

    cursor.execute(create_comments_query)
    connection.commit()

    com_list = []
    DataBase = client['Youtube_data']
    collection1 = DataBase['channel_details']

    for com_data in collection1.find({},{'_id':0,'comment_information':1}):
        for i in range(len(com_data['comment_information'])):
            com_list.append(com_data['comment_information'][i])
            
    DataF3 = pd.DataFrame(com_list)

    for index, row in DataF3.iterrows():

        insert_query = '''
            INSERT INTO comments(
                Comment_Id,
                Video_Id,
                Comment_Text,
                Comment_Author,
                Comment_Published
                )

             VALUES (?, ?, ?, ?, ?)'''

        values = (row['Comment_Id'], row['Video_Id'],row['Comment_Text'], row['Comment_Author'], row['Comment_Published'])

        cursor.execute(insert_query, values)
        connection.commit()

                                                 # Function For Creating All Tables        
        
def tables():
    channels_table()
    playlist_table()
    videos_table()
    comments_table()
    
    return "Tables Created Successfully"

                                             # Streamlit Functions' For Show The Tables


def show_channels_table():
    ch_list = []

    DataBase = client['Youtube_data']
    collection1 = DataBase['channel_details']

    for ch_data in collection1.find({},{'_id':0,'channel_information':1}):
        ch_list.append(ch_data['channel_information'])

    DataF = st.dataframe(ch_list)
    
    return DataF

def show_playlists_table():
    pl_list = []
    DataBase = client['Youtube_data']
    collection1 = DataBase['channel_details']

    for pl_data in collection1.find({},{'_id':0,'playlist_information':1}):
        for i in range(len(pl_data['playlist_information'])):
            pl_list.append(pl_data['playlist_information'][i])
    DataF1 = st.dataframe(pl_list)
    
    return DataF1

def show_videos_table():
    vi_list = []
    DataBase = client['Youtube_data']
    collection1 = DataBase['channel_details']

    for vi_data in collection1.find({},{'_id':0,'video_information':1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])
    DataF2 = st.dataframe(vi_list)
    
    return DataF2


def show_comments_table():
    com_list = []
    DataBase = client['Youtube_data']
    collection1 = DataBase['channel_details']

    for com_data in collection1.find({},{'_id':0,'comment_information':1}):
        for i in range(len(com_data['comment_information'])):
            com_list.append(com_data['comment_information'][i])

    DataF3 = st.dataframe(com_list)
    
    return DataF3

                                            # Creating sidebars and Getting user input  

with st.sidebar:
    st.title(':blue[YOUTUBE DATA HAVERSTING AND WAREHOSING]')
    st.header('Skill Take Away')
    st.caption('Python Scripting')
    st.caption('Data Collection')
    st.caption('MongoDB')
    st.caption('API Intergration')
    st.caption('Data Managment using MongoDB and SQL')
    
channel_id = st.text_input('Enter The Channel ID')

if st.button('collect and store data'):
    ch_ids = []
    db = client ['Youtube_data']
    collection1 = db['channel_details']

    for ch_data in collection1.find({},{'_id':0,'channel_information':1}):
        ch_ids.append(ch_data['channel_information']['Channel_Id'])
        
    if channel_id in ch_ids:
        st.success("channel details of the given channel id already exists")
        
    else:
        insert = channel_details(channel_id)
        st.success(insert)
    

if st.button('Migrate to SQL'):
    Table = tables()
    st.success(Table)
    
    
show_table = st.radio('SELECT THE TABLE FOR VIEW',('CHANNELS','PLAYLISTS','VIDEOS','COMMENTS'))

if show_table == 'CHANNELS':
    show_channels_table()

elif show_table == "PLAYLISTS":
    show_playlists_table()    

elif show_table == "VIDEOS":
    show_videos_table()    

elif show_table == "COMMENTS":
    show_comments_table()    

    
                                                         # Questions
    
    
connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                            'SERVER=SMARTSATHISH;'
                            'DATABASE=youtube_data;'
                            'UID=sa;'
                            'PWD=admin9')
cursor = connection.cursor()



question = st.selectbox('Select your question',("1. What are the names of all the videos and their corresponding channels?",
                                             "2. Which channels have the most number of videos, and how many videos dothey have?",
                                             "3. What are the top 10 most viewed videos and their respective channels?",
                                             "4. How many comments were made on each video, and what are their corresponding video names?",
                                             "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                                             "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                             "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                                             "8. What are the names of all the channels that have published videos in the year 2022?",
                                             "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                             "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))


if question == "1. What are the names of all the videos and their corresponding channels?":
    query1 = "SELECT title as videos, channel_name as channelname FROM videos"  
    cursor.execute(query1)
    data = cursor.fetchall()
    data_dict = [{'video title': row[0], 'channel name': row[1]} for row in data]
    df1 = pd.DataFrame(data_dict)
    st.write(df1)
    
elif question == "2. Which channels have the most number of videos, and how many videos dothey have?":
    query2 = '''select channel_name as channelname,total_videos as no_videos from channels
                   order by total_videos desc'''
    cursor.execute(query2)
    data = cursor.fetchall()
    data_dict = [{'channel name': row[0], 'No of videos': row[1]} for row in data]
    df2 = pd.DataFrame(data_dict)
    st.write(df2)
    
elif question == "3. What are the top 10 most viewed videos and their respective channels?":
    query3 = '''select top 10 views as views, channel_name as channelname, title as videotitle from videos
            where views is not null order by views desc'''

    cursor.execute(query3)
    data = cursor.fetchall()

    if data:
        data_dict = [{'views': row[0], 'channelname': row[1], 'videotitle':row[2]} for row in data]
        df3 = pd.DataFrame(data_dict)
        st.write(df3)
    else:
        st.write("No data found for the query.")

    
elif question == "4. How many comments were made on each video, and what are their corresponding video names?":
    query4 = '''SELECT comments as no_comments,title as videotitle FROM videos WHERE comments IS NOT NULL'''
    cursor.execute(query4)
    data = cursor.fetchall()
    data_dict = [{'no of comments': row[0], 'videotitle': row[1]} for row in data]
    df4 = pd.DataFrame(data_dict)
    st.write(df4)
    

elif question == "5. Which videos have the highest number of likes, and what are their corresponding channel names?":
    query5 = '''SELECT title as videotitle, channel_name as channelname, likes as likescount
               FROM videos WHERE likes IS NOT NULL ORDER BY [likes] DESC'''
    cursor.execute(query5)
    data = cursor.fetchall()
    data_dict = [{'videotitle': row[0], 'channelname': row[1], 'likecount': row[1]} for row in data]
    df5 = pd.DataFrame(data_dict)
    st.write(df5)
    
elif question ==  "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    query6 = '''SELECT likes as likescount,title as videotitle FROM videos'''
    cursor.execute(query6)
    data = cursor.fetchall()
    data_dict = [{'likecount': row[0], 'vdeotitle': row[1]} for row in data]
    df6 = pd.DataFrame(data_dict)
    st.write(df6)

elif question == "7. What is the total number of views for each channel, and what are their corresponding channel names?":
    query7 = "SELECT channel_name as channelname, views as totalviews FROM channels"
    cursor.execute(query7)
    data = cursor.fetchall()
    data_dict = [{'channel name': row[0], 'totalviews': row[1]} for row in data]
    df7 = pd.DataFrame(data_dict)
    st.write(df7)

    
elif question == "8. What are the names of all the channels that have published videos in the year 2022?":
    query8 = '''SELECT title as video_title ,published_date as videorelease,channel_name as channelname 
            FROM videos WHERE YEAR(published_date) = 2022'''
    cursor.execute(query8)
    data = cursor.fetchall()
    data_dict = [{'videotitle': row[0], 'published_date': row[1], 'channelname': row[2]} for row in data]
    df8 = pd.DataFrame(data_dict)
    st.write(df8)

elif question =="9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    def duration_to_seconds(duration):
        parts = duration.split('PT')[-1].split('H')
        if len(parts) == 1:
            hours = 0
        else:
            hours = int(parts[0])

        parts = parts[-1].split('M')
        if len(parts) == 1:
            minutes = 0
        else:
            minutes = int(parts[0])

        parts = parts[-1].split('S')
        if len(parts) == 1:
            seconds = 0
        else:
            seconds = int(parts[0])

        return hours * 3600 + minutes * 60 + seconds

    connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                                'SERVER=SMARTSATHISH;'
                                'DATABASE=youtube_data;'
                                'UID=sa;'
                                'PWD=admin9')
    cursor = connection.cursor()
    query9 = '''SELECT channel_name as channelname, duration
                FROM videos'''

    try:
        cursor.execute(query9)
        data = cursor.fetchall()

        if data:
            result = []
            for row in data:
                channelname, duration = row
                if duration.startswith('PT'):
                    duration_seconds = duration_to_seconds(duration)
                    result.append({'channelname': channelname, 'duration_seconds': duration_seconds})

            df = pd.DataFrame(result)
            grouped_data = df.groupby('channelname')['duration_seconds'].mean().reset_index()
            grouped_data['avgduration'] = pd.to_timedelta(grouped_data['duration_seconds'], unit='s').astype(str)
            grouped_data.drop(columns=['duration_seconds'], inplace=True)
            st.write(grouped_data)

        else:
            st.write("No data found for the query.")

    except Exception as e:
        print(f"An error occurred: {e}")
        
        
elif question =="10.Which videos have the highest number of comments, and what are their corresponding channel names?":
    query10 =  '''SELECT title as videotitle, channel_name as channelname, comments as comments 
                    FROM videos 
                    WHERE comments IS NOT NULL 
                    ORDER BY comments DESC'''
    cursor.execute(query10)
    data = cursor.fetchall()
    data_dict = [{'video title': row[0], 'channel name': row[1], 'comments': row[2]} for row in data]
    df10 = pd.DataFrame(data_dict)
    st.write(df10)