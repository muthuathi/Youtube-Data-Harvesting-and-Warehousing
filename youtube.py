from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st


#API Key Connection
def Api_connect():
    api_id="AIzaSyBFrIyONc8y1-lO14WeeDkOsQXAbTmtUdo"
    api_service_name="youtube"
    api_version="V3" 

    youtube=build(api_service_name,api_version,developerKey=api_id)   

    return(youtube)

#function access
youtube=Api_connect()


#get chennal information
def get_channel_info(channel_id):
    request=youtube.channels().list(
        part="snippet,ContentDetails,statistics",
        id=channel_id
)

    response=request.execute()

    for i in response['items']:
        data=dict(Channel_Name=i['snippet']['title'],
            Channel_Id=i['id'],
            Subscribers=i['statistics']['subscriberCount'],
            Views=i['statistics']['viewCount'],
            Total_videos=i['statistics']['videoCount'],
            Description=i['snippet']['description'],
            Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads'])
    return(data)
              

#get video id
def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None
    
    while True:
        response1=youtube.playlistItems().list(part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return (video_ids)


#get video information
def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response=request.execute()

        for item in response['items']:
            data=dict(channel_name=item['snippet']['channelTitle'],
                    channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    published_Date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Favorite_Count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_Status=item['contentDetails']['caption']
            ) 
        video_data.append(data)    
    return (video_data)  

#get comment information
def get_comment_info(video_ids):

    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                Comment_data.append(data)
    except:
        pass
    return (Comment_data)
        

#connect mongodb database
client=pymongo.MongoClient('mongodb://localhost:27017')
db=client["youtube_data"]

def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    vi_ids=get_videos_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)

    coll1=db['channel_details']
    coll1.insert_one({"channel_information":ch_details,
                       "video_information":vi_details,
                       "comment_information":com_details})
    return ("upload completed successfully")



#Tables creation for channel data
def channels_table():
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="Admin",
                        database="youtube_collection",
                        port="5432")
    cursor=mydb.cursor()


    drop_query='''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()


    try:
        create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                            Channel_Id varchar(80) primary key,
                                                            Subscribers bigint,
                                                            Views bigint,
                                                            Total_videos int,
                                                            Description text,
                                                            Playlist_Id varchar(80))'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("channel table already created")




    ch_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)


    for index,row in df.iterrows():
        #print(index,":",row)
        insert_query='''insert into channels(Channel_Name,
                                            Channel_Id,
                                            Subscribers,
                                            Views,
                                            Total_videos,
                                            Description,
                                            Playlist_Id)

                                            values(%s,%s,%s,%s,%s,%s,%s)'''

        values=(row["Channel_Name"],
                row["Channel_Id"],
                row["Subscribers"],
                row["Views"],
                row["Total_videos"],
                row["Description"],
                row["Playlist_Id"])
        
        try:
            cursor.execute(insert_query,values)
            mydb.commit()

        except:
            print("Channels values are already inserted")


#Tables creation for videos data

def videos_table():
    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="Admin",
                            database="youtube_collection",
                            port = "5432"
                                )
    cursor = mydb.cursor()

    drop_query = '''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query = '''create table if not exists videos(channel_Name varchar(100),
                                                        channel_Id varchar(100),
                                                        Video_Id varchar(30) primary key,
                                                        Title varchar(150),
                                                        Tags text,
                                                        Thumbnail varchar(200),
                                                        Description text,
                                                        published_Date timestamp,
                                                        Duration interval,
                                                        Views bigint,
                                                        Likes bigint,
                                                        Comments int,
                                                        Favorite_Count int,
                                                        Definition varchar(10),
                                                        Caption_Status varchar(50)
                                                        )'''
    cursor.execute(create_query)
    mydb.commit()


    vi_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            #print(vi_data["video_information"][i])
            vi_list.append(vi_data["video_information"][i])
    df2 = pd.DataFrame(vi_list)


    for index, row in df2.iterrows():
        #print(index,":",row)
        insert_query = '''insert into videos (Channel_Name,
                                            Channel_Id,
                                            Video_Id, 
                                            Title, 
                                            Tags,
                                            Thumbnail,
                                            Description, 
                                            Published_Date,
                                            Duration, 
                                            Views, 
                                            Likes,
                                            Comments,
                                            Favorite_Count, 
                                            Definition, 
                                            Caption_Status)
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) '''

        values = (
                    row['channel_name'],
                    row['channel_Id'],
                    row['Video_Id'],
                    row['Title'],
                    row['Tags'],
                    row['Thumbnail'],
                    row['Description'],
                    row['published_Date'],
                    row['Duration'],
                    row['Views'],
                    row['Likes'],
                    row['Comments'],
                    row['Favorite_Count'],
                    row['Definition'],
                    row['Caption_Status'])
                                
        try:    
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            print("videos values already inserted in the table")

#Tables creation for comments data

def comments_table():
    mydb = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="Admin",
                        database="youtube_collection",
                        port = "5432"
                            )
    cursor = mydb.cursor()

    drop_query = '''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query = '''create table if not exists comments(comment_Id varchar(100) primary key,
                                                        Video_id varchar(50),
                                                        Comment_Text text,
                                                        Comment_Author varchar(150),
                                                        Comment_Published timestamp
                                                
                                                                                    )'''
    cursor.execute(create_query)
    mydb.commit()


    com_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3 = pd.DataFrame(com_list)



    for index, row in df3.iterrows():
        #print(index,":",row)
        insert_query = '''insert into comments(comment_Id,
                                                Video_id, 
                                                Comment_Text, 
                                                Comment_Author,
                                                Comment_Published
                                            )
                                            
                                        VALUES (%s, %s, %s, %s, %s) '''

        values = (
                    row['comment_Id'],
                    row['Video_id'],
                    row['Comment_Text'],
                    row['Comment_Author'],
                    row['Comment_Published']
                )
        try:
            cursor.execute(insert_query,values)
            mydb.commit() 
        except:
            print("comments values already inserted in the table")   


def tables():
    channels_table()
    videos_table()
    comments_table()

    return "Tables Created Successfully"


#streamlit code

def show_channels_table():
    ch_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)

    return df


def show_videos_table():
    vi_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2 = st.dataframe(vi_list)

    return df2


def show_comments_table():
    com_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3 = st.dataframe(com_list)

    return df3


#streamlit sidebar menu

with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("SKILL TAKE AWAY")
    st.caption('Python scripting')
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption(" Data Managment using MongoDB and SQL")
    
channel_id = st.text_input("Enter the Channel id")
channels = channel_id.split(',')
channels = [ch.strip() for ch in channels if ch]

#store the data in mongodb

if st.button("Collect and Store data"):
    for channel in channels:
        ch_ids = []
        db = client["youtube_data"]
        coll1 = db["channel_details"]
        for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
            ch_ids.append(ch_data["channel_information"]["Channel_Id"])
        if channel in ch_ids:
            st.success("Channel details of the given channel id: " + channel + " already exists")
        else:
            output = channel_details(channel)
            st.success(output)
            
if st.button("Migrate to SQL"):
    display = tables()
    st.success(display)
    
show_table = st.radio("SELECT THE TABLE FOR VIEW",(":green[CHANNELS]",":red[VIDEOS]",":blue[COMMENTS]"))

if show_table == ":green[CHANNELS]":
    show_channels_table()

elif show_table ==":red[VIDEOS]":
    show_videos_table()

elif show_table == ":blue[COMMENTS]":
    show_comments_table()

#SQL connection
mydb = psycopg2.connect(host="localhost",
            user="postgres",
            password="Admin",
            database= "youtube_collection",
            port = "5432"
            )
cursor = mydb.cursor()
    
question = st.selectbox(
    'Please Select Your Question',
    ('1. What are the names of all the videos and their corresponding channels?',
     '2. Which channels have the most number of videos, and how many videos do they have?',
     '3. What are the top 10 most viewed videos and their respective channels?',
     '4. How many comments were made on each video, and what are their corresponding video names?',
     '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
     '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
     '7. What is the total number of views for each channel, and what are their corresponding channel names?',
     '8. What are the names of all the channels that have published videos in the year 2022?',
     '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
     '10. Which videos have the highest number of comments, and what are their corresponding channel names?'))



if question == '1. What are the names of all the videos and their corresponding channels?':
    query1 = "select Title as videos, Channel_Name as ChannelName from videos;"
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    st.write(pd.DataFrame(t1, columns=["Video Title","Channel Name"]))

elif question == '2. Which channels have the most number of videos, and how many videos do they have?':
    query2 = "select Channel_Name as ChannelName,Total_Videos as NO_Videos from channels order by Total_Videos desc;"
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    st.write(pd.DataFrame(t2, columns=["Channel Name","No Of Videos"]))

elif question == '3. What are the top 10 most viewed videos and their respective channels?':
    query3 = '''select Views as views , Channel_Name as ChannelName,Title as VideoTitle from videos 
                        where Views is not null order by Views desc limit 10;'''
    cursor.execute(query3)
    mydb.commit()
    t3 = cursor.fetchall()
    st.write(pd.DataFrame(t3, columns = ["views","channel Name","video title"]))

elif question == '4. How many comments were made on each video, and what are their corresponding video names?':
    query4 = "select Comments as No_comments ,Title as VideoTitle from videos where Comments is not null;"
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    st.write(pd.DataFrame(t4, columns=["No Of Comments", "Video Title"]))

elif question == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
    query5 = '''select Title as VideoTitle, Channel_Name as ChannelName, Likes as LikesCount from videos 
                       where Likes is not null order by Likes desc;'''
    cursor.execute(query5)
    mydb.commit()
    t5 = cursor.fetchall()
    st.write(pd.DataFrame(t5, columns=["video Title","channel Name","like count"]))

elif question == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
    query6 = '''SELECT v1.Title AS VideoTitle,
               SUM(v1.Likes) AS TotalLikes,
               SUM(v2.Likes) AS TotalDislikes
        FROM videos v1
        JOIN videos v2 ON v1.video_id = v1.video_id
        GROUP BY v1.Title;'''
    cursor.execute(query6)
    mydb.commit()
    t6 = cursor.fetchall()
    st.write(pd.DataFrame(t6, columns=["Video Title" ,"Total Likes", "Total Dislikes"]))

elif question == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
    query7 = "select Channel_Name as ChannelName, Views as Channelviews from channels;"
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    st.write(pd.DataFrame(t7, columns=["channel name","total views"]))

elif question == '8. What are the names of all the channels that have published videos in the year 2022?':
    query8 = '''select Title as Video_Title, Published_Date as VideoRelease, Channel_Name as ChannelName from videos 
                where extract(year from Published_Date) = 2022;'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    st.write(pd.DataFrame(t8,columns=["Name", "Video Publised On", "ChannelName"]))



elif question == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    query9 =  "SELECT Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM videos GROUP BY Channel_Name;"
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    t9 = pd.DataFrame(t9, columns=['ChannelTitle', 'Average Duration'])
    T9=[]
    
    for index, row in t9.iterrows():
        channel_title = row['ChannelTitle']
        average_duration = row['Average Duration']
        average_duration_str = str(average_duration)
        T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
    st.write(pd.DataFrame(T9))
        

elif question == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
    query10 = '''select Title as VideoTitle, Channel_Name as ChannelName, Comments as Comments from videos 
                       where Comments is not null order by Comments desc;'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    st.write(pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'NO Of Comments']))