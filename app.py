import streamlit as st
import re
from googleapiclient.discovery import build
from data_extraction import (
    get_channel_data,
    get_video_ids,
    get_video_data,
    date_time_converion,
    get_video_comments,
    output
)
from store_at_mongodb import MongoDB_handler
from store_at_mysql import MySQLDatabase
from mysql_connection import my_Sql
import pandas as pd
import plotly.express as px


api_service_name = 'youtube'
api_version = 'v3'
api_key = 'AIzaSyBd5o0mrDLCyhyHVdXWlP-gTgvh77kGbMU'
youtube = build(api_service_name, api_version, developerKey=api_key)

st.set_page_config(layout="wide")
st.markdown(
    """
    <h1 style='text-align: left; color: green;'>
        YouTube Data Harvesting
    </h1>
    """,
    unsafe_allow_html=True
)
def is_valid_channel_id(channel_id):
    pattern = r'^UC[a-zA-Z0-9_-]{22}$'
    return re.match(pattern, channel_id)

col1, col2 = st.columns(2)

with col1:
    st.title('YouTube Channel Data Retrieval')

    input_value = st.text_input('Enter Channel_Id')
    if st.button('Retrieve Data'):
        if not input_value:
            st.error('Input cannot be empty.')
        elif not is_valid_channel_id(input_value):
            st.error('Invalid YouTube Channel ID format. Please enter a valid ID.')
        else:
            try:
                out = output(youtube, input_value)
                st.session_state.out = out
                st.json(st.session_state.out['channel_data']['Channel_Details'])
            except HTTPError as http_err:
                st.error('HTTP error occurred, Your API request exceed for a day ',icon="🚨")
            except Exception as e:
                st.error(f'An error occurred: {e}')

with col2:
    st.title('Data Migrate to MongoDB')
    url = 'mongodb+srv://USERNAME:PASSWORD@cluster0.4xtn4hs.mongodb.net/'
    database_name = 'Youtube_DB'
    col_name = 'Youtube_data'
    mongo = MongoDB_handler(url, database_name, col_name)

    if st.button('Migrate to MongoDB'):
        if 'out' in st.session_state and st.session_state.out:
            mongo.insert_doc(st.session_state.out)
            st.success("Data successfully migrated to MongoDB")
        else:
            st.error("No data to migrate. Please retrieve data first.")

docs = mongo.get_documents()   

col3, col4 = st.columns(2)

with col3:
    st.title('Data transformed and store at MySQL')
    db = MySQLDatabase("localhost", "root", "Janu19042002")

    if st.button('Transfer to MySQL'):
        db.connect()
        db.create_db('Youtube_db')
        db.use_db('Youtube_db')
        db.create_table_ch()
        for doc in docs:
            db.insert_val_ch(doc)
            db.insert_val_pl(doc)
            playlist_id = doc['channel_data']['Channel_Details'].get('Playlist_Id')
            if playlist_id:
                video_ids = get_video_ids(youtube, playlist_id)
                db.insert_val_video_details(doc, video_ids)
            db.insert_val_comments(doc)
        st.success("Data successfully transferred to MySQL")

with col4:
    connection, cursor = my_Sql('localhost', 'root', 'Janu19042002', 'Youtube_db') 
    questions = st.selectbox('**Select your Question**',
                              ['1. What are the names of all the videos and their corresponding channels?',
                               '2. Which channels have the most number of videos, and how many videos do they have?',
                               '3. What are the top 10 most viewed videos and their respective channels?',
                               '4. How many comments were made on each video, and what are their corresponding video names?',
                               '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                               '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                               '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                               '8. What are the names of all the channels that have published videos in the year 2022?',
                               '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                               '10. Which videos have the highest number of comments, and what are their corresponding channel names?'],
                              key='collection_question',help='Select Desired Option')
    if  questions == '1. What are the names of all the videos and their corresponding channels?':

        cursor.execute("""SELECT channel_details.Channel_Name, video_details.Video_Name 
        FROM channel_details JOIN playlist_details JOIN video_details
        ON channel_details.Channel_Id = playlist_details.Channel_Id AND playlist_details.Playlist_Id = video_details.Playlist_Id;
        """)
        result = cursor.fetchall()
        question_1 = pd.DataFrame(result,columns=['Channel_Name','Video_Name'])
        st.dataframe(question_1)

    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':

        col1,col2 = st.columns(2)
        with col1:
            cursor.execute("""SELECT Channel_Name, Video_Count FROM channel_details ORDER BY Video_Count DESC """)
            result = cursor.fetchall()
            question_2 = pd.DataFrame(result, columns=['Channel_Name', 'Video_Count'])
            st.dataframe(question_2)

        with col2:
            fig = px.bar(question_2, x='Channel_Name', y='Video_Count',
             labels={'Video_Count':'Video Count'},
             title='Video Count by Channel',
             color='Channel_Name')
            fig.update_layout(xaxis_tickangle=-45)  
            st.plotly_chart(fig, use_container_width=True)

    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        
        col1,col2 = st.columns(2)
        with col1:
            cursor.execute("""SELECT channel_details.Channel_Name, video_details.Video_Name, video_details.View_Count
            FROM channel_details JOIN video_details ON channel_details.Playlist_Id =  video_details.Playlist_Id 
            ORDER BY video_details.View_Count DESC LIMIT 10;""")
            result = cursor.fetchall()
            question_3 = pd.DataFrame(result, columns=['Channel_Name','Video_Name','View_Count'])
            st.dataframe(question_3)

        with col2:
            fig = px.bar(question_3, x='Video_Name', y='View_Count',
             labels={'View_Count':'View Count'},
             title='Top 10 Videos by View Count',
             color='Channel_Name')
            fig.update_layout(xaxis_tickangle=-45)  
            st.plotly_chart(fig, use_container_width=True)

    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':

        cursor.execute("""SELECT video_details.Video_Name, video_details.Comment_Count FROM video_details""")
        result = cursor.fetchall()
        question_4 = pd.DataFrame(result, columns=['Video_Name','Comment_Count'])
        st.dataframe(question_4)  

    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':

        cursor.execute("""SELECT channel_details.Channel_Name, video_details.Video_Name, video_details.Like_Count
        FROM channel_details JOIN video_details ON channel_details.Playlist_Id = video_details.Playlist_Id
        ORDER BY video_details.Like_Count DESC LIMIT 10""")
        result = cursor.fetchall()
        question_5 = pd.DataFrame(result, columns=['Channel_Name','Video_Name', 'Like_Count'])
        st.dataframe(question_5)

    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':

        cursor.execute("""SELECT channel_details.Channel_Name, video_details.Video_Name, video_details.Like_Count, video_details.Dislike_Count
        FROM channel_details JOIN video_details ON channel_details.Playlist_Id = video_details.Playlist_Id""")
        result = cursor.fetchall()
        question_6 = pd.DataFrame(result, columns=['Channel_Name','Video_Name', 'Like_Count','Dislike_Count'])
        st.dataframe(question_6)

    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        col1,col2 = st.columns(2)

        with col1:
            cursor.execute("""SELECT Channel_Name, Channel_Views From channel_details""")
            result = cursor.fetchall()
            question_7 = pd.DataFrame(result, columns=['Channel_Name','Channel_Views'])
            st.dataframe(question_7)

        with col2:
            fig = px.bar(question_7, x='Channel_Name', y='Channel_Views', 
                labels={'Channel_Views':'Channel Views'}, 
                title='Channel Views', 
                color='Channel_Name')
            fig.update_layout(xaxis_tickangle=-45)  
            st.plotly_chart(fig, use_container_width=True)

    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':

        cursor.execute("""SELECT DISTINCT Channel_Name
        FROM channel_details cd
        JOIN video_details vd  ON cd.Playlist_Id = vd.Playlist_Id
        WHERE YEAR(vd.PublishedAt) = 2022;
        """)
        result = cursor.fetchall()
        question_8 = pd.DataFrame(result,columns= ['Channel_Name'])
        st.dataframe(question_8)

    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':

        cursor.execute("""SELECT DISTINCT channel_details.Channel_Name, ROUND(AVG(video_details.Duration)DIV 60)
        FROM channel_details JOIN video_details ON channel_details.Playlist_Id = video_details.Playlist_Id
        GROUP BY channel_details.Channel_Name
        """)
        result = cursor.fetchall()
        question_9 = pd.DataFrame(result,columns= ['Channel_Name','Average_Duration_Minutes'])
        st.dataframe(question_9)

    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':

        cursor.execute("""SELECT channel_details.Channel_Name,video_details.Comment_Count
        FROM channel_details JOIN video_details ON channel_details.Playlist_Id = video_details.Playlist_Id
        ORDER BY video_details.Comment_Count DESC LIMIT 1""")
        result = cursor.fetchall()
        question_10 = pd.DataFrame(result, columns=['Channel_Name','Highest_Comments'])
        st.dataframe(question_10)