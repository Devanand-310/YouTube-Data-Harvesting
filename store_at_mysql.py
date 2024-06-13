from datetime import datetime
import mysql.connector


class MySQLDatabase:
    def __init__(self, host_name, user_name, user_password):
        self.host_name = host_name
        self.user_name = user_name
        self.user_password = user_password
        self.connection = None
        self.cursor = None

    def connect(self):
        self.connection = mysql.connector.connect(
            host=self.host_name,
            user=self.user_name,
            password=self.user_password
        )
        if self.connection.is_connected():
            self.cursor = self.connection.cursor()

    def create_db(self, db_name):
        try:
            self.cursor.execute(f'CREATE DATABASE {db_name}')
        except:
            return f'{db_name} exists'

    def use_db(self, db_name):
        self.cursor.execute(f'USE {db_name}')

    def create_table_ch(self):
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS Channel_Details (
            Channel_Name VARCHAR(255),
            Channel_Id VARCHAR(255),
            Video_Count INT,
            Subscriber_Count INT,
            Channel_Views INT,
            Channel_Description TEXT,
            Playlist_Id VARCHAR(255)
        )""")
        
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS Playlist_Details (
            Channel_Id VARCHAR(255),
            Playlist_Id VARCHAR(255)
        )""")
        
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS Video_Details (
            Playlist_Id VARCHAR(255),
            Video_Id VARCHAR(255),
            Video_Name VARCHAR(255),
            Video_Description TEXT,
            Tags VARCHAR(255),
            PublishedAt DATETIME,
            View_Count INT,
            Like_Count INT,
            Dislike_Count INT,
            Favorite_Count INT,
            Comment_Count INT,
            Duration INT,
            Thumbnail VARCHAR(255),
            Caption_Status VARCHAR(255)
        )""")
        
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS Comment_Details (
            Video_Id VARCHAR(255),
            Comment_Id VARCHAR(255),
            Comment_Text TEXT,
            Comment_Author VARCHAR(255),
            Comment_PublishedAt VARCHAR(255)
        )""")

    def insert_val_ch(self, doc):
        channel_details = doc['channel_data']['Channel_Details']
        
        self.cursor.execute("""
        INSERT INTO Channel_Details (Channel_Name, Channel_Id, Video_Count, Subscriber_Count, Channel_Views, Channel_Description, Playlist_Id)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (channel_details['Channel_Name'], channel_details['Channel_Id'], channel_details['Video_Count'],
              channel_details['Subscriber_Count'], channel_details['Channel_Views'], channel_details['Channel_Description'], channel_details['Playlist_Id']))
        self.connection.commit()

    def insert_val_pl(self, doc):
        channel_details = doc['channel_data']['Channel_Details']
        
        self.cursor.execute("""
            INSERT INTO Playlist_Details (Channel_Id, Playlist_Id) VALUES (%s, %s)
            """, (channel_details['Channel_Id'], channel_details['Playlist_Id']))
        self.connection.commit()

    def insert_val_video_details(self, doc,video_ids):
        channel_details = doc['channel_data']['Channel_Details']
        playlist_id = channel_details['Playlist_Id']

        for i in range(int(channel_details['Video_Count'])):
                    video_details = doc['channel_data'][f'Video_Id_{i+1}']
                    video_id = video_details['Video_Id']
                    video_name = video_details['Video_Name']
                    video_description = video_details['Video_Description']
                    tags = ','.join(video_details.get('Tags', []))
                    tags = tags[:255]
                    published_at_str = video_details['PublishedAt']
                    published_at = datetime.strptime(published_at_str, '%B %d, %Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
                    view_count = video_details['View_Count']
                    like_count = video_details['Like_Count']
                    dislike_count = video_details['Dislike_Count']
                    favorite_count = video_details['Favorite_Count']
                    comment_count = video_details['Comment_Count']
                    duration_str = video_details['Duration']
                    hours, minutes, seconds = map(int, duration_str.split(':'))
                    duration = hours * 3600 + minutes * 60 + seconds  # Convert to total seconds
                    thumbnail = video_details['Thumbnail']
                    caption_status = video_details['Caption_Status']

                    self.cursor.execute("""
                    INSERT INTO Video_Details (Playlist_Id, Video_Id, Video_Name, Video_Description, Tags, PublishedAt, View_Count, Like_Count, Dislike_Count, Favorite_Count, Comment_Count, Duration, Thumbnail, Caption_Status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (playlist_id, video_id, video_name, video_description, tags, published_at, view_count, like_count,
                          dislike_count, favorite_count, comment_count, duration, thumbnail, caption_status))
        self.connection.commit()

    def insert_val_comments(self, doc):
        channel_details = doc['channel_data']['Channel_Details']

        for i in range(int(channel_details['Video_Count'])):
            comment_details = doc['channel_data'][f'Video_Id_{i+1}']['Comments']

            if comment_details == 'Unavailable':

                video_id = doc['channel_data'][f'Video_Id_{i+1}']['Video_Id']
                comment_id = "Unavailable"
                comment_text = "Unavailable"
                comment_author = "Unavailable"
                comment_published_at = "Unavailable"


            else:
                for comment in comment_details:
                    video_id = doc['channel_data'][f'Video_Id_{i+1}']['Video_Id']
                    comment_id = comment['Comment_Id']
                    comment_text = comment['Comment_Text']
                    comment_author = comment['Comment_Author']
                    comment_published_at_str = comment['Comment_PublishedAt']
                    comment_published_at = datetime.strptime(comment_published_at_str, '%B %d, %Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
                    self.cursor.execute("""
                    INSERT INTO Comment_Details (Video_Id, Comment_Id, Comment_Text, Comment_Author, Comment_PublishedAt) VALUES (%s, %s, %s, %s, %s)
                    """, (video_id, comment_id, comment_text, comment_author, comment_published_at))
            self.connection.commit()

    def close(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print('Database connection closed.')
            


