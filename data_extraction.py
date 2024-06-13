import re
from datetime import datetime
from googleapiclient.discovery import build

api_service_name = 'youtube'
api_version = 'v3'
api_key = 'AIzaSyBd5o0mrDLCyhyHVdXWlP-gTgvh77kGbMU'
youtube = build(api_service_name, api_version, developerKey=api_key)

def get_channel_data(youtube, channel_id):
    channel_request = youtube.channels().list(
        part='snippet,statistics,contentDetails',
        id=channel_id)
    channel_response = channel_request.execute()
    return channel_response



def get_video_ids(youtube, channel_playlist_id):

    video_id = []
    next_page_token = None
    while True:
 
        request = youtube.playlistItems().list(
            part='contentDetails',
            playlistId=channel_playlist_id,
            maxResults=50,
            pageToken=next_page_token)
        response = request.execute()


        for item in response['items']:
            video_id.append(item['contentDetails']['videoId'])


        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break

    return video_id



def get_video_data(youtube, video_ids):
    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(
            part='snippet,statistics,contentDetails',
            id=video_id)
        response = request.execute()
        video = response['items'][0]
        video['comment_threads'] = get_video_comments(youtube, video_id) 
        duration = video.get('contentDetails', {}).get('duration', 'Not Available')
        if duration != 'Not Available':
            duration = convert_duration(duration)
        video['contentDetails']['duration'] = duration
        video_data.append(video)
    return video_data


def get_video_comments(youtube, video_id, max_comments=50):
    comments = []
    next_page_token = None
    
    while True:
        request = youtube.commentThreads().list(
            part='snippet',
            maxResults=max_comments,  # Max allowed by the API per request
            textFormat="plainText",
            videoId=video_id,
            pageToken=next_page_token
        )
        response = request.execute()
        
        for item in response['items']:
            comment = item['snippet']['topLevelComment']['snippet']
            comment_id = item['id']
            comment_text = comment['textDisplay']
            comment_author = comment['authorDisplayName']
            comment_published_at = comment['publishedAt']
            comments.append({
                'Comment_Id': comment_id,
                'Comment_Text': comment_text,
                'Comment_Author': comment_author,
                'Comment_PublishedAt': date_time_converion(comment_published_at)
            })
        
        next_page_token = response.get('nextPageToken')
        if not next_page_token or (max_comments and len(comments) >= max_comments):
            break
    
    if max_comments:
        comments = comments[:max_comments]
    
    return comments if comments else "Unavailable"


# Define a function to convert duration
def convert_duration(duration):
    regex = r'PT(\d+H)?(\d+M)?(\d+S)?'
    match = re.match(regex, duration)
    if not match:
        return '00:00:00'
    hours, minutes, seconds = match.groups()
    hours = int(hours[:-1]) if hours else 0
    minutes = int(minutes[:-1]) if minutes else 0
    seconds = int(seconds[:-1]) if seconds else 0
    total_seconds = hours * 3600 + minutes * 60 + seconds
    return '{:02d}:{:02d}:{:02d}'.format(int(total_seconds / 3600), int((total_seconds % 3600) / 60),
                                            int(total_seconds % 60))

def date_time_converion(s):
    datetime_obj = datetime.strptime(s,"%Y-%m-%dT%H:%M:%SZ")
    new_datetime_obj = datetime_obj.strftime("%B %d, %Y %H:%M:%S")
    return new_datetime_obj


def output(youtube, input_value):
    channel_data = get_channel_data(youtube, input_value)


    channel_name = channel_data['items'][0]['snippet']['title']
    channel_video_count = channel_data['items'][0]['statistics']['videoCount']
    channel_subscriber_count = channel_data['items'][0]['statistics']['subscriberCount']
    channel_view_count = channel_data['items'][0]['statistics']['viewCount']
    channel_description = channel_data['items'][0]['snippet']['description']
    channel_playlist_id = channel_data['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    channel = {
        "Channel_Details": {
            "Channel_Name": channel_name,
            "Channel_Id": input_value,
            "Video_Count": int(channel_video_count),
            "Subscriber_Count": int(channel_subscriber_count),
            "Channel_Views": int(channel_view_count),
            "Channel_Description": channel_description,
            "Playlist_Id": channel_playlist_id
        }
    }

    video_ids = get_video_ids(youtube, channel_playlist_id)    
    video_data = get_video_data(youtube, video_ids)

    videos = {}

    for i, video in enumerate(video_data):
        video_id = video['id']
        video_name = video['snippet']['title']
        video_description = video['snippet']['description']
        tags = video['snippet'].get('tags', [])
        published_at = video['snippet']['publishedAt']
        view_count = video['statistics']['viewCount']
        like_count = video['statistics'].get('likeCount', 0)
        dislike_count = video['statistics'].get('dislikeCount', 0)
        favorite_count = video['statistics'].get('favoriteCount', 0)
        comment_count = video['statistics'].get('commentCount', 0)
        duration = video.get('contentDetails', {}).get('duration', 'Not Available')
        thumbnail = video['snippet']['thumbnails']['high']['url']
        caption_status = video.get('contentDetails', {}).get('caption', 'Not Available')
        

        videos[f"Video_Id_{i + 1}"] = {
            'Video_Id': video_id,
            'Video_Name': video_name,
            'Video_Description': video_description,
            'Tags': tags,
            'PublishedAt': date_time_converion(published_at),
            'View_Count': int(view_count),
            'Like_Count': int(like_count),
            'Dislike_Count': int(dislike_count),
            'Favorite_Count': int(favorite_count),
            'Comment_Count': int(comment_count),
            'Duration': duration,
            'Thumbnail': thumbnail,
            'Caption_Status': caption_status,
            'Comments': get_video_comments(youtube,video_id)
        }    

    final_output = {**channel, **videos}
    final_output_data ={
        'channel_name':channel['Channel_Details']['Channel_Name'],
        'channel_data':final_output
        }
    return final_output_data    