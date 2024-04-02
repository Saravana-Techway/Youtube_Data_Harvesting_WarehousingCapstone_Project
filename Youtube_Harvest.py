import pandas as pd
import isodate
import datetime
from googleapiclient.discovery import build


class Youtube_Harvest:
    def __init__(self):
        self.api_service_name = "youtube"
        self.api_version = "v3"
        self.api_key = ""
        self.youtube = build(self.api_service_name,self.api_version,developerKey=self.api_key)

    def channel_details(self, channel_name):
        # Search for the channel
        request = self.youtube.search().list(
            part="snippet",
            maxResults=25,
            q=channel_name
        )
        response = request.execute()

        # Extract channel ID from the search result
        channel_id = response['items'][0]['snippet']['channelId']

        # Retrieve detailed information about the channel
        request1 = self.youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
        )
        response1 = request1.execute()

        # Extract relevant channel details
        Channel_ID = response1['items'][0]['id']
        Channel_Name = response1['items'][0]['snippet']['title']
        Channel_Description = response1['items'][0]['snippet']['description']
        Channel_Published_Date = response1['items'][0]['snippet']['publishedAt']
        Channel_Subscription_Count = response1['items'][0]['statistics']['subscriberCount']
        Channel_Video_Count = response1['items'][0]['statistics']['videoCount']
        Channel_View_Count = response1['items'][0]['statistics']['viewCount']
        Channel_Playlist_ID = response1['items'][0]['contentDetails']['relatedPlaylists']['uploads']

        return {'Channel_ID': Channel_ID, 'Channel_Name': Channel_Name, 'Channel_Description': Channel_Description,
                'Channel_Published_Date': Channel_Published_Date,
                'Channel_Subscription_Count': Channel_Subscription_Count,
                'Channel_Video_Count': Channel_Video_Count, 'Channel_view_count': Channel_View_Count,
                'Channel_Playlist_ID': Channel_Playlist_ID}

    def video_id_info(self, play_list_id):
        video_info = []
        # Retrieve video IDs from the playlist
        request = self.youtube.playlistItems().list(
            part="snippet,contentDetails",
            maxResults=50,
            playlistId=play_list_id
        )
        response = request.execute()

        for item in response['items']:
            video_info.append(item['contentDetails']['videoId'])

        # Check for additional pages of results
        next_page_flag = response.get('nextPageToken')
        while next_page_flag is not None:
            request = self.youtube.playlistItems().list(
                part="snippet,contentDetails",
                maxResults=50,
                playlistId=play_list_id,
                pageToken=next_page_flag
            )
            response = request.execute()

            for item in response['items']:
                video_info.append(item['contentDetails']['videoId'])

            next_page_flag = response.get('nextPageToken')
        return video_info

    def video_details_info(self, video_id):
        video_info_details = []

        for vid in video_id:
            try:
                # Retrieve detailed information about each video
                request = self.youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=vid
                )
                response = request.execute()

                # Extract relevant video details
                Video_ID = response['items'][0]['id']
                Channel_ID = response['items'][0]['snippet']['channelId']
                Video_Description = response['items'][0]['snippet']['description']
                Video_Title = response['items'][0]['snippet']['title']
                Video_Duration_ISO = response['items'][0]['contentDetails']['duration']

                # Convert ISO 8601 duration to time format (HH:MM:SS)
                duration_seconds = isodate.parse_duration(Video_Duration_ISO).total_seconds()
                Video_Duration = str(datetime.timedelta(seconds=duration_seconds))

                Video_Published_Date = response['items'][0]['snippet']['publishedAt']
                Video_Comment_Count = response['items'][0]['statistics']['commentCount']
                Video_Like_Count = response['items'][0]['statistics']['likeCount']
                Video_View_Count = response['items'][0]['statistics']['viewCount']

                video_info_details.append({'Video_ID': Video_ID, 'Channel_ID': Channel_ID,
                                           'Video_Description': Video_Description, 'Video_Title': Video_Title,
                                           'Video_Duration': Video_Duration,
                                           'Video_Published_Date': Video_Published_Date,
                                           'Video_Comment_Count': Video_Comment_Count,
                                           'Video_Like_Count': Video_Like_Count,
                                           'Video_View_Count': Video_View_Count})

            except Exception as E:
                continue

        return video_info_details

    def comment_details_info(self, video_id):
        comment_info_details = []

        for cid in video_id:
            try:
                # Retrieve comments for each video
                request = self.youtube.commentThreads().list(
                    part="snippet,replies",
                    maxResults=100,
                    videoId=cid
                )
                response = request.execute()

                for item in response['items']:
                    # Extract relevant comment details
                    Comment_ID = item['id']
                    Video_ID = cid
                    Video_Comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
                    Video_Comment_Author = item['snippet']['topLevelComment']['snippet']['authorDisplayName']
                    Comment_Published_Date = item['snippet']['topLevelComment']['snippet']['publishedAt']

                    comment_info_details.append({'Comment_ID': Comment_ID, 'Video_ID': Video_ID,
                                                 'Video_Comment': Video_Comment,
                                                 'Video_Comment_Author': Video_Comment_Author,
                                                 'Comment_Published_Date': Comment_Published_Date})

            except Exception as E:
                continue

        return comment_info_details
