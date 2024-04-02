# Import necessary libraries
from Youtube_Harvest import Youtube_Harvest
from DB_Management import Database_Management
import pandas as pd
import streamlit as st


# Set page title
st.set_page_config(page_title="Youtube Data Harvesting & Warehousing")

# Sidebar
# List of tuples containing query number and description
queries = [
    (1, "What are the names of all the videos and their corresponding channels?"),
    (2, "Which channels have the most number of videos, and how many videos do they have?"),
    (3, "What are the top 10 most viewed videos and their respective channels?"),
    (4, "How many comments were made on each video, and what are their corresponding video names?"),
    (5, "Which videos have the highest number of likes, and what are their corresponding channel names?"),
    (6, "What is the total number of likes and dislikes for each video, and what are their corresponding video names?"),
    (7, "What is the total number of views for each channel, and what are their corresponding channel names?"),
    (8, "What are the names of all the channels that have published videos in the year 2022?"),
    (9, "What is the average duration of all videos in each channel, and what are their corresponding channel names?"),
    (10, "Which videos have the highest number of comments, and what are their corresponding channel names?")
]
st.sidebar.title("Queries")

# Display the list of query descriptions in the sidebar
selected_query_description = st.sidebar.selectbox("Select a query", [desc for _, desc in queries])

# Get the selected query number based on the selected description
selected_query_number = [num for num, desc in queries if desc == selected_query_description][0]


# Main title and logo
st.markdown(
    """
    <style>
        .title-container {
            display: flex;
            align-items: center;
            justify-content: center;
            margin-bottom: 20px; /* Adjust as needed */
        }
        .logo {
            height: 40px;
            margin-right: 10px; /* Adjust as needed */
        }
        .title {
            margin: 0;
            padding: 0;
            border-bottom: 4px solid red;
        }
    </style>
    <div class="title-container">
        <img class="logo" src='https://upload.wikimedia.org/wikipedia/commons/thumb/4/42/YouTube_icon_%282013-2017%29.png/240px-YouTube_icon_%282013-2017%29.png'></img>
        <h1 class="title">Data Harvesting & Warehousing</h1>
    </div>
    """,
    unsafe_allow_html=True
)

# Add content to the main area
st.write("Enter the YouTube channel name:")

# Get user input for channel name
channel_name = st.text_input("")

# Create an instance of Database_Management
db_manage = Database_Management()

# Create the database and tables if they don't exist
db_manage.create_database()
db_manage.table_creation()

# Create an instance of Youtube
YB_Harvest = Youtube_Harvest()

#Extract the channel information by calling the channel_details function
channel_info=YB_Harvest.channel_details(channel_name)
df_Channel_Info=pd.DataFrame.from_dict(channel_info,orient='index')
df_Channel_Info=df_Channel_Info.transpose()
table_name="Channel_Details"

#Checking that user input youtube channel already exist in backend table or not. If that youtube channel not exist then only the entire program will get proceed
chk_channel_id=df_Channel_Info['Channel_ID'].iloc[0]
channel_check_query = f"select Channel_ID from Channel_Details where lower(Channel_ID) = lower('{chk_channel_id}')"
Check_Channel_ID=db_manage.Check_Channel_Info(channel_check_query)

if Check_Channel_ID == chk_channel_id:
    st.warning(f"The YouTube channel '{channel_name}' already exists in our database.")
else:
    st.write("Channel Info:")
    st.write(df_Channel_Info)
    if st.button("Push Data to Database"):

        db_manage.df_to_sql(df_Channel_Info,table_name)

        play_list_id = df_Channel_Info['Channel_Playlist_ID'].iloc[0]
        video_id = YB_Harvest.video_id_info(play_list_id)

        video_details = YB_Harvest.video_details_info(video_id)
        df_Video_Details_Info = pd.DataFrame(video_details)
        video_list_id = df_Video_Details_Info['Video_ID'].tolist()
        table_name="Video_Details"

        db_manage.df_to_sql(df_Video_Details_Info, table_name)

        comment_details = YB_Harvest.comment_details_info(video_list_id)
        df_Comment_Details_Info = pd.DataFrame(comment_details)
        table_name = "Comment_Details"

        db_manage.df_to_sql(df_Comment_Details_Info, table_name)
        st.success("Data successfully pushed to the database.")

#This function created to display the result based on the queries which have been selected by the users
def execute_query_and_display_result(query_number):
    if query_number == 1:
        prj_query1 = "select Video_Title, Channel_Name from Video_Details a, Channel_Details b where a.Channel_ID=b.Channel_ID;"
        query_result = db_manage.Query_Output(prj_query1)
        if query_result:
            df = pd.DataFrame(query_result, columns=['Video_Title', 'Channel_Name'])

            # Convert DataFrame to list of dictionaries
            data = df.to_dict(orient='records')

            # Create DataFrame from list of dictionaries
            df_result = pd.DataFrame(data)

            # Write DataFrame to Streamlit
            st.write(df_result)
        else:
            st.write("Query result is empty.")
    elif query_number == 2:
        prj_query2 = "select b.Channel_Name as Channel_Name,Count(a.Video_ID) as Video_Count from Video_Details a, Channel_Details b where a.Channel_ID=b.Channel_ID group by b.Channel_Name order by 2 desc;"
        query_result = db_manage.Query_Output(prj_query2)
        if query_result:
            df = pd.DataFrame(query_result, columns=['Channel_Name', 'Video_Count'])

            # Convert DataFrame to list of dictionaries
            data = df.to_dict(orient='records')

            # Create DataFrame from list of dictionaries
            df_result = pd.DataFrame(data)

            # Write DataFrame to Streamlit
            st.write(df_result)
        else:
            st.write("Query result is empty.")

    elif query_number == 3:
        prj_query3 = "Select Channel_Name,Video_Title, max(Video_View_Count) Maximun_Video_View_Count from Video_Details a, Channel_Details b where a.Channel_ID=b.Channel_ID group by Video_Title,Channel_Name order by 3 desc limit 10;"
        query_result = db_manage.Query_Output(prj_query3)
        if query_result:
            df = pd.DataFrame(query_result, columns=['Channel_Name','Video_Title', 'Maximun_Video_View_Count'])
            # Reset index and drop the current index
            df_without_index = df.reset_index(drop=True)
            st.write(df_without_index, index=False)
        else:
            st.write("Query result is empty.")

    elif query_number == 4:
        prj_query4 = "select b.Video_Title as Video_Title,Count(a.Comment_ID) as Comment_Count from Comment_Details a, Video_Details b where a.Video_ID=b.Video_ID group by a.Video_ID,b.Video_Title order by 2 desc;"
        query_result = db_manage.Query_Output(prj_query4)
        if query_result:
            df = pd.DataFrame(query_result, columns=['Video_Title', 'Comment_Count'])
            # Reset index and drop the current index
            df_without_index = df.reset_index(drop=True)
            st.write(df_without_index, index=False)
        else:
            st.write("Query result is empty.")

    elif query_number == 5:
        prj_query5 = "Select Channel_Name,Video_Title, max(Video_Like_Count) Maximun_Video_Like_Count from Video_Details a, Channel_Details b where a.Channel_ID=b.Channel_ID group by Video_Title,Channel_Name order by 3 desc;"
        query_result = db_manage.Query_Output(prj_query5)
        if query_result:
            df = pd.DataFrame(query_result, columns=['Channel_Name', 'Video_Title', 'Maximun_Video_Like_Count'])
            # Reset index and drop the current index
            df_without_index = df.reset_index(drop=True)
            st.write(df_without_index, index=False)
        else:
            st.write("Query result is empty.")

    elif query_number == 6:
        prj_query6 = "Select Channel_Name,Video_Title, SUM(Video_Like_Count) Total_Video_Like_Count from Video_Details a, Channel_Details b where a.Channel_ID=b.Channel_ID group by Video_Title,Channel_Name order by 3 desc;"
        query_result = db_manage.Query_Output(prj_query6)
        if query_result:
            df = pd.DataFrame(query_result, columns=['Channel_Name', 'Video_Title', 'Total_Video_Like_Count'])
            # Reset index and drop the current index
            df_without_index = df.reset_index(drop=True)
            st.write(df_without_index, index=False)
        else:
            st.write("Query result is empty.")

    elif query_number == 7:
        prj_query7 = "select Channel_Name, Channel_view_count as Total_Channel_View_Count from Channel_Details order by 2 desc;"
        query_result = db_manage.Query_Output(prj_query7)
        if query_result:
            df = pd.DataFrame(query_result, columns=['Channel_Name', 'Total_Channel_View_Count'])
            # Reset index and drop the current index
            df_without_index = df.reset_index(drop=True)
            st.write(df_without_index, index=False)
        else:
            st.write("Query result is empty.")

    elif query_number == 8:
        prj_query8 = "select DISTINCT Channel_Name as Channel_Name,year(Video_Published_Date) as Extract_Year_From_Published_Date from Video_Details a, Channel_Details b where a.Channel_ID=b.Channel_ID and year(Video_Published_Date) = 2022;"
        query_result = db_manage.Query_Output(prj_query8)
        if query_result:
            df = pd.DataFrame(query_result, columns=['Channel_Name', 'Extract_Year_From_Published_Date'])
            # Reset index and drop the current index
            df_without_index = df.reset_index(drop=True)
            st.write(df_without_index, index=False)
        else:
            st.write("Query result is empty.")

    elif query_number == 9:
        prj_query9 = "select b.Channel_Name as Channel_Name, time_format(SEC_TO_TIME(AVG(TIME_TO_SEC(a.VIDEO_DURATION))),'%H:%i:%s') AS AVERAGE_VIDEO_DURATION from Video_Details a, Channel_Details b where a.Channel_ID=b.Channel_ID  group by Channel_Name order by 2 desc;"
        query_result = db_manage.Query_Output(prj_query9)
        if query_result:
            df = pd.DataFrame(query_result, columns=['Channel_Name', 'AVERAGE_VIDEO_DURATION'])
            # Reset index and drop the current index
            df_without_index = df.reset_index(drop=True)
            st.write(df_without_index, index=False)
        else:
            st.write("Query result is empty.")

    elif query_number == 10:
        prj_query10 = "select c.Channel_Name Channel_Name,b.Video_Title Video_Title, MAX(a.Comment_Count) as Maximum_Comment_Count from Channel_Details c,Video_Details b,(select Video_ID,count(Comment_ID) as Comment_Count from Comment_Details group by Video_ID) a where c.Channel_ID=b.Channel_ID and b.Video_ID=a.Video_ID group by c.Channel_Name,b.Video_Title order by 3 desc;"
        query_result = db_manage.Query_Output(prj_query10)
        if query_result:
            df = pd.DataFrame(query_result, columns=['Channel_Name', 'Video_Title', 'Maximum_Comment_Count'])
            # Reset index and drop the current index
            df_without_index = df.reset_index(drop=True)
            st.write(df_without_index, index=False)
        else:
            st.write("Query result is empty.")

# Main area
st.write("Query Result")
st.write(selected_query_description)
# Execute the selected query and display the result
execute_query_and_display_result(selected_query_number)




