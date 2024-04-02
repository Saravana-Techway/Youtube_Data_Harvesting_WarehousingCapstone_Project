from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, VARCHAR, Time, ForeignKey, text
from sqlalchemy.exc import IntegrityError

class Database_Management:
    def __init__(self):
        self.db_connection = ""
        self.database_name = "youtube_harvesting_capstone"
        self.engine = create_engine(self.db_connection)

    def create_database(self):
        try:
            # Establish a connection to the MySQL server
            connection = self.engine.connect()

            # Create the database if it doesn't exist
            create_db_statement = text(f"CREATE DATABASE IF NOT EXISTS {self.database_name}")
            connection.execute(create_db_statement)

            # Close the connection explicitly
            connection.close()
        except Exception as e:
            print(f"An error occurred while creating the database: {e}")
            raise

    def table_creation(self):
        try:
            # Establish a connection to the MySQL server
            connection = self.engine.connect()

            # Use the database
            use_db_statement = text(f"USE {self.database_name}")
            connection.execute(use_db_statement)

            # Define metadata
            metadata = MetaData()

            # Define tables
            channel_table = Table(
                'Channel_Details', metadata,
                Column('Channel_ID', VARCHAR(80), primary_key=True),
                Column('Channel_Name', VARCHAR(80)),
                Column('Channel_Description', VARCHAR(300)),
                Column('Channel_Published_Date', DateTime),
                Column('Channel_Subscription_Count', Integer, nullable=True),
                Column('Channel_Video_Count', Integer, nullable=True),
                Column('Channel_View_Count', Integer, nullable=True),
                Column('Channel_Playlist_ID', VARCHAR(100), nullable=True),
                schema=self.database_name
            )

            # Create the table if it doesn't exist
            channel_table.create(self.engine, checkfirst=True)

            video_table = Table(
                'Video_Details', metadata,
                Column('Video_ID', VARCHAR(80), primary_key=True),
                Column('Channel_ID', VARCHAR(80),ForeignKey(f'{self.database_name}.Channel_Details.Channel_ID')),
                Column('Video_Description', VARCHAR(500)),
                Column('Video_Title', VARCHAR(500)),
                Column('Video_Duration', Time, nullable=True),
                Column('Video_Published_Date', DateTime, nullable=True),
                Column('Video_Comment_Count', Integer, nullable=True),
                Column('Video_Like_Count', Integer, nullable=True),
                Column('Video_View_Count', Integer, nullable=True),
                schema=self.database_name
            )

            # Create the table if it doesn't exist
            video_table.create(self.engine, checkfirst=True)

            comment_table = Table(
                'Comment_Details', metadata,
                Column('Comment_ID', VARCHAR(80), primary_key=True),
                Column('Video_ID', VARCHAR(80),ForeignKey(f'{self.database_name}.Video_Details.Video_ID')),
                Column('Video_Comment', VARCHAR(1000), nullable=True),
                Column('Video_Comment_Author', VARCHAR(500), nullable=True),
                Column('Comment_Published_Date', DateTime, nullable=True),
                schema=self.database_name
            )

            # Create the table if it doesn't exist
            comment_table.create(self.engine, checkfirst=True)

            # Close the connection explicitly
            connection.close()

        except IntegrityError as ie:
            print(f"An integrity error occurred while creating the tables: {ie}")
            raise

        except Exception as e:
            print(f"An error occurred while creating the database: {e}")
            raise

    def Check_Channel_Info(self, Chnl_Chk_Query):
        try:
            # Establish a connection to the MySQL server
            connection = self.engine.connect()

            # Use the database
            use_db_statement = text(f"USE {self.database_name}")
            connection.execute(use_db_statement)

            select_statement=text(Chnl_Chk_Query)
            result = connection.execute(select_statement)
            channel_result=result.fetchall()

            if channel_result:
                return channel_result[0][0]
            else:
                return None
            connection.close()

        except Exception as e:
            print(f"An error occurred while creating the database: {e}")
            raise

    def df_to_sql(self,df_details,table_name):
        try:
            # Establish a connection to the MySQL server
            connection = self.engine.connect()

            # Use the database
            use_db_statement = text(f"USE {self.database_name}")
            connection.execute(use_db_statement)

            # Push the dataframe to sql
            df_details.to_sql(table_name, con=self.engine, if_exists='append', index=False, schema=self.database_name)

            # Close the connection explicitly
            connection.close()
        except Exception as e:
            print(f"An error occurred while creating the database: {e}")
            raise

    def Query_Output(self, Chnl_Chk_Query):
        try:
            # Establish a connection to the MySQL server
            connection = self.engine.connect()

            # Use the database
            use_db_statement = text(f"USE {self.database_name}")
            connection.execute(use_db_statement)

            select_statement=text(Chnl_Chk_Query)
            result = connection.execute(select_statement)
            channel_result=result.fetchall()

            if channel_result:
                return channel_result
            else:
                return None
            connection.close()

        except Exception as e:
            print(f"An error occurred while creating the database: {e}")
            raise