import random
import requests
import json

from database_util import AWSDBConnector
from Posts.pinterest_streaming import stream_message
from time import sleep
from sqlalchemy import text

random.seed(100)
new_connector = AWSDBConnector()

class Pinterestpost:

    def emulate_pinterest(self, table_name, random_row, connection, topic_stream, batch=True):

        """
        Pinterest emulation method that posts messages from randomly selected rows in the AWS database onto three topics or streams.
        
        --------------------------------------------------------------
        Args:
        table_name (str) : AWS database table name to randomly select message from.

        randow_row (int) : row number randomly selected.

        connection : sqlalchemy engine object

        topic_stream (list) : list of topic or stream names.
        """

        data_string = text(f"SELECT * FROM {table_name} LIMIT {random_row}, 1")
        selected_row = connection.execute(data_string)


        for row in selected_row:
            post_result = dict(row._mapping)
            if batch:
                self.post_message(topic_stream, post_result)
            else:
                stream_message(topic_stream, post_result)
            
    def run_infinite_post_data_loop(self, topic_stream, batch=True):

        """
        This method makes an infinite API request to the MSK cluster to post messages to three topics or streams.
        
        --------------------------------------------------------------
        Args:
        topic_stream (list) : list of the topic or stream names.
        """

        while True:
            sleep(random.randrange(0, 5))
            random_row = random.randint(0, 11000)
            engine = new_connector.create_db_connector()

            with engine.connect() as connection:
                
                # Pin post
                self.emulate_pinterest('pinterest_data', random_row, connection, topic_stream[0], batch=batch)

                # Geolocation post
                self.emulate_pinterest('geolocation_data', random_row, connection, topic_stream[1], batch=batch)

                # User post
                self.emulate_pinterest('user_data', random_row, connection, topic_stream[2], batch=batch)
            connection.close()

    def post_message(self, topic_stream, msg):
        
        """
        This methods make RESTful API request to Apache MSK cluster.

        Args:
        ----------------------------------------------------------------------------------------
        topic_name (str) : The MSK topic name where the post message will be stored
        msg (dict) : key-value pair message posted

        Return
        status_code (int) : The response status code from the server

        """
        
        payload = json.dumps({
            "records": [
                {
                    "value": msg
                }
            ]
        }, default=str)

        headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}

        invoke_url = f"https://s2ez23hzo7.execute-api.us-east-1.amazonaws.com/test/topics/{topic_stream}"
        response = requests.request("POST", invoke_url, headers=headers, data=payload)
        print(response.status_code)
        
        return response.status_code


if __name__ == "__main__":
    post = Pinterestpost()

    #Batch processing
    topics = ['12853887c065.pin', '12853887c065.geo', '12853887c065.user']
    post.run_infinite_post_data_loop(topics) #Comment this line to run the streaming

    #Streaming
    streams = ['streaming-12853887c065-pin', 'streaming-12853887c065-geo', 'streaming-12853887c065-user']
    post.run_infinite_post_data_loop(streams, batch=False)