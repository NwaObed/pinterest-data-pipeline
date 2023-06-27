import requests
import yaml
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text


random.seed(100)
# invoke_url = "https://YourAPIInvokeURL/YourDeploymentStage/topics/YourTopicName"
invoke_url = "https://s2ez23hzo7.execute-api.us-east-1.amazonaws.com/test/topics/"
db_cred_file = './db_cred.yaml'


class AWSDBConnector:

    """
    This implements a secured connection to AWS database
    
    """

    def __init__(self):
        
        self.cred = self.read_db_creds(db_cred_file)
        self.HOST = self.cred['HOST']
        self.USER = self.cred['USER']
        self.PASSWORD = self.cred['PASSWORD']
        self.DATABASE = self.cred['DATABASE']
        self.PORT = self.cred['PORT']

    def read_db_creds(self, file):
        """
        This method reads the database credentials from a file and returns a dict of the credentials

        Args:
        -----------------------------------------------
        file (str) : The yaml file name containing the database credentials


        Returns:
        -------------------------------------------------
        cred (dict) : The dictionary object of the database credentials
        """

        with open(file, 'r') as my_file:
            cred = yaml.safe_load(my_file)
        return cred
        
    def create_db_connector(self):
        """
        This method creates the engine used to connect to the database

        Returns
        --------------------------------------------------------
        engine : sqlalchemy engine
        """


        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()


def run_infinite_post_data_loop():

    """
    This method makes an infinite API request to the MSK cluster to post messages to three topics.The message posted to the topics is randomly selected from the Pinterest table hosted on AWS dababase. The topics are the pinterest post, geolocation of the post and the users details"""


    while True:
        sleep(random.randrange(0, 5))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)
                post_message(pin_result,'12853887c065.pin')

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)
                post_message(geo_result,'12853887c065.geo')

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
                post_message(user_result,'12853887c065.user')
        connection.close()

def post_message(message_dict, topic_name):
    #To send JSON messages you need to follow this structure
    # value_dict = {}
    # for val in message_dict:
    #     value_dict[val] = message_dict[val]

    """
    This methods make RESTful API request to Apache MSK cluster.

    Args:
    ----------------------------------------------------------------------------------------
    message_dict (json) : The message requested to the API
    
    topic_name (str) : The MSK topic name where the post message will be stored

    Return
    status_code (int) : The response status code from the server

    """

    payload = json.dumps({
        "records": [
            {
            #Data should be send as pairs of column_name:value, with different columns separated by commas       
            "value": message_dict
            }
        ]
    }, default=str)

    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}

    response = requests.request("POST", invoke_url+topic_name, headers=headers, data=payload)
    print(response.status_code)
    
    return response.status_code



if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')

    
    


