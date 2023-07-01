import json
import requests
from user_posting_emulation import run_infinite_post_data_loop



@run_infinite_post_data_loop
def stream_message(stream_name, *message_dict):
    
    """
    This methods make RESTful API request to Apache MSK cluster.

    Args:
    ----------------------------------------------------------------------------------------
    message_dict (json) : The message requested to the API
    
    topic_name (str) : The MSK topic name where the post message will be stored

    Return
    status_code (int) : The response status code from the server

    """

    try:
        if stream_name == 'streaming-12853887c065-pin':
            PartitionKey = "1"
        elif stream_name == 'streaming-12853887c065-geo':
            PartitionKey = "2"
        elif stream_name == 'streaming-12853887c065-user':
            PartitionKey = "3"
    except NameError:
        print('Invalid Stream name') 
    
    print(type(PartitionKey))
    payload = json.dumps({
        "StreamName": stream_name,
        "Data": {
                #Data should be send as pairs of column_name:value, with different columns separated by commas      
                "index": message_dict
                },
                "PartitionKey": PartitionKey
                }, default=str)

    headers = {'Content-Type': 'application/json'}
    
    invoke_url = f"https://s2ez23hzo7.execute-api.us-east-1.amazonaws.com/test/streams/{stream_name}/records"

    response = requests.request("PUT", invoke_url, headers=headers, data=payload)

    print(response.status_code)
    print('Put completed')



if __name__ == '__main__':
    streams = ['streaming-12853887c065-pin', 'streaming-12853887c065-geo', 'streaming-12853887c065-user']
    stream_message(streams)
