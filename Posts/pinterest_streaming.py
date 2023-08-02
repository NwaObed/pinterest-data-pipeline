import json
import requests


def stream_message(stream_name, message_dict):
    
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
            payload = json.dumps({
                "StreamName": stream_name,
                "Data": {
                        #Data should be send as pairs of column_name:value, with different columns separated by commas      
                        'index': message_dict["index"],
                        'unique_id': message_dict["unique_id"],
                        'title': message_dict["title"],
                        'follower_count': message_dict["follower_count"],
                        'poster_name': message_dict["poster_name"],
                        'tag_list': message_dict["tag_list"],
                        'is_image_or_video': message_dict["is_image_or_video"],
                        'image_src': message_dict["image_src"],
                        'save_location': message_dict["save_location"],
                        'category': message_dict["category"],
                        'downloaded': message_dict["downloaded"],
                        'description' : message_dict["description"]
                        },
                        "PartitionKey": "1"},
                 default=str)
        elif stream_name == 'streaming-12853887c065-geo':
            payload = json.dumps({
                "StreamName": stream_name,
                "Data": {
                        #Data should be send as pairs of column_name:value, with different columns separated by commas      
                        'ind': message_dict["ind"],
                        'country': message_dict["country"],
                        'latitude': message_dict["latitude"],
                        'longitude': message_dict["longitude"],
                        'timestamp': message_dict["timestamp"]
                        },
                        "PartitionKey": "2"},
                 default=str)
        elif stream_name == 'streaming-12853887c065-user':
            payload = json.dumps({
                "StreamName": stream_name,
                "Data": {
                        #Data should be send as pairs of column_name:value, with different columns separated by commas      
                        'ind': message_dict["ind"],
                        'first_name': message_dict["first_name"],
                        'last_name': message_dict["last_name"],
                        'age': message_dict["age"],
                        'date_joined': message_dict["date_joined"]
                        },
                        "PartitionKey": "3"},
                 default=str)
    except NameError:
        print('Invalid Stream name')

    headers = {'Content-Type': 'application/json'}
    
    invoke_url = f"https://s2ez23hzo7.execute-api.us-east-1.amazonaws.com/test/streams/{stream_name}/record"

    response = requests.request("PUT", invoke_url, headers=headers, data=payload)
    print(response.status_code)


