import requests
import pandas as pd
import json
import s3fs
from datetime import datetime


def autanticating():
    # note that CLIENT_ID refers to 'personal use script' and SECRET_TOKEN to 'token'
    auth = requests.auth.HTTPBasicAuth('<CLIENT_ID>', '<SECRET_TOKEN>')

    # here we pass our login method (password), username, and password
    data = {'grant_type': 'password',
            'username': '<USERNAME>',
            'password': '<PASSWORD>'}

    # setup our header info, which gives reddit a brief description of our app
    headers = {'User-Agent': 'de-dit/0.0.1'}

    # send our request for an OAuth token
    res = requests.post('https://www.reddit.com/api/v1/access_token',
                        auth=auth, data=data, headers=headers)

    # convert response to JSON and pull access_token value
    TOKEN = res.json()['access_token']

    # add authorization to our headers dictionary
    headers['Authorization'] = f"bearer {TOKEN}"
    return headers


def pull_post(**context):
    ti = context['ti']
    headers = ti.xcom_pull(task_ids='auth')
    res = requests.get("https://oauth.reddit.com/r/python/hot", headers=headers, params={'limit': '100'})
    with open('response.json', 'w') as f:
        # Write the JSON variable to the file
        json.dump(res.json(), f)


def transforming_to_csv():
    # read the json file that contains the posts
    with open('response.json', 'r') as f:
        res = json.load(f)
    # initialize dataframe
    df = pd.DataFrame()

    # loop through each post retrieved from GET request
    for post in res['data']['children']:
        # append relevant data to dataframe
        new_row = {
            'subreddit': post['data']['subreddit'],
            'title': post['data']['title'],
            'selftext': post['data']['selftext'],
            'upvote_ratio': post['data']['upvote_ratio'],
            'ups': post['data']['ups'],
            'downs': post['data']['downs'],
            'score': post['data']['score'],
            "name": post['data']['name']
        }
        df = pd.concat([df, pd.DataFrame(new_row, index=[0])], ignore_index=True)

        # saving transformed data to csv file
        df.to_csv('converted_data.csv')


def loading_data():
    # reading the transformed posts
    df = pd.read_csv('converted_data.csv')

    # Get the current date and time
    now = datetime.now()
    # Format the current date and time
    formatted_date = now.strftime("%Y-%m-%d-%H:%M")

    # loading data to s3
    df.to_csv(f's3://redit-etl-us-east-1-dev/{formatted_date}.csv')
