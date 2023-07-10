import requests
from flask import Flask, request, jsonify
from pyspark.context import SparkContext
from pyspark.sql import functions as F, SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

app = Flask(__name__)

spark = SparkSession.builder.appName("API").getOrCreate()

# Sample API key for demonstration purposes
X_API_KEY = '3f293e29-c095-4078-9adc-99c490273b6b'

# Endpoint URL to make a request
API_URL = 'https://api.github.com/users/daniellidio/followers'

githubuser = ''

followers_list = []

followers_info = []

@app.route('/', methods=['GET'])
def make_request():
    # Check if 'api_key' is present in the request headers
    if 'x-api-key' not in request.headers:
        return jsonify({'error': 'X-API-Key is required.'}), 400

    # Verify the provided API key
    provided_x_api_key = request.headers.get('x-api-key')
    if provided_x_api_key != X_API_KEY:
        return jsonify({'error': 'Invalid X-API-Key'}), 401

    # Check if 'githubuser' parameter is provided
    githubuser = request.args.get('githubuser')
    if not githubuser:
        return jsonify({'error': '"githubuser" is required.'}), 402

    followers_list = get_followers(githubuser)

    data = followers_data(followers_list)

    return jsonify(data)

    
def get_followers(githubuser):

    url = f'https://api.github.com/users/{githubuser}/followers'

    response = requests.get(url)
    if response.status_code == 200:
        # Successful response
        followers = response.json()
        # Process the data as needed
        for follower in followers:
            followers_list.append(follower['login'])
        
        return followers_list
    else:
        # Error response
        error_message = f'Error occurred while making the request: {response.text}'
        return jsonify({'error': error_message}), response.status_code

def followers_data(followers_list):

    for follower in followers_list:

        url = f'https://api.github.com/users/{follower}'

        response = requests.get(url)

        data = response.json()

        followers_info.append(data)

    return followers_info

def select_data(data):
    return True

if __name__ == '__main__':
    app.run()

