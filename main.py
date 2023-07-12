import requests
import json
from flask import Flask, request, jsonify, Response
from pyspark.sql import functions as F, SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
import io
import pandas as pd

app = Flask(__name__)

spark = SparkSession.builder.appName("API").getOrCreate()

# Sample API key for demonstration purposes
X_API_KEY = '3f293e29-c095-4078-9adc-99c490273b6b'

githubuser = ''

followers_list = []

followers_info = []

with open("schema.json") as f:
    json_schema = json.load(f)

schema = StructType.fromJson(json_schema)

@app.route('/', methods=['GET'])
def make_request():
    """
    A function that makes a request to the root endpoint.

    Returns:
    - If 'x-api-key' is not present in the request headers, returns a JSON response with an error message and status code 400.
    - If the provided 'x-api-key' is not valid, returns a JSON response with an error message and status code 401.
    - If the 'githubuser' parameter is not provided, returns a JSON response with an error message and status code 402.
    - If an exception occurs during the execution of the function, returns a JSON response with the error message and status code 500.
    - Otherwise, returns the result of exporting a cleaned DataFrame as a CSV file.
    """
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

    try:
        followers_list = get_followers(githubuser)
        df = create_spark_df()
        df = followers_data(followers_list, df)
        df_select = select_columns(df)
        df_cleaned = clean_data(df_select)
        return export_csv(df_cleaned)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

def get_followers(githubuser):
    """
    Retrieves the followers of a given GitHub user.

    Args:
        githubuser (str): The username of the GitHub user.

    Returns:
        list: A list of the followers' usernames.
    """

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

def create_spark_df():
    """
    Creates a new Spark DataFrame with defined schema.
    Returns:
        df: Spark DataFrame.
    """

    df = spark.createDataFrame([], schema)

    return df

def followers_data(followers_list, df):
    """
    Retrieves data for each follower in the given followers list and appends it to the provided dataframe.

    Parameters:
    - followers_list (list): A list of strings representing the usernames of the followers.
    - df (DataFrame): The dataframe to which the retrieved data will be appended.

    Returns:
    - DataFrame: The updated dataframe with the appended data.
    """

    for follower in followers_list:

        url = f'https://api.github.com/users/{follower}'
        response = requests.get(url)
        data = response.json()
        
        new_row = spark.createDataFrame([data], schema)
        df = df.union(new_row)

    return df

def select_columns(df):
    """
    Selects specific columns from a DataFrame.

    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: The DataFrame with the selected columns.
    """

    df_select = df.select('name', 'company', 'blog', 'email', 'bio', 'public_repos', 'followers', 'following', 'created_at')

    return df_select

def clean_data(df):
    """
    Cleans the data in the given DataFrame by formatting the "created_at" column to "dd/MM/yyyy" format and removing any "@" symbol from the "company" column.

    Parameters:
    - df: The DataFrame to be cleaned.

    Returns:
    - df_clean_company: The cleaned DataFrame.
    """

    df_date_formated = df.withColumn("created_at", date_format(col("created_at"), "dd/MM/yyyy"))

    df_clean_company = df_date_formated.withColumn("company", regexp_replace(col("company"), "@", ""))

    return df_clean_company

def export_csv(df):
    """
    Export a DataFrame to a CSV file and return it as a Response object.

    Parameters:
        df (DataFrame): The DataFrame to be exported.

    Returns:
        Response: The CSV file as a Response object.

    """
    pandas_df = df.toPandas()
    csv_buffer = io.StringIO()
    pandas_df.to_csv(csv_buffer, index=False)
    headers = {
        'Content-Type': 'text/csv',
        'Content-Disposition': 'attachment; filename=data.csv'
    }
    return Response(csv_buffer.getvalue(), headers=headers)

if __name__ == '__main__':
    app.run()