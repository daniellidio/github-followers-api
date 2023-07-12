# GitHub Followers Information

This project retrieves GitHub user followers data using Flask API and Apache Spark.

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/daniellidio/github-followers-api
   ```

2. Navigate to the project directory:
   ```
   cd github-followers-api
   ```

3. Create and activate a virtual environment:
   ```
   python -m venv .venv
   source .venv/bin/activate
   ```

3. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

## Usage

1. Run the Flask API:
   ```
   python main.py
   ```

2. Make a GET request to the root endpoint (`/`) with the following headers:
   - `x-api-key`: `3f293e29-c095-4078-9adc-99c490273b6b`

   Example using cURL:
   ```
   curl -H "x-api-key: 3f293e29-c095-4078-9adc-99c490273b6b" http://localhost:5000
   ```

3. Provide the `githubuser` query parameter with the GitHub username of the user whose followers' data you want to retrieve.

   Example using cURL:
   ```
   curl -H "x-api-key: 3f293e29-c095-4078-9adc-99c490273b6b" http://localhost:5000/?githubuser=daniellidio
   ```

   Replace `daniellidio` with the desired GitHub username.

4. The API will return a CSV as response containing the cleaned data of the user's followers.
