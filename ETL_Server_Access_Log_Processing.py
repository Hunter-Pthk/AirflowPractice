import requests

input_file = 'data/web-server-access-log.txt'

url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt"
# Send a GET request to the URL
with requests.get(url, stream=True) as response:
    # Raise an exception for HTTP errors
    response.raise_for_status()
    # Open a local file in binary write mode
    with open(input_file, 'wb') as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)
print(f"File downloaded successfully: {input_file}")



