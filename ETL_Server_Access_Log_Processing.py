import requests

input_file = 'data/web-server-access-log.txt'
extract_file = 'data/extracted-data.txt'
transform_file = 'data/transformed.txt'
load_file = 'data/capitalized.txt'

def download_file():
    url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt"
    # Send a GET request to the URL
    with requests.get(url, stream=True) as response:
        # Raise an exception for HTTP errors
        response.raise_for_status()
        # Open a local file in binary write mode
        with open(input_file, 'wb') as file:
            # Write the content to the local file in chunks
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
    print(f"File downloaded successfully: {input_file}")

def extract():
    global input_file
    # Read the contents of the file into a string
    with open(input_file, 'r') as infile, \
        open(extract_file, 'w') as outfile:
        for line in infile:
            fields = line.split('#')
            if len(fields) >= 4:
                field_1 = fields[0]
                field_4 = fields[3]
                outfile.write(field_1 + "#" + field_4 + "\n" )

def transform():
    global extract_file, transform_file
    # Read extracted file and write to transform file
    with open(extract_file, 'r') as infile, \
        open(transform_file, 'w') as outfile:
        for line in infile:
            processed_line = line.upper()
            outfile.write(processed_line)

def load():
    global transform_file, load_file
    print("Loading....")
    with open(transform_file,'r') as infile, \
        open(load_file, 'w') as outfile:
        for line in infile:
            outfile.write(line)

def check():
    global load_file
    print("checking...")
    with open(load_file, 'r') as infile:
        for line in infile:
            print(line)


download_file()
extract()
transform()
load()
check()
