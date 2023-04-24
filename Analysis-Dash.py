from dash import Dash, html, dash_table, dcc
import plotly.express as px
import pandas as pd
import boto3
import os
from pyspark.sql import SparkSession

""" 
    Taken from file TopActors.py
"""
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Learning_Spark") \
    .config("spark.ui.port", "4041") \
    .getOrCreate()

BUCKET_NAME = 'imdb-kaminsca'

aws_access_key = input("Enter aws access key id:\n");
aws_secret_access_key = input("Enter aws secret access key:\n");
# enter authentication credentials
s3 = boto3.resource('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_access_key)


"""
    Functions
"""

# use these for multiple aggregation functions
from pyspark.sql.functions import sum, avg, max, min, mean, count, collect_list

# read directory from s3
def downloadDirectoryFroms3(bucketName, remoteDirectoryName):
    bucket = s3.Bucket(bucketName)
    for obj in bucket.objects.filter(Prefix=remoteDirectoryName):
        if not obj.key.startswith('.') and not obj.key.endswith('/'):
            if not os.path.exists(os.path.dirname(obj.key)):
                os.makedirs(os.path.dirname(obj.key))
            bucket.download_file(obj.key, obj.key)  # save to same path


# Read and combine parquet files from a directory into a spark df
def readLocalDirectoryToParquet(directory):
    file_path_list = []
    files = os.listdir(directory)
    for file in files:
        if ".parquet" in file:
            file_path_list.append(directory + "/" + file)
    return spark.read.parquet(*file_path_list)


"""### Taking parquet file from s3"""

# Finding the data in the s3 folder 'ActorRoles'
directoryName = 'ActorRoles'
downloadDirectoryFroms3(BUCKET_NAME, directoryName)
actorRoles = readLocalDirectoryToParquet(directoryName)

# Let's sort these actors by the average rating of their movies!
bestActors = actorRoles.groupBy("primaryName") \
    .agg(avg("averageRating"), \
         count("*").alias("numMovies"), \
         collect_list("primaryTitle").alias("Movies")) \
    .orderBy("avg(averageRating)", ascending=False)

# top actors who have more than 5 movies with more than 25000 votes each
bestActors = bestActors.filter(bestActors.numMovies > 5).limit(20)

"""
    End of code from TopActors.py
"""


app = Dash(__name__)

df = bestActors.toPandas()
# convert the DataFrame to a list of dictionaries
data = df.to_dict('records')
print(df)
fig = px.scatter(df, x='numMovies', y='avg(averageRating)', title='My Graph', hover_name="primaryName")
fig.show()

print("Creating layout")
app.layout = html.Div([
    html.Div(children='Actors'),
    #dash_table.DataTable(data=df, page_size=10),
    dcc.Graph(
        id='my-graph',
        figure= fig
    )
])

if __name__ == '__main__':
    print("Starting app server")
    app.run_server()