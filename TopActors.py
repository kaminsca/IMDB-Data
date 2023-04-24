

import boto3
import os
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Learning_Spark") \
    .getOrCreate()

BUCKET_NAME = 'imdb-kaminsca'

aws_access_key = input("Enter aws access key id:\n");
aws_secret_access_key = input("Enter aws secret access key:\n");
# enter authentication credentials
s3 = boto3.resource('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_access_key)


"""## Data pipelining functions

"""

# use these for multiple aggregation functions
from pyspark.sql.functions import sum, avg, max, min, mean, count, collect_list


# Upload local directory to s3
def uploadDirectory(path, bucketname, directory):
    for root, dirs, files in os.walk(path):
        for file in files:
            s3.meta.client.upload_file(os.path.join(root, file), bucketname, directory + '/' + file)


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
print("Showing data downloaded from s3: ")
actorRoles.show(truncate=False)

print("What we see if we filter by a specific actor: ")
actorRoles.filter(actorRoles.primaryName == "Kirk Douglas").show(10, False)

# Let's sort these actors by the average rating of their movies!
bestActors = actorRoles.groupBy("primaryName") \
    .agg(avg("averageRating"), \
         count("*").alias("numMovies"), \
         collect_list("primaryTitle").alias("Movies")) \
    .orderBy("avg(averageRating)", ascending=False)

bestActors = bestActors.filter(bestActors.numMovies > 5)
# top actors who have more than 5 movies with more than 25000 votes each
print("Top actors who have more than 5 movies with more than 25000 votes each:")
bestActors.show(n=50, truncate=False)