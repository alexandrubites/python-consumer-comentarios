# Import some necessary modules
# pip install kafka-python
# pip install pymongo
# pip install "pymongo[srv]"
from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.server_api import ServerApi

import json

uri = "mongodb+srv://alexandrubites:IUMD6wqtYGbVh0yw@cluster0.z4xurdo.mongodb.net/?retryWrites=true&w=majority"
# Create a new client and connect to the server
# client = MongoClient(uri, server_api=ServerApi('1'))
# Send a ping to confirm a successful connection

# try:
#    client.admin.command('ping')
#    print("Pinged your deployment. You successfully connected to MongoDB!")
# except Exception as e:
#    print(e)

# Connect to MongoDB and pizza_data database

try:
    client = MongoClient(uri, server_api=ServerApi('1'))
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")

    db = client.bdnosql
    print("MongoDB Connected successfully!")
except Exception as e:
    print("Could not connect to MongoDB: ", e)


consumer = KafkaConsumer('comments', bootstrap_servers=[
'my-kafka-0.my-kafka-headless.alexandrubites.svc.cluster.local:9092'])
# Parse received data from Kafka
for msg in consumer:
    record = json.loads(msg.value)
    print(record)
    userId = record["userId"]
    objectId = record["objectId"]
    comment = record["comment"]

    # Create dictionary and ingest data into MongoDB
    try:
        comment_rec = {
            'userId': userId,
            'objectId': objectId,
            'comment': comment
        }
        print(comment_rec)
        comment_id = db.bdnosql_comments.insert_one(comment_rec)
        print("Comment inserted with record ids", comment_id)
    except Exception as e:
        print("Could not insert into MongoDB:", e)

    # Create bdnosql_sumary and insert groups into mongodb
    try:
        agg_result = db.bdnosql_comments.aggregate([
            {
                "$group": {
                    "_id": "$objectId",
                    "nComments": {"$sum": 1}
                }
            }
        ])
        db.bdnosql_sumaryComments.delete_many({})
        for i in agg_result:
            print(i)
            sumaryComments_id = db.bdnosql_sumaryComments.insert_one(i)
            print("Sumary Comments inserted with record ids: ", sumaryComments_id)
    except Exception as e:
        print(f'group vy cought {type(e)}: ')
        print(e)
