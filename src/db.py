import pandas as pd
import pymongo

# Import data to DB
file = 'movies_metadata.csv'
client = pymongo.MongoClient()
db = client['moviedb']
collection = db['movie']
df = pd.read_csv(file)
df.reset_index(inplace=True)
data_dict = df.to_dict("records")
collection.insert_many(data_dict)
