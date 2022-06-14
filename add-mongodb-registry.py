#!/usr/bin/python3
import pymongo
import json

with open('cluster-config.json', 'r') as f:
    config = json.load(f)


client = pymongo.MongoClient(f"mongodb+srv://{config['user']}:{config['password']}@{config['endpoint']}/?retryWrites=true&w=majority")
db = client.test

db = client[config['db']]
mycol = db[config['collection']]

entry = {'commit': '2bf740b0414a2a0d707b6f9a22291feeba97b7d9', 'username': 'Kaysera'}

x = mycol.insert_one(entry)