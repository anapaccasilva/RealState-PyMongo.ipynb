# -*- coding: utf-8 -*-
"""
MongoDB São Paulo Real State Data Manipulation 
Author: Ana Paula Pacca
"""

# Install required packages
!python -m pip install pymongo
import requests
import certifi
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# MongoDB connection URI
uri = "mongodb+srv://appacca:12345@cluster-fatec.aajed1r.mongodb.net/?retryWrites=true&w=majority&appName=Cluster-fatec"

# Connect to MongoDB server
client = MongoClient(uri, server_api=ServerApi('1'), tls=True, tlscafile=certifi.where())

# Verify connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(f"Connection failed: {e}")

# Load and insert data into collection
db = client["db_imoveis_sp"]
collection = db["imoveis_sp"]
response = requests.get("https://raw.githubusercontent.com/samuelhenrick1/MongoDB/main/fatec.imoveis.json")

# Insert documents into the collection
docs = collection.insert_many(response.json())
print(f"Inserted {len(docs.inserted_ids)} documents.")

# Count of documents in the collection
doc_count = collection.count_documents({})
print(f"Total documents in collection: {doc_count}")

# Listing available databases and collections
print(f"Databases: {client.list_database_names()}")
print(f"Collections in db_imoveis_sp: {db.list_collection_names()}")

# Create and insert documents into a new collection
colab_collection = db["Colab"]
documento = {"Fatec": "Ipiranga"}
colab_collection.insert_one(documento)

# Define a function to display documents in a collection
def show_collection(collection):
    documentos = collection.find()
    for documento in documentos:
        print(documento)

# Insert multiple documents
documents = [
    {"ETEC": "018"},
    {"ETEC": "061"},
    {"ETEC": "222"}
]
colab_collection.insert_many(documents)

# Display the inserted documents
show_collection(colab_collection)

# Delete a single record
colab_collection.delete_one({"ETEC": "061"})
show_collection(colab_collection)

# Delete multiple records
colab_collection.delete_many({"$or": [{"ETEC": "018"}, {"ETEC": "222"}]})
show_collection(colab_collection)

# Insert and display a new document
colab_collection.insert_one({"ETEC": "Cotia"})
show_collection(colab_collection)

# Insert multiple documents again
colab_collection.insert_many([{"ETEC": "061"}, {"ETEC": "204"}])
show_collection(colab_collection)

# Delete documents based on a condition
colab_collection.delete_many({"$or": [{"FATEC": "Ipiranga"}, {"ETEC": "Cotia"}, {"FATEC": "204"}]})
show_collection(colab_collection)

# Drop the collection
colab_collection.drop()
print(f"Collections after drop: {db.list_collection_names()}")

# Perform queries on imoveis_sp collection
colecao = db["imoveis_sp"]

# Filtering all documents where the neighborhood (bairro) is "paraiso"
resultado = colecao.find({"bairro": "paraiso"})
for documento in resultado:
    print(documento)

# Filtering documents with area greater than 60, sorted by area in ascending order, limit 15
resultado = colecao.find({"areaM2": {"$gt": 60}}).sort("areaM2", 1).limit(15)
for documento in resultado:
    print(documento)

# Various filter and sort operations
# Filtering area equal to 40 or 60
resultado = colecao.find({"areaM2": {"$in": [40, 60]}}).sort("areaM2", 1)
for documento in resultado:
    print(documento)

# Filtering based on $or condition
resultado = colecao.find({"$or": [{"vagas": 2}, {"suites": 2}]}, {"_id": 0}).sort("vagas", -1).limit(10)
for documento in resultado:
    print(documento)

# Filtering based on $and condition
resultado = colecao.find({"$and": [{"vagas": 2}, {"suites": 2}]}, {"_id": 0}).sort("vagas", -1).limit(10)
for documento in resultado:
    print(documento)

# Switch to db_produtos database and list collections
db = client["db_produtos"]
print(f"Collections in db_produtos: {db.list_collection_names()}")

# Close connection
client.close()
print("MongoDB connection closed.")
