from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import os
from dotenv import load_dotenv
import json


load_dotenv()
user = os.getenv('DB_USER') 
password = os.getenv('DB_PASSWORD')

# Conexión a MongoDB
uri = f"mongodb+srv://{user}:{password}@cluster1.ejeppg3.mongodb.net/?retryWrites=true&w=majority&appName=Cluster1"
# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))
# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

# Selección de base y colección
db = client["m3_pi"]
collection = db["dataset_henry"]

# Leer todos los archivos JSON en el directorio actual
json_files = [f for f in os.listdir('files') if f.endswith('.json')]

# Insertar documentos desde cada archivo JSON
for file_name in json_files:
    with open(f'files/{file_name}', 'r', encoding='utf-8') as f:
        data = json.load(f)
        if isinstance(data, list):
            collection.insert_many(data)
        else:
            collection.insert_one(data)

print("Carga completa de archivos JSON en MongoDB.")
