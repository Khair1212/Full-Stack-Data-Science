from mongodb import MongoDB
import time

# Make Connection with MongoDB
connectionString = "mongodb://root:root@cluster0-shard-00-00.dnihu.mongodb.net:27017,cluster0-shard-00-01.dnihu.mongodb.net:27017,cluster0-shard-00-02.dnihu.mongodb.net:27017/myFirstDatabase?ssl=true&replicaSet=atlas-kz38wl-shard-0&authSource=admin&retryWrites=true&w=majority"
conn = MongoDB(connectionString)
client = conn.get_client()

# create database
print('Creating Database ...')
time.sleep(2)
db = conn.create_database(db_name='ineuron', client= client)
print('Database Created!')

# create collection
print('Creating Collection ...')
time.sleep(2)
coll = conn.create_collection(coll_name='mongodb_task', database = db)
print('Collection Created!')

# read data from local system
print('Reading data from local system ...')
time.sleep(2)
df = conn.read_data("carbon_nanotubes.csv")
print('Data onboarded!')

# insert csv data (bulk) to mongodb
print('Inserting the data to MongoDB Database ...')
time.sleep(2)
conn.insert_bulk(dataframe = df, collection = coll)
print('All the data are Inserted!')

# insert one document
document = {'Chiral indice n': 2,
  'Chiral indice m': 3,
  'Initial atomic coordinate u': {'x': 0, 'y': 435263},
  'Initial atomic coordinate v': {'x': 0, 'y': 8446272},
  'Initial atomic coordinate w': {'x': 0, 'y': 874625},
  "Calculated atomic coordinates u'": {'x': 0, 'y': 764522},
  "Calculated atomic coordinates v'": {'x': 0, 'y':764627},
  "Calculated atomic coordinates w'": {'x': 0, 'y':546235}}

print('*** Insert One Document ***')
time.sleep(2)
print("Inserted Document: ", conn.insert_one(document= document, collection=coll))

# insert many documents
documents = [{'Chiral indice n': 4,
  'Chiral indice m': 3,
  'Initial atomic coordinate u': {'x': 0, 'y': 435263},
  'Initial atomic coordinate v': {'x': 0, 'y': 8446272},
  'Initial atomic coordinate w': {'x': 0, 'y': 874625},
  "Calculated atomic coordinates u'": {'x': 0, 'y': 764522},
  "Calculated atomic coordinates v'": {'x': 0, 'y':764627},
  "Calculated atomic coordinates w'": {'x': 0, 'y':546235}},
  {'Chiral indice n': 4,
    'Chiral indice m': 3,
    'Initial atomic coordinate u': {'x': 0, 'y': 435263},
    'Initial atomic coordinate v': {'x': 0, 'y': 8446272},
    'Initial atomic coordinate w': {'x': 0, 'y': 874625},
    "Calculated atomic coordinates u'": {'x': 0, 'y': 764522},
    "Calculated atomic coordinates v'": {'x': 0, 'y':764627},
    "Calculated atomic coordinates w'": {'x': 0, 'y':546235}}]

print('*** Insert Many Documents ***')
time.sleep(2)
print("Inserted Documents: ", conn.insert_many(col_document=documents, collection=coll))


# Find first document
print("*** Find First Document ***")
time.sleep(2)
print("First Document: ", conn.find_first(collection=coll))

# Find filtered document
print("*** Find Filtered Document ***")
time.sleep(2)
for document in conn.find_all(collection=coll, query = {"Chiral indice n":{'$eq': 2}}):
  print(document, "\n")

# limit document
print("*** Limit the Documents (10) ***")
time.sleep(2)
for document in conn.find_all(collection=coll).limit(10):
  print(document, "\n")

# Delete One document
print("Delete One Document ...")
time.sleep(2)
conn.delete_first(collection=coll, query = {"Chiral indice n" : 2})
print("One Document Deleted")

# Delete Many documents
print("Delete Many Documents ...")
time.sleep(2)
print(conn.delete_all(collection=coll, query = {"Chiral indice n":{'$eq': 2}}))
print("Many Documents Deleted")

# Update one document
print("Update One Document...")
time.sleep(2)
conn.update_one_document(collection= coll, present_data= {"Chiral indice n": 3}, new_data= {"$set": {"Chiral indice n": 2}} )
print("One Document Updated!")

# Update All matched document
print("Update Many Documents")
time.sleep(2)
conn.update_many_documents(collection= coll, present_data= {"Chiral indice n": 3}, new_data= {"$set": {"Chiral indice n": 2}} )
print("Many Document Updated!")

# Drop Collections
print("Drop Collection...")
time.sleep(2)
conn.drop_collection(collection=coll)
print('Collection Dropped!')