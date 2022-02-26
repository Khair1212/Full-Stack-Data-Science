import logging
import pandas as pd
import pymongo
from logger import MyLogger


class MongoDB:

    logging = MyLogger.log_file("test2.log")

    def __init__(self, connectionString):
        """"
        Initializes the MongoDB Connection

        Arguments:
            connectionString: ConnectionString of the MongoDB Cluster
        """
        try:
            self.client = pymongo.MongoClient(connectionString)
            self.logging.info('Connection Successful!')
        except Exception as e:
            self.logging.error("Failed to Connect")
            self.logging.exception("Exception occured " + str(e))

    def get_client(self):
        return self.client
    #def logger(self, filename):
     #   logging.basicConfig(filename=filename, level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    # initiate log
    #logger('test.log')

    def read_data(self, filename):
        """"
        Read ';' separated data from the local system and return a dataframe

        Arguments:
            filename: name of the file

        Returns:
            DataFrame
        """
        try:
            self.logging.info('Reading data from local system...')
            df = pd.read_csv(filename, sep=";")
            return df
        except Exception as e:
            self.logging.error('Failed reading data from local system')
            self.logging.exception("Exception occured " + str(e))

    def create_database(self, db_name, client):
        """"
        Create database with the specific name if not duplicate

        Arguments:
            db_name: name of the database
            client:  mysql connected client

        Returns:
            database object
        """
        if db_name in client.list_database_names():
            self.logging.info(db_name + 'Database is already there!')

        try:
            db = client[db_name]
            self.logging.info('Succesfully created the database!')
            return db
        except Exception as e:
            self.logging.error("Failed to Create the database!")
            self.logging.exception("Exception occured " + str(e))

    def create_collection(self, coll_name, database):
        """"
        Create collection with the specific name if not duplicate

        Arguments:
            coll_name: name of the collection
            database:  database object

        Returns:
            collection object
        """
        if coll_name in database.list_collection_names():
            self.logging.info(coll_name + ' Collection is already there!')
        try:
            coll = database[coll_name]
            self.logging.info('Successfully created the collection!')
            return coll
        except Exception as e:
            self.logging.error("Failed to Create the Collection!")
            self.logging.exception("Exception occured " + str(e))

    def df_to_dict(self, dataframe):
        """"
        Convert Multilevel dataframe into dictionary. Comma (,) separated coordinate column's values are formatted into X, Y

        Arguments:
            dataframe: dataframe as the input file

        Returns:
            List of dictionary object
        """
        columns = dataframe.columns.to_list()
        dictionary_list = []

        try:
            for i in dataframe.itertuples(index=False, name=None):
                li = list(i)
                diction = {}
                try:
                    for idx, j in enumerate(li):
                        if (type(j) == str) and (len(j.split(',')) > 1):
                            x = j.split(',')
                            di = {'x': x[0], 'y': x[1]}
                            li[idx] = di
                            diction[columns[idx]] = li[idx]
                        else:
                            diction[columns[idx]] = li[idx]

                    dictionary_list.append(diction)

                except Exception as e:
                    self.logging.error("Failed to convert the tuple object into dictionary!")
                    self.logging.exception("Exception Occured " + str(e))

            return dictionary_list

        except Exception as e:
            self.logging.error("Failed to convert the rows into dictionary document!")
            self.logging.exception('Exception occured ', str(e))

    def insert_bulk(self, dataframe, collection):
        """"
        Insert dataframe data to MongoDB database

        Arguments:
            dataframe: dataframe to be inserted
            collection: mongodb collection object

        Returns:
            None
        """
        try:
            collection.insert_many(self.df_to_dict(dataframe))
            self.logging.info("Bulk insertion successful!")
        except Exception as e:
            self.logging.error("Bulk insertion Failed!")
            self.logging.exception("Exception occured ", str(e))

    def insert_one(self, document, collection):
        """"
        Insert one document into the collection

        Arguments:
            document: dictionary document object to be inserted
            collection:  collection object

        Returns:
            inserted document
        """
        try:
            collection.insert_one(document)
            self.logging.info(" insertion successful!")
            return document
        except Exception as e:
            self.logging.error(" insertion Failed!")
            self.logging.exception("Exception occured ", str(e))

    def insert_many(self, col_document, collection):
        """"
       Insert many documents into the collection

       Arguments:
           col_document: List of  dictionary document object to be inserted
           coll:  collection object

       Returns:
           inserted Documents
       """
        try:
            collection.insert_many(col_document)
            self.logging.info("All data insertion successful!")
            return col_document
        except Exception as e:
            self.logging.error("Data insertion Failed!")
            self.logging.exception("Exception occured ", str(e))

    def find_first(self, collection):
        """"
        Find the first element of the collection

        Arguments:
           collection:  collection object

        Returns:
           document object
        """
        try:
            x = collection.find_one()
            self.logging.info("Found the first document!")
            return x
        except Exception as e:
            self.logging.error("Failed to find the first document!")
            self.logging.exception("Exception occured ", str(e))

    def find_all(self, collection, query=None):

        """"
        Find many elements of the collection with or without filter.

        Arguments:
            collection:  collection object
            query: Filtering approach, query takes dictionary object.

        Returns:
           list of document objects
        """
        try:
            if query == None:
                x = collection.find()
                self.logging.info("Fetched all the documents")
                return x
            else:
                x = collection.find(query)
                self.logging.info("Fetched the filtered documents")
                return x

        except Exception as e:
            self.logging.error("Failed to find the first document!")
            self.logging.exception("Exception occured ", str(e))

    def limit(self, documents, n):
        """"
        Limit the documents according to your interest

        Arguments:
           documents:  documents object
           n: limit size (1,2....n)

        Returns:
           n documents
        """
        try:
            return documents.limit(n)
            self.logging.info('Fetched only first ' + n + ' documents')
        except Exception as e:
            self.logging.error("Failed to limit the documents!")
            self.logging.exception("Exception occured ", str(e))

    def delete_first(self, collection, query):
        """"
        Delete the first element of the collection

        Arguments:
           collections:  collections object
           query: filter to delete
        Returns:
           Deleted document
        """
        try:
            x = collection.delete_one(query)
            self.logging.info("Delete the first document!")
            return x
        except Exception as e:
            self.logging.error("Failed to delete the first document!")
            self.logging.exception("Exception occured ", str(e))

    def delete_all(self, collection, query):
        """"
        Delete elements of the collection by filtering

        Arguments:
           collection:  collections object
           query:Filtering approach, query takes dictionary object.

        Returns:
           Deleted documents
        """
        try:
            x = collection.delete_many(query)
            self.logging.info("Deleted the filtered documents")
            return x
        except Exception as e:
            self.logging.error("Failed to find the first document!")
            self.logging.exception("Exception occured ", str(e))

    def drop_collection(self, collection):
        """"
        Drop the collection

        Arguments:
           collection:  collections object

        Returns:
           None
        """
        try:
            collection.drop()
            self.logging.info("Collection dropped!")
        except Exception as e:
            self.logging.error("Failed to drop the collection")
            self.logging.exception("Exception occured ", str(e))

    def update_one_document(self, collection, present_data, new_data):
        """"
        Update first document of the collection

        Arguments:
            collection: Collection object
           present_data:  present data, takes dictionary format data
           new_data: new data to insert, takes dictionary format data

        Returns:
           None
        """
        try:
            collection.update_one(present_data, new_data)
            self.logging.info("Successfully updated!")
        except Exception as e:
            self.logging.error("Failed to update the collection")
            self.logging.exception("Exception occured ", str(e))

    def update_many_documents(self, collection, present_data, new_data):
        """"
        Update  documents of the collection

        Arguments:
            collection: Collection object
            present_data:  present data, takes dictionary format data
            new_data: new data to insert, takes dictionary format data

        Returns:
          None
        """
        try:
            collection.update_many(present_data, new_data)
            self.logging.info("Successfully updated!")
        except Exception as e:
            self.logging.error("Failed to update the collection")
            self.logging.exception("Exception occured ", str(e))
