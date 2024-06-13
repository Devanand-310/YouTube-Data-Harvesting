from pymongo import MongoClient

class MongoDB_handler:
    def __init__(self, url, database_name, collection_name):
        self.client = MongoClient(url)
        self.database = self.client[database_name]
        self.collection = self.database[collection_name]
        
    def insert_doc(self,document):
        self.collection.insert_one(document)
        try:
            print('Document Uploaded Successfully')
        except errors.DuplicateKeyError:
            print("Document with this _id already exists")
        except Exception as e:
            print(f"An error occurred: {e}")
    def close_db(self):
                  self.client.close()
            
    def get_db(self):
        return self.database
    
    def get_col(self):
        return self.collection
    
    def get_documents(self, query=None):
        if query:
            return self.collection.find(query)
        else:
            return self.collection.find()

if __name__ =="__main__":
    url ='mongodb+srv://deva03102001:7871400420@cluster0.4xtn4hs.mongodb.net/'
    database_name = 'Youtube_DB'
    col_name = 'Youtube_data'
    mongo = MongoDB_handler(url,database_name,col_name)
    mongo.insert_doc(final_output_data)