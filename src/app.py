
import pymongo
from pymongo import MongoClient, errors, ASCENDING, DESCENDING
# from pymongo.errors import ConnectionFailure
from pymongo.errors import CollectionInvalid, ConnectionFailure
from pymongo import DeleteOne

from config import config
from bson.json_util import dumps, loads
from bson import json_util
from pprint import pprint

import json
import random


class Mongo_Client:
    def __init__(self):
        self.cfg = config.Config()
        self.client = self.connect()

    def connect(self):
        url = self.cfg.get_conn_uri()
        try:
            client = MongoClient(url)
            print("server version:", client.server_info()["version"])
        except errors.ServerSelectionTimeoutError as err:
            client = None
            print("pymongo ERROR:", err)
        except ConnectionFailure as err:
            client = None
            print("Server not available", err)

        return client

    def list_databases(self):
        return self.client.list_database_names()

    def list_collections(self, db=None):
        database = self.client[db]
        return database.list_collection_names()

    def get_document_count(self, db=None, coll_name=None, options={}):
        # explicitly ignore the db: 'config', as it throws an error: pymongo.errors.OperationFailure: not authorized on config to execute command
        if db == 'config':
            return 0
        database = self.client[db]
        collection = database[coll_name]
        # docs = list(collection.find({}, options))
        total_count = collection.count_documents({})
        # print("count(documents): {} in {}".format(total_count, coll_name))
        return total_count

    def get_documents(self, db_name="", coll_name="", options={}, sort_index='_id', limit=100):
        # explicitly ignore the db: 'config', as it throws an error: pymongo.errors.OperationFailure: not authorized on config to execute command
        if db_name == 'config':
            return json.loads('' or 'null')
        database = self.client[db_name]
        collection = database[coll_name]

        # creat a Cursor instance using find() function
        all_doc = collection.find(options).sort(
            sort_index, pymongo.DESCENDING).limit(limit)

        json_doc = json.dumps(list(all_doc), default=json_util.default)
        return json.loads(str(json_doc))

    def search_document(self, db_name="", coll_name="", conditions={}):

        if db_name == 'config':
            return json.loads('' or 'null')

        database = self.client[db_name]
        collection = database[coll_name]

        single_doc = collection.find_one(conditions)
        json_doc = json.dumps(single_doc, default=json_util.default)
        return json.loads(str(json_doc))

    def create_index(self, db_name="", coll_name="", field_to_index="", order_by=ASCENDING):
        database = self.client[db_name]
        collection = database[coll_name]

        # create an index in ASCENDING order
        resp = collection.create_index(
            [(field_to_index, order_by)], unique=True)
        print("CREAT INDEX RESPONSE:{}".format(resp))
        return resp

    def create_collection(self, db_name="", coll_name="", options={}, json_file="names.json"):

        database = self.client[db_name]
        # Created or Switched to collection
        collection = database[coll_name]

        if json_file == "":
            print("CREATED AN EMPTYY COLLECTON:{}".format(coll_name))
            return collection

        # Loading or Opening the json file
        with open(json_file) as file:
            file_data = json.load(file)

        if isinstance(file_data, list):
            collection.insert_many(file_data)
        else:
            collection.insert_one(file_data)

        return collection

    def drop_collection(self, db_name="", coll_name=""):
        database = self.client[db_name]
        # Created or Switched to collection
        collection = database[coll_name]
        status = collection.drop()
        print("DROPPED COLLECTION:{} FROM DB:{}, status: {}".format(
            coll_name, db_name, status))
        # if status == True:
        #     print("DROPPED COLLECTION:{} FROM DB:{}".format(coll_name, db_name))
        # else:
        #     print("FAILED TO DROP COLLECTION:{} FROM DB:{}".format(
        #         coll_name, db_name))

    def insert_documents(self, db_name="", coll_name="", doc_str=""):

        database = self.client[db_name]
        # Created or Switched to collection
        collection = database[coll_name]

        if doc_str == "":
            print("CAN'T INSERT EMPTY DOCS LIST IN TO COLLECTON:{}".format(coll_name))
            return collection

        docs = json.loads(doc_str)
        if isinstance(docs, list):
            collection.insert_many(docs)
        else:
            collection.insert_one(docs)

        print("INSERTED IDS:{}".format(collection.inserted_ids))
        return collection

    def list_duplicates(self, db_name="", coll_name="", field_names=[]):
        database = self.client[db_name]
        # Created or Switched to collection
        collection = database[coll_name]

        fields_dict = {}
        for name in field_names:
            key = "%s" % (name)
            value = "$%s" % (name)
            fields_dict[key] = value
            # print(d)
        # print("fields_dict:{}".format(fields_dict))
        # json_object = json.dumps(fields_dict)
        # pprint(json_object)
        pipeline = [
            {
                '$group': {
                    # '_id': {'FirstName': '$FirstName'},
                    '_id': fields_dict,
                    'count': {'$sum': 1},  # ,
                    # 'ids': {'$push': "$_id"}
                }
            },
            {'$match': {'count': {'$gte': 2}}}  # ,
            # {'$sort': {'count': -1}}
        ]

        all_doc = collection.aggregate(pipeline)
        json_doc = json.dumps(list(all_doc), default=json_util.default)
        return json.loads(str(json_doc))

    def delete_duplicates(self, db_name="", coll_name="", field_names=[]):
        database = self.client[db_name]
        # Created or Switched to collection
        collection = database[coll_name]

        fields_dict = {}
        for name in field_names:
            key = "%s" % (name)
            value = "$%s" % (name)
            fields_dict[key] = value

        # json_object = json.dumps(fields_dict)
        # pprint(json_object)

        pipeline = [
            {
                '$group': {
                    # '_id': {'FirstName': '$FirstName'},
                    '_id': fields_dict,
                    'count': {'$sum': 1},
                    'ids': {'$push': '$_id'}
                }
            },
            {
                '$match': {
                    'count': {'$gte': 2}
                }
            }
        ]

        requests = []
        for document in collection.aggregate(pipeline):
            it = iter(document['ids'])
            next(it)
            for id in it:
                requests.append(DeleteOne({'_id': id}))
        result = collection.bulk_write(requests)
        pprint(result.bulk_api_result)

    def get_distinct_documents(self, db_name="", coll_name="", field_name=""):
        database = self.client[db_name]
        # Created or Switched to collection
        collection = database[coll_name]

        unique_doc = collection.distinct(field_name)
        return unique_doc

    def remove_duplicates(self, db_name="", coll_name=""):
        database = self.client[db_name]
        # Created or Switched to collection
        collection = database[coll_name]

        # 1. create an temp collection
        temp_name = coll_name + "_" + "bkup" + str(random.randint(1, 100))
        # temp_coll = self.create_collection(db_name, temp_name)

        # 2. create an index on the fields which will be unique
        field_name_to_index = input("Enter Field Name To Index: ")
        resp = self.create_index(
            db_name, temp_name, field_name_to_index, ASCENDING)
        print("CREAT INDEX:{}".format(resp))

        # 3. clone/copy docs from existing collection to new collection
        pipeline = [{'$out': temp_name}]
        src_docs = collection.aggregate(pipeline)
        pprint(src_docs)

        # 4. rename the cloned collection to actual collection
        # temp_coll.rename(temp_name)


def process_input(client=None):

    def help():
        print("============================================")
        print("""
        1. Create Collection
        2. List Documents
        3. Find Document
        4. Count Documents
        5. Exit/Quit
        6. Drop Collection
        7. List Databases
        8. List Collections
        9. List Duplicates
        10. Insert Documents
        11. Delete Duplicates
        12. Create Index
        13. Distinct Documents
        14. Remove Duplicates/with bkup
        15. help
    """)
        print("============================================")

    while True:
        help()

        ans = input("Enter Choice: ")

        if ans == "1":
            db_name = input("Enter DB Name: ")
            coll_name = input("Enter Collection Name: ")

            input_json_file = input("Enter Absolute path to input json file: ")

            print("\n CREATING COLLECTION:{} IN DB:{}".format(coll_name, db_name))
            coll = client.create_collection(
                db_name, coll_name, {}, input_json_file)

        elif ans == "2":
            db_name = input("Enter DB Name: ")
            coll_name = input("Enter Collection Name: ")

            print("\n LISTING ALL DOCUMEMNTS IN A COLLECTION:{} IN DB:{}".format(
                coll_name, db_name))
            docs = client.get_documents(db_name, coll_name, {})
            # print("{}".format(docs))
            pprint(docs)

        elif ans == "3":
            db_name = input("Enter DB Name: ")
            coll_name = input("Enter Collection Name: ")

            print("\n FINDING A DOCUMENT IN COLLECTION:{} IN DB:{}".format(
                coll_name, db_name))
            print("\n Find Document")
            doc = client.search_document(db_name, coll_name, {})
            # print("{}".format(doc))
            pprint(doc)

        elif ans == "4":
            db_name = input("Enter DB Name: ")
            coll_name = input("Enter Collection Name: ")

            print("\n COUNTING COLLECTION:{} IN DB:{}".format(coll_name, db_name))
            total_docs = client.get_document_count(db_name, coll_name, {})
            print("\n TOTAL DOCS: {}".format(total_docs))

        elif ans == "5" or ans.lower() == "q" or ans.lower() == "quit":
            print("\n Goodbye")
            exit(0)

        elif ans == "6":
            db_name = input("Enter DB Name: ")
            coll_name = input("Enter Collection Name: ")

            print("\n DROPPING COLLECTION:{} FROM DB:{}".format(coll_name, db_name))
            coll = client.drop_collection(db_name, coll_name)

        elif ans == "7":
            print("\n LISTING EXISTING DATBASES: \n")
            db_list = client.list_databases()
            print("\n AVAILABLE DATBASES: {}".format(db_list))

        elif ans == "8":
            db_name = input("Enter DB Name: ")
            print("\n Listing Collections IN DB:{}".format(db_name))
            coll_list = client.list_collections(db_name)
            print("\n AVAILABLE COLLECTIONS: {}".format(coll_list))

        elif ans == "9":
            db_name = input("Enter DB Name: ")
            coll_name = input("Enter Collection Name: ")

            print("\n LISTING DUPLICATE DOCS IN COLLECTION:{} IN DB:{}".format(
                coll_name, db_name))

            dups = client.list_duplicates(
                db_name, coll_name, ['FirstName'])
            # print("\n DUPLICATE DOCUMNETS: {}".format(dups))
            print("\n DUPLICATE DOCUMNETS:")
            pprint(dups)

        elif ans == "10":
            db_name = input("Enter DB Name: ")
            coll_name = input("Enter Collection Name: ")

            doc_str = input("Enter Documents string to insert: ")

            print("doc_str: {}".format(doc_str))
            print("\n INSERTING DOCUMENTS INTO COLLECTION:{} in DB:{}".format(
                coll_name, db_name))
            collection = client.insert_documents(db_name, coll_name, doc_str)

        elif ans == "11":
            db_name = input("Enter DB Name: ")
            coll_name = input("Enter Collection Name: ")

            print("\n REMOVING DUPLICATE DOCS FROM COLLECTION:{} IN DB:{}".format(
                coll_name, db_name))

            client.delete_duplicates(db_name, coll_name, ['FirstName'])

        elif ans == "12":
            db_name = input("Enter DB Name: ")
            coll_name = input("Enter Collection Name: ")

            print("\n CREATING INDEX IN COLLECTION:{} IN DB:{}".format(
                coll_name, db_name))

            field_name_to_index = input("Enter Field Name To Index: ")

            client.create_index(db_name, coll_name,
                                field_name_to_index, ASCENDING)

        elif ans == "13":
            db_name = input("Enter DB Name: ")
            coll_name = input("Enter Collection Name: ")

            field_name = input("Enter Field Name for Distinct Values: ")

            print("\n DISTINCT DOCUMENTS IN COLLECTION:{} IN DB:{}".format(
                coll_name, db_name))

            unique_docs = client.get_distinct_documents(
                db_name, coll_name, field_name)
            pprint(unique_docs)

        elif ans == "14":
            db_name = input("Enter Existing DB Name: ")
            coll_name = input("Enter Existing Collection Name: ")

            client.remove_duplicates(db_name, coll_name)

        elif ans == "15" or ans.lower() == "help" or ans.lower() == "h":
            continue

        else:
            print("\n Not Valid Choice Try again")


def main():
    cli = Mongo_Client()
    dblist = cli.list_databases()
    print("AVAILABLE DBs: {}".format(dblist))
    process_input(cli)


if __name__ == '__main__':
    main()
