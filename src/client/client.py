import pymongo
from pymongo import MongoClient, errors, ASCENDING, DESCENDING
from pymongo.errors import CollectionInvalid, ConnectionFailure
from pymongo import DeleteOne

from bson.json_util import dumps, loads
from bson import json_util
from pprint import pprint
from config import config
import json
import random
import string


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

    def clone_collection(self, db_name="", coll_name=""):

        database = self.client[db_name]
        collection = database[coll_name]

        letters = string.ascii_lowercase
        suffix = ''.join(random.choice(letters) for i in range(5))
        temp_name = coll_name + "_" + "bkup" + "_" + suffix

        # 1. create temp collection
        temp_coll = self.create_collection(db_name, temp_name)

        # 2. clone/copy all the docs from existing collection to the new collection
        pipeline = [{'$match': {}}, {'$out': temp_name}]

        all_doc = collection.aggregate(pipeline)
        json_doc = json.dumps(list(all_doc), default=json_util.default)

        success = True if collection.count_documents(
            {}) == temp_coll.count_documents({}) else False

        return temp_coll.name, success

###########################################################################
# below code is specific to `subscriptions` collection only.
# 1. ````get_docs_with_nested_array_size```: Can Get List of documents from `subscriptions` collection, Given size of the nested array(subscriptions)
# 2. ````get_docs_with_in_range```: Can Get List of documents from `subscriptions` collection, Given range of lengths of the nested array(subscriptions)
# 3. ````get_distinct_products```: Can Get List of documents from `subscriptions` collection, with distinct values from the nested array(subscriptions)
# 4. ```update_subscriptions```: update documents in `subscriptions` collection, with distinct values in nested array(subscriptions)

#############################################################################

    def get_docs_with_in_range(self, db_name="", coll_name="", min=0, max=0):

        database = self.client[db_name]
        collection = database[coll_name]

        if min < 0:
            print("Invalid value for Min:{}".format(min))
            return

        if max <= 0:
            print("Invalid value for Min:{}".format(min))
            return

        if min >= max:
            print("Invalid range:({}-{}], Retry...".format(min, max))
            return

        pipeline = [
            {
                '$match': {'subscriptions': {'$elemMatch': {'$exists': True}}}
            },
            {
                '$addFields': {
                    'subscriptions_count': {'$size': '$subscriptions'}
                }
            },
            {
                '$match': {
                    'subscriptions_count': {
                        '$gt': min,
                        '$lte': max
                    }
                }
            }
        ]

        cursor = collection.aggregate(pipeline)
        json_doc = json.dumps(list(cursor), default=json_util.default)
        return json.loads(str(json_doc))

    def get_docs_with_nested_array_size(self, db_name="", coll_name="", sz=0):

        database = self.client[db_name]
        collection = database[coll_name]

        query = {
            'subscriptions': {'$exists': True, '$size': sz}
        }

        cursor = collection.find(query)
        json_doc = json.dumps(list(cursor), default=json_util.default)
        return json.loads(str(json_doc))

    def get_distinct_products(self, db_name="", coll_name="", sz=5):
        database = self.client[db_name]
        collection = database[coll_name]

        index_pos = sz - 1
        if index_pos < 0:
            print("Invalid size, size can't be a negative/zero:{}".format(sz))
            return "Invalid size: {}".format(sz)

        # match/get all docs with subscriptions size >= input size(sz)
        # filter_by_size_stage = {
        #     "$match": {"subscriptions" + "." + str(index_pos) : {"$exists": True}}
        # }

        # match/get all docs with subscriptions size == input size(sz)
        filter_by_size_stage = {
            '$match': {'subscriptions': {'$exists': True, '$size': sz}}
        }

        pipeline = [
            filter_by_size_stage,
            {
                u"$addFields": {
                    u"subscriptions": {
                        u"$reduce": {
                            u"input": u"$subscriptions",
                            u"initialValue": [],
                            u"in": {
                                u"$concatArrays": [
                                    u"$$value",
                                    {
                                        u"$cond": [
                                            {
                                                u"$and": [
                                                    {
                                                        u"$in": [
                                                            u"$$this.productCode",
                                                            u"$$value.productCode"
                                                        ]
                                                    },
                                                    {
                                                        u"$in": [
                                                            u"$$this.status",
                                                            u"$$value.status"
                                                        ]
                                                    },
                                                    {
                                                        u"$in": [
                                                            u"$$this.type",
                                                            u"$$value.type"
                                                        ]
                                                    }
                                                ]
                                            },
                                            [],
                                            [
                                                u"$$this"
                                            ]
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                }
            }
        ]

        cursor = collection.aggregate(pipeline)
        json_doc = json.dumps(list(cursor), default=json_util.default)
        # pprint(json.loads(str(json_doc)))
        return json.loads(str(json_doc))
    
    def update_subscriptions(self, db_name="", coll_name="", sz=5):
        database = self.client[db_name]
        collection = database[coll_name]

        index_pos = sz - 1
        if index_pos < 0:
            print("Invalid size, size can't be a negative/zero:{}".format(sz))
            return "Invalid size: {}".format(sz)

        # match/get all docs with subscriptions size >= input size(sz)
        # filter_by_size_stage = {
        #     "$match": {"subscriptions" + "." + str(index_pos) : {"$exists": True}}
        # }

        # match/get all docs with subscriptions size == input size(sz)
        filter_by_size_stage = {
            '$match': {'subscriptions': {'$exists': True, '$size': sz}}
        }

        pipeline = [
            # {
            #     "$match": {"subscriptions.4": {"$exists": True}}
            # },
            filter_by_size_stage,
            {
                u"$addFields": {
                    u"subscriptions": {
                        u"$reduce": {
                            u"input": u"$subscriptions",
                            u"initialValue": [],
                            u"in": {
                                u"$concatArrays": [
                                    u"$$value",
                                    {
                                        u"$cond": [
                                            {
                                                u"$and": [
                                                    {
                                                        u"$in": [
                                                            u"$$this.productCode",
                                                            u"$$value.productCode"
                                                        ]
                                                    },
                                                    {
                                                        u"$in": [
                                                            u"$$this.status",
                                                            u"$$value.status"
                                                        ]
                                                    },
                                                    {
                                                        u"$in": [
                                                            u"$$this.type",
                                                            u"$$value.type"
                                                        ]
                                                    }
                                                ]
                                            },
                                            [],
                                            [
                                                u"$$this"
                                            ]
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                }
            }
        ]

        cursor = collection.aggregate(pipeline)
        requests = []
        # newValues = {"$set": {"subscriptions": "Canyon 123"}}

        # update each doc by vin and replace `subscriptions` list
        for doc in cursor:
            requests.append(
                pymongo.UpdateOne(
                    # query
                    {'vin': doc['vin']},
                    # set
                    {
                        "$set": {'subscriptions': doc['subscriptions']}
                    }
                )
            )

        if len(requests) == 0:
            print("\n ***** NO OPERATIONS TO REQUEST BULK WRITE *****")
            return "WARN: NO REQUESTS TO BULK WRITE"

        result = collection.bulk_write(requests)
        # pprint(result.bulk_api_result)
        # print("********END*************")
        return result.bulk_api_result
