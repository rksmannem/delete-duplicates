import datetime
import json
import time

import pymongo
from bson import json_util
from pymongo import MongoClient, errors
from pymongo.errors import ConnectionFailure

from . import query
from config import config

# constants
# todo:- NEED TO DECIDE ON VALID NAMES FOR THE BELOW
UPDATED_SOURCE = "CTP_PES_DATA_CLEANUP"
HISTORY_ACTION = "DUPLICATE_PRODUCTS_CLEANUP"
HISTORY_DESC = "delete duplicate products in subscriptions list"


class Client:
    def __init__(self, logger):
        self.cfg = config.Config()
        self.log = logger
        self.client = self.connect()

    def connect(self):
        url = self.cfg.get_conn_uri()
        try:
            cli = MongoClient(url)
            self.log.info("server version: %s", cli.server_info()["version"])
        except errors.ServerSelectionTimeoutError as err:
            cli = None
            self.log.exception("pymongo exception: %s", err)
        except ConnectionFailure as err:
            cli = None
            self.log.exception("Server not available", err)
        except Exception as err:
            self.log.exception("unknown exception...", err)
            cli = None

        return cli

    def get_db(self, db_name):
        return self.client.get_database(db_name)

    def get_collection(self, db_name, coll_name):
        return self.get_db(db_name).get_collection(coll_name)

    def list_databases(self):
        return self.client.list_database_names()

    def list_collections(self, db=None):
        database = self.get_db(db)
        return database.list_collection_names()

    def get_document_count(self, db=None, coll_name=None, options=None):
        # explicitly ignore the db: 'config', as it throws an error: pymongo.errors.OperationFailure: not authorized
        # on config to execute command
        if options is None:
            options = {}
        if db == 'config':
            return 0
        collection = self.get_collection(db, coll_name)
        total_count = collection.count_documents(options)
        return total_count

    def get_documents(self, db_name="", coll_name="", options=None, sort_index='_id', limit=100):
        # explicitly ignore the db: 'config', as it throws an error: pymongo.errors.OperationFailure: not authorized
        # on config to execute command
        if options is None:
            options = {}
        if db_name == 'config':
            return json.loads('' or 'null')

        collection = self.get_collection(db_name, coll_name)

        # create a Cursor instance using find() function
        all_doc = collection.find(options).sort(sort_index, pymongo.DESCENDING).limit(limit)

        json_doc = json.dumps(list(all_doc), default=json_util.default)
        return json.loads(str(json_doc))

    def create_collection(self, db_name="", coll_name="", json_file="names.json"):
        collection = self.get_collection(db_name, coll_name)

        if json_file == "":
            self.log.warn("empty json file name: %s", json_file)
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
        collection = self.get_collection(db_name, coll_name)
        status = collection.drop()
        self.log.info("dropped collection %s from db %s, status: %s", coll_name, db_name, status)

    def clone_collection(self, db_name="", coll_name=""):
        collection = self.get_collection(db_name, coll_name)

        suffix = time.strftime("%m%d%Y_%H%M%S")
        temp_name = coll_name + "_" + "bkup" + "_" + suffix

        # 1. create temp collection
        temp_coll = self.create_collection(db_name, temp_name)

        # 2. clone/copy all the docs from existing collection to the new collection
        pipeline = [{'$match': {}}, {'$out': temp_name}]
        collection.aggregate(pipeline)

        success = True if collection.count_documents({}) == temp_coll.count_documents({}) else False
        return temp_coll.name, success

    # ##########################################################################
    # below code is specific to `subscriptions` collection only.
    # 1. `get_docs_with_nested_array_size`: Can Get List of documents from
    # `subscriptions` collection, Given size of the nested array(subscriptions)
    # 2. `get_docs_with_in_range`: Can Get List of documents from `subscriptions` collection,
    # Given range of lengths of the nested array( subscriptions)
    # 3. `get_distinct_products`: Can Get List of documents from `subscriptions` collection, with distinct values
    # from the nested array(subscriptions) 4. ```update_subscriptions```: update documents in `subscriptions`
    # collection, with distinct values in nested array(subscriptions)

    #############################################################################

    def get_docs_with_in_range(self, db_name="", coll_name="", start=0, end=0):

        collection = self.get_collection(db_name, coll_name)

        if start < 0:
            self.log.error("invalid value for start:{0}".format(start))
            return

        if end <= 0:
            self.log.error("invalid value for end:{0}".format(end))
            return

        if start >= end:
            self.log.error("invalid range:({0}-{1}],retry...".format(start, end))
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
                        '$gt': start,
                        '$lte': end
                    }
                }
            }
        ]

        cursor = collection.aggregate(pipeline)
        json_doc = json.dumps(list(cursor), default=json_util.default)
        return json.loads(str(json_doc))

    def get_docs_with_nested_array_size(self, db_name="", coll_name="", sz=0):
        collection = self.get_collection(db_name, coll_name)

        cursor = collection.find({'subscriptions': {'$exists': True, '$size': sz}})
        json_doc = json.dumps(list(cursor), default=json_util.default)
        return json.loads(str(json_doc))

    def get_distinct_products(self, db_name="", coll_name="", sz=5):
        collection = self.get_collection(db_name, coll_name)

        index_pos = sz - 1
        if index_pos < 0:
            self.log.error("invalid size, size can't be a negative/zero:{}".format(sz))
            return "invalid size: {0}".format(sz)

        pipeline = query.get_distinct_subscriptions_aggr_pipeline(index_pos)
        cursor = collection.aggregate(pipeline)
        json_doc = json.dumps(list(cursor), default=json_util.default)

        out_file = db_name + "_" + coll_name + "_" + "data" + "_" + str(sz) + ".json"
        with open(out_file, 'w', encoding='utf-8') as f:
            f.write(json_doc)

        self.log.info("output in file named : {0}".format(out_file))
        return json.loads(str(json_doc))

    def update_subscriptions(self, db_name="", coll_name="", sz=5) -> (object, object):
        database = self.get_db(db_name)
        collection = self.get_collection(db_name, coll_name)

        index_pos = sz - 1
        if index_pos < 0:
            self.log.error("invalid size, size can't be a negative:{0}".format(sz))
            return {}, {}

        pipeline = query.get_distinct_subscriptions_aggr_pipeline(index_pos)

        # append additional aggregates to fetch only unique subscriptions
        # for those documents with duplicates
        # pipeline.extend(
        #     [
        #         {
        #             "$addFields": {
        #                 "subscriptions": "$unique_subscriptions"
        #             }
        #         },
        #         {
        #             "$unset": ["duplicate_subscriptions", "unique_subscriptions"]
        #         }
        #     ]
        # )
        cursor = collection.aggregate(pipeline)

        update_requests, history_requests = [], []
        # update each doc by vin and replace `subscriptions` list
        for doc in cursor:
            update_requests.append(
                pymongo.UpdateOne(
                    {
                        '_id': doc['_id'],
                        'vin': doc['vin'],
                    },
                    {
                        "$set": {
                            'subscriptions': doc['unique_subscriptions'],
                            'updateDate': datetime.datetime.utcnow(),
                            'updateSource': UPDATED_SOURCE
                        }
                    }
                )
            )

            # _id:5da797768cf437000accd684
            # vin:"5TDDW5G12GS127734"
            # subscriberGuid:"12ff26e93c1d4b30ba7440be083a0cd6"
            # createDate:"2019-10-16T22:19:34.794+0000"
            # action:"CANCEL"
            # description:"vehicleStatus SOLD"
            # subscription:Object
            history_requests.append(
                pymongo.UpdateOne(
                    {
                        '_id': doc['_id'],
                        'vin': doc['vin'],
                        'action': HISTORY_ACTION,
                        'description': HISTORY_DESC
                    },
                    {
                        "$set": {
                            '_id': doc['_id'],
                            'vin': doc['vin'],
                            'subscriberGuid': doc['subscriberGuid'],
                            'createDate': datetime.datetime.utcnow(),
                            'action': HISTORY_ACTION,
                            'description': HISTORY_DESC,
                            'subscription': doc
                        }
                    },
                    upsert=True
                )
            )

        if len(update_requests) == 0:
            self.log.warn("no update requests for bulk_write: %s", update_requests)
            return {}, {}

        if len(history_requests) != len(update_requests):
            self.log.warn("incorrect request count, update_requests: %s, history_requests: %s",
                          update_requests, history_requests)
            return {}, {}

        try:
            update_res = collection.bulk_write(requests=update_requests, ordered=False)  # session=update_session)
            res1: object = update_res.bulk_api_result
        except pymongo.errors.BulkWriteError as bwe:
            self.log.exception("bulk write error: {0}".format(bwe.details))
            raise
        except Exception as err:
            self.log.exception("bulk update error: {0}".format(err))
            raise

        # upsert each updated document in to history
        try:
            hist_coll_name = coll_name + "_" + "history"
            history_res = database.get_collection(hist_coll_name).bulk_write(requests=history_requests, ordered=False)
            res2: object = history_res.bulk_api_result
        except (pymongo.errors.BulkWriteError, pymongo.errors.DuplicateKeyError) as bwe:
            self.log.exception("bulk write error: {0}".format(bwe.details))
            raise
        except Exception as err:
            self.log.exception("bulk upsert error: {0}".format(err))
            raise

        return res1, res2

    def remove_extra_field_in_hist(self, db_name="", coll_name=""):
        collection = self.get_collection(db_name, coll_name)
        resp = collection.update(
            {},
            {
                "$unset": {"subscription.unique_subscriptions": 1}
            },
            multi=True
        )
        return resp

    def get_max_array_size(self, db_name="", coll_name=""):
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
                '$group': {
                    '_id': None,
                    'max_size': {
                        '$max': '$subscriptions_count'
                    }
                }
            }
        ]

        collection = self.get_collection(db_name, coll_name)
        cursor = collection.aggregate(pipeline)

        max_size = 0
        for doc in cursor:
            self.log.info("max size: %s", doc['max_size'])
            max_size = doc['max_size']

        return max_size
