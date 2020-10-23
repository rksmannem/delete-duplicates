# import json
# import random
from pymongo import ASCENDING
from pprint import pprint
from client import client

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
        14. Clone Collection
        15. Get Documents with Nested Array Size Range
        16. Get Documents with the Nested Array Size
        17. Get Distinct Products in Subscriptions
        18. Update Subscriptions
        19. help
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
            # print("count after insert:{}".format())

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

            cloned_coll, status = client.clone_collection(db_name, coll_name)

            if status == True:
                print("CREATED CLONE: `{}` OF `{}`".format(
                    cloned_coll, coll_name))
            else:
                print("ERROR CREATING CLONE: `{}` OF `{}`".format(
                    cloned_coll, coll_name))

        elif ans == "15":
            db_name = input("Enter Existing DB Name: ")
            coll_name = input("Enter Existing Collection Name: ")

            min = int(input("Enter min: "))
            max = int(input("Enter max: "))

            docs = client.get_docs_with_in_range(
                db_name, coll_name, min, max)

            print("##############################################")
            pprint(docs)
            print("##############################################")

        elif ans == "16":
            db_name = input("Enter Existing DB Name: ")
            coll_name = input("Enter Existing Collection Name: ")
            sz = int(input("Enter Size: "))
            docs = client.get_docs_with_nested_array_size(
                db_name, coll_name, sz)

            print("##############################################")
            pprint(docs)
            print("##############################################")
        
        elif ans == "17":
            db_name = input("Enter Existing DB Name: ")
            coll_name = input("Enter Existing Collection Name: ")
            dups = client.get_distinct_products(db_name, coll_name)

            print("##############################################")
            pprint(dups)
            print("##############################################")
        
        elif ans == "18":
            db_name = input("Enter Existing DB Name: ")
            coll_name = input("Enter Existing Collection Name: ")
            result = client.update_subscriptions(db_name, coll_name)

            print("##############################################")
            pprint(result.bulk_api_result)
            print("##############################################")

        elif ans == "18" or ans.lower() == "help" or ans.lower() == "h":
            continue

        else:
            print("\n Not Valid Choice Try again")


def main():
    cli = client.Mongo_Client()
    dblist = cli.list_databases()
    print("AVAILABLE DBs: {}".format(dblist))
    process_input(cli)


if __name__ == '__main__':
    main()
