from pprint import pprint
from client import client
from log import logger
import os
import pprint

LOG_FILE_NAME_PREFIX = "app"


def process_input(cli=None, log=None):
    def usage():

        print("============================================")
        print("""
        1. Create Collection
        2. List Documents
        3. Count Documents
        4. List Databases
        5. List Collections
        6. Drop Collection
        15. Get Documents with Nested Array Size Range
        16. Get Documents with the Nested Array Size
        17. Get Distinct Products in Subscriptions
        18. Update Subscriptions
        19. help
        20. Quit
    """)
        print("============================================")

    if cli is None:
        log.critical("Invalid connection to MongoDB, client:{}".format(cli))
        return

    def read_db_collection_names():
        db = input("Enter DB Name: ")
        col = input("Enter Collection Name: ")
        return db, col

    while True:
        usage()

        ans = input("Enter Choice: ")
        if ans == "1":
            db_name, coll_name = read_db_collection_names()
            input_json_file = input("Enter Absolute path to input json file: ")
            coll = cli.create_collection(db_name, coll_name, input_json_file)
            log.info("created collection:%s In DB: %s", coll, db_name)

        elif ans == "2":
            db_name, coll_name = read_db_collection_names()
            docs = cli.get_documents(db_name, coll_name, {})
            pprint(docs)
            # log.info("docs: %s", docs)

        elif ans == "3":
            db_name, coll_name = read_db_collection_names()
            total_docs = cli.get_document_count(db_name, coll_name, {})
            log.info("count: %s", total_docs)

        elif ans == "4":
            db_list = cli.list_databases()
            log.info("available dbs: %s", db_list)

        elif ans == "5":
            db_name = input("Enter DB Name: ")
            coll_list = cli.list_collections(db_name)
            log.info("%s: %s", db_name, coll_list)

        elif ans == "6":
            db_name, coll_name = read_db_collection_names()
            cli.drop_collection(db_name, coll_name)

        elif ans == "15":
            db_name, coll_name = read_db_collection_names()
            try:
                start = int(input("Enter min: "))
                end = int(input("Enter max: "))
                docs = cli.get_docs_with_in_range(db_name, coll_name, start, end)
            except ValueError as err:
                log.exception("error in get_docs_with_in_range:{0}".format(err))
            except Exception as err:
                log.exception("Unexpected error:{0}".format(err))

        elif ans == "16":
            db_name, coll_name = read_db_collection_names()

            try:
                sz = int(input("Enter Size: "))
                docs = cli.get_docs_with_nested_array_size(db_name, coll_name, sz)
            except ValueError as err:
                log.exception("error in get_docs_with_nested_array_size:{0}".format(err))
            except Exception as err:
                log.exception("Unexpected error:{0}".format(err))

        elif ans == "17":
            db_name, coll_name = read_db_collection_names()
            try:
                sz = int(input("Enter the Size of subscriptions array to start from: "))
                dup_products = cli.get_distinct_products(db_name, coll_name, sz)
                pprint.pprint(dup_products)
                log.info("duplicate products: %s", dup_products)
            except ValueError as err:
                log.exception("error getting distinct products:{0}".format(err))
            except Exception as err:
                log.exception("Unexpected error:{0}".format(err))

        elif ans == "18":
            db_name, coll_name = read_db_collection_names()

            try:
                sz = int(input("Enter the Size of subscriptions array to start from: "))
                update_res, insert_res = cli.update_subscriptions(db_name, coll_name, sz)
                log.info("update_results: %s, history_results: %s", update_res, insert_res)
            except ValueError as err:
                log.exception("error getting deleting duplicate products:{0}".format(err))
            except Exception as err:
                log.exception("Unexpected error:{0}".format(err))

        elif ans == "19":
            db_name, coll_name = read_db_collection_names()
            try:
                max_size = cli.get_max_array_size(db_name, coll_name)
                log.info("max_size: %s", max_size)
            except ValueError as err:
                log.exception("error in get_max_array_size:{0}".format(err))
            except Exception as err:
                log.exception("Unexpected error:{0}".format(err))

        elif ans == "20" or ans.lower() == "help" or ans.lower() == "h":
            continue

        elif ans == "21" or ans.lower() == "q" or ans.lower() == "quit":
            print("\n Goodbye")
            exit(0)

        else:
            print("\n Not Valid Choice Try again")


def clean_duplicate_products(cli=None, log=None, db='subscription_management', coll='subscription'):
    if cli is None:
        raise ValueError("invalid db client")

    if log is None:
        print("invalid logger: {0}".format(log))
        raise ValueError("invalid logger")

    max_sz = cli.get_max_array_size(db, coll)
    for sz in range(5, max_sz+1):
        update_res, insert_res = cli.update_subscriptions(db, coll, sz)
        log.info("update_results: %s, history_results: %s for subscriptions size: %s", update_res, insert_res, sz)


def main():
    log = logger.LoggerType(LOG_FILE_NAME_PREFIX).get_logger()

    cli = client.Client(log)
    if cli.client is None:
        log.critical("error connecting to mongodb: %s", cli.cfg.get_conn_uri())
        return

    db_list = cli.list_databases()
    log.info("AVAILABLE DBs: %s", db_list)

    run_as_console_app = os.getenv("CONSOLE_APP", "false")
    log.info("is running as a console app: %s", run_as_console_app)
    if run_as_console_app == 'true':
        process_input(cli, log)
        return

    if run_as_console_app == 'false':
        try:
            db_name = "users"
            coll_name = "subscription"
            clean_duplicate_products(cli=cli, log=log, db=db_name, coll=coll_name)
        except ValueError as err:
            log.exception("error cleaning duplicate products:{0}".format(err))
        except Exception as err:
            log.exception("Unexpected error:{0}".format(err))


if __name__ == '__main__':
    main()
