import os
from dotenv import find_dotenv, load_dotenv
import pprint


class Config:
    def __init__(self):
        load_dotenv(find_dotenv())
        self.host = os.getenv("MONGO_HOST")
        self.port = os.getenv("MONGO_PORT")
        self.user = os.getenv("MONGO_USER")
        self.passwd = os.getenv("MONGO_PASS")
        # self.db = os.getenv("MONGO_DB")

    def get_conn_uri(self):
        uri = "mongodb://{}:{}@{}:{}/?authSource=admin".format(
            self.user, self.passwd, self.host, self.port)
        return uri


# def main():
#     mcfg = Config()
#     print("Mongo_Config:{}".format(mcfg.mongo.get_conn_uri()))


# if __name__ == '__main__':
#     main()
