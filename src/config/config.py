import os
from dotenv import find_dotenv, load_dotenv
import pprint
import urllib


class Config:
    def __init__(self):
        load_dotenv(find_dotenv())
        self.host = os.getenv("MONGO_HOST")
        self.port = os.getenv("MONGO_PORT")
        self.user = os.getenv("MONGO_USER")
        self.passwd = os.getenv("MONGO_PASS")
        self.db = os.getenv("MONGO_DB")

    def get_conn_uri(self):
        # uri = "mongodb://{}:{}@{}:{}/?authSource=admin".format(
        #     self.user, self.passwd, self.host, self.port)


        # ctp-stg-shard-00-02.qeksk.mongodb.net
        # ctp-stg.qeksk.mongodb.net
        # ctp-qa-au.qeksk.mongodb.net

        # uri = "mongodb://{}:{}@{}:{}/{}?retryWrites=true&w=majority".format(
        #     self.user, self.passwd, self.host, self.port, self.db)

        username = urllib.parse.quote_plus(self.user)
        password = urllib.parse.quote_plus(self.passwd) 
        # uri = "mongodb+srv://{}:{}@{}/{}?retryWrites=true&w=majority&authSource=admin".format(
        #     username, password, self.host, self.db)

        uri = "mongodb://{}:{}@{}:{}/{}?retryWrites=true&w=majority".format(
            username, password, self.host, self.port, self.db)

        
        # url = "mongodb+srv://rama.mannem@toyota.com:Toyota@123@ctp-stg.qeksk.mongodb.net/subscription-management?retryWrites=true&w=majority")

        return uri


# def main():
#     mcfg = Config()
#     print("Mongo_Config:{}".format(mcfg.mongo.get_conn_uri()))


# if __name__ == '__main__':
#     main()
