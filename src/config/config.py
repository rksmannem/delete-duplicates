import os
from dotenv import find_dotenv, load_dotenv
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
        username = urllib.parse.quote_plus(self.user)
        password = urllib.parse.quote_plus(self.passwd) 

        uri = "mongodb://{}:{}@{}:{}/{}?retryWrites=true&w=majority".format(
            username, password, self.host, self.port, self.db)
        return uri
