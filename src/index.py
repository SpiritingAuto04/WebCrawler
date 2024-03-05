import threading
import requests
import time
import urllib.parse
import urllib.request

from bs4 import BeautifulSoup as BS, BeautifulSoup
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from conf import usr, passw, ip
from datetime import datetime, timezone


class DataBase:
    USER_AGENT = "Recluse/v1 - Crawler Agent"
    DATABASE_URL = 'mongodb://{}:{}@{}:27015/'.format(usr, passw, ip)

    DB = MongoClient(DATABASE_URL)

    INGESTED = DB["crawler"]["ingest"]
    QUEUE = DB["crawler"]["queue"]

    INGESTED.create_index("url", unique=True)
    QUEUE.create_index("url", unique=True)

    DB_MUTEX = threading.Lock()


class Crawler:
    class EndOfWorkload(Exception):
        pass

    def __init__(self, start: list[str], domain: str | None = None):
        self.__working = start

        self.discovered = []

        self.__locked_domain = domain

    def run(self) -> tuple[list[str], BeautifulSoup | None]:
        if len(self.__working) == 0:
            raise self.EndOfWorkload("No more work found.")

        current = self.__get_next()

        return current, self.__scrape(current)

    def __get_next(self) -> list[str]:
        c = self.__working[0]
        self.__working.pop(0)
        return c

    def __scrape(self, i: str) -> BS | None:
        try:
            resp = requests.get(i)
        except Exception:
            return None

        if resp.status_code != 200:
            return None

        soup = BS(resp.text, 'html.parser')

        for url in soup.find_all('a', href=True):
            '''if not url.get("href"):
                continue'''

            u = urllib.parse.urljoin(resp.url, url.get("href"))

            if self.__locked_domain and self.__locked_domain not in urllib.parse.urlparse(resp.url).netloc:
                continue

            if u in self.discovered:
                continue

            self.__working.append(u)

        return soup


if __name__ == '__main__':
    cr = Crawler(["https://google.com"])

    while True:
        url, soup = cr.run()

        '''if not soup:
            continue'''

        print(f"{datetime.now(timezone.utc)} <- {url}")

        p = {
            "url": url,
            "status": {
                "online": True,
                "error": "None"
            }
        }

        try:
            DataBase.QUEUE.insert_one(p)
        except DuplicateKeyError:
            pass

        time.sleep(1)

        '''for u in DataBase.INGESTED.find():
            if not DataBase.INGESTED:
                try:
                    DataBase.INGESTED.insert_one(p)
                except DuplicateKeyError:
                    pass'''
