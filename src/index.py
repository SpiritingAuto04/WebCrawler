import threading
import requests
import time
import urllib.parse
import urllib.request

from helpers import fetch, fetch_url
from bs4 import BeautifulSoup as BS
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, BulkWriteError
from conf import usr, passw, ip
from datetime import datetime, timezone
from requests.exceptions import TooManyRedirects


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

    def run(self) -> tuple[list[str], BS | None]:
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

        '''if resp.status_code != 200:
            return None'''

        soup = BS(resp.text, 'html.parser')

        for url in soup.find_all('a', href=True):
            if not url.get("href"):
                continue

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

        ent = DataBase.QUEUE.find_one_and_delete({})

        if ent is None:
            continue

        link = ent["url"]

        if DataBase.INGESTED.count_documents({"url": link}) > 0:
            continue

        response = fetch(link, DataBase.USER_AGENT)
        pageCont = response.__str__()
        statCode = response

        subDomain, topDomain, fullDomain = fetch_url(link)

        Beauti = BS(pageCont, "html.parser")
        soupEle = Beauti.find_all('a')

        pLink = [
            {"url": u["href"]}
            for u in soupEle
            if u.has_attr("href")
            if u["href"].startswith("http")
        ]

        try:
            DataBase.INGESTED.insert_one({
                "url": link,
                "status": {
                    "code": statCode[0],
                    "message": str(statCode[1])
                },
                "domains": {
                    "sub": subDomain,
                    "top": topDomain,
                    "full": fullDomain
                }
            })
        except DuplicateKeyError:
            pass

        if isinstance(statCode[0] != 200, TooManyRedirects):
            subDomain = "Unsuccessful"
            topDomain = "Unsuccessful"
            fullDomain = ""

        if len(pLink) == 0:
            continue

        try:
            DataBase.QUEUE.insert_many(pLink, ordered=False)
        except BulkWriteError:
            pass
        except TooManyRedirects:
            pass
