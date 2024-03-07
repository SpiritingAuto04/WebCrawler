import threading

from datetime import datetime, timezone
from helpers import fetch, fetch_url
from pymongo import MongoClient
from conf import usr, passw, ip
from time import sleep
from random import randint


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
    @staticmethod
    def push_to_ingest(data: dict) -> None | Exception:
        try:
            DataBase.INGESTED.insert_one(data)
            return None
        except Exception as e:
            return e

    @staticmethod
    def push_to_queue(items: list) -> None | Exception:
        try:
            DataBase.QUEUE.insert_many(items, ordered=False)
        except Exception as e:
            return e


def crawler_thread(thread_id: int) -> None:
    mutex = DataBase.DB_MUTEX
    while True:
        mutex.acquire()

        entry = DataBase.QUEUE.find_one_and_delete({})

        if entry is None:
            mutex.release()
            print(f"Released {thread_id}")
            continue

        url = entry["url"]

        if DataBase.INGESTED.count_documents({"url": url}) > 0:
            mutex.release()
            continue

        mutex.release()
        resp = fetch(url, DataBase.USER_AGENT)

        if isinstance(resp[1], Exception):
            Crawler.push_to_ingest(data={
                "link": url,
                "status": {"code": resp[0], "message": str(resp[2])},
                "subDomain": "Undefined",
                "topDomain": "Undefined",
                "fullDomain": "Undefined",

            })
            continue

        if "html" not in str(resp[2]):
            Crawler.push_to_ingest({
                "link": url,
                "status": {"code": resp[0], "message": str(resp[2])},
                "domains": {
                    "subDomain": "Undefined",
                    "topDomain": "Undefined",
                    "fullDomain": "Undefined"
                }
            })
            continue

        linkedURLs = resp[3]
        topDomain, subDomain, fullDomain = fetch_url(url)

        Crawler.push_to_ingest({
            "url": url,
            "status": {"code": resp[0], "message": str(resp[2])},
            "domains": {
                "top": topDomain,
                "sub": subDomain,
                "full": fullDomain
            }
        })

        queued_urls = [{
            "link": fullDomain,
            "url": url,
            }
            for url in linkedURLs
        ]

        print(f"Thread <{thread_id}> <-- {url}: \nIngested {len(queued_urls)} links to QUEUE @ {datetime.now(timezone.utc)}")

        if len(queued_urls) > 0:
            Crawler.push_to_queue(queued_urls)

        sleep(randint(2, 10))


def main():
    threads = []

    for thread_id in range(0, 10):
        thread = threading.Thread(target=crawler_thread, args=(thread_id,))
        thread.start()

        threads.append(thread)

    for thread in threads:
        thread.join()


if __name__ == "__main__":
    main()
