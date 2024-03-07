from typing import Any
import requests

from bs4 import BeautifulSoup as BS, Doctype as DT, Doctype
from urllib.parse import urljoin


def fetch(url: str, user_agent: str) -> tuple[int, str, Doctype | str, list[str | Any]] | tuple[None, str, Exception]:
    try:
        resp = requests.get(url, headers={
            'User-Agent': user_agent
        }, timeout=5)

        Beauti = BS(resp.text, "html.parser")
        header = [i for i in Beauti if isinstance(i, DT)]
        if header:
            header = header[0]
        else:
            header = "Undefined"

        links = []

        for link in Beauti.find_all('a', href=True):
            link = link.get("href")

            if link.startswith("http"):
                links.append(link)
                continue

            if link.startswith("/"):
                links.append(urljoin(url, link))
                continue

        return resp.status_code, resp.text, header, links

    except Exception as e:
        return None, "Exception:", e


def fetch_url(url: str):
    if "://" not in url:
        return {
            "top": "Undefined",
            "sub": "Undefined"
        }

    part = url.split("://", 1)[1]
    part = part.split("/", 1)
    part = part[0].split(".")

    subDomain = part[1]
    topDomain = part[0]
    fullDomain = '.'.join(part[:-2] + part[-2:])

    return topDomain, subDomain, fullDomain