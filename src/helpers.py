import requests

from bs4 import BeautifulSoup as BS, Doctype as DT, Doctype
from requests.exceptions import ReadTimeout, TooManyRedirects


def fetch(url: str, user_agent: str) -> tuple[int, str] | tuple[None, Exception]:
    try:
        resp = requests.get(url, headers={
            'User-Agent': user_agent
        }, timeout=5)

        Beauti = BS(resp.text, "html.parser")
        header = [i for i in Beauti if isinstance(i, Doctype)]
        if header:
            header = header[0]
        else:
            header = "Undefined"

        return resp.status_code, header

    except ReadTimeout as RT:
        return None, RT

    except TooManyRedirects as TMR:
        return None, TMR

    except Exception as e:
        return None, e


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
