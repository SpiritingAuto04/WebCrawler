import json
import urllib.parse

with open('H:\\Programming\\Bot Folders\\LinkCrawler\\src\\config.json') as c:
    config = json.load(c)
    # passw = config['pass']
    passw = urllib.parse.quote_plus(config['pass'])
    # usr = config['user']
    usr = urllib.parse.quote_plus(config['user'])
    ip = config['ip']
