#coding=utf-8

from elasticsearch import Elasticsearch
import random
import csv
import io
import sys
from elasticsearch import helpers

reload(sys)
sys.setdefaultencoding('utf-8')

keywords = []

csv_reader = csv.reader(io.open('keywords.csv', encoding='utf-8'))
for row in csv_reader:
    keywords.append(row[0].encode('utf-8'))

es = Elasticsearch("localhost")
matchModes = ["Exact", "Contain", "SpitContain", "Fuzzy"]

ACTIONS = []


for i in range(0, 500000):
    open = random.randint(0, 100) % 4 == 0
    lon = random.uniform(115, 122) #安徽东部-上海
    lat = random.uniform(26, 32)  #浙江南部-上海
    for j in range(0, 50):
        id = i * 50 + j
        shop = {}
        shop["shopId"] = i
        shop["open"] = random.randint(0, 100) % 4 == 0
        shop["biding"] = keywords[random.randint(0, len(keywords) - 1)]
        shop["weight"] = random.randint(30,400)
        shop["matchMode"] = matchModes[random.randint(0, len(matchModes)) - 1]
        shop["lon"] = lon
        shop["lat"] = lat
        index = {}
        index["_index"] = "fuzzy_search_ad"
        index["_type"] = "shop_keyword"
        index["_id"] = id
        index["_source"] = shop
        ACTIONS.append(index)
        if id % 10000 == 0:
            helpers.bulk(es, actions=ACTIONS)
            print id
            ACTIONS = []

print 'done'