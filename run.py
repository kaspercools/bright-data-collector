#!/usr/bin/python3
import os
import re
import json
import requests
import time
import os

BASE_DIR = './lexicons'
TARGET_FOLDER = './resultsets/'

API_TOKEN = os.getenv('BD_API_TOKEN')
API_BASE_URL = 'https://api.brightdata.com/dca/'
RESULT_COUNT = 100
WAIT_FOR_N = 60*60*12

collection_id = os.getenv('BD_collection_id')

url = API_BASE_URL + 'trigger?collector='+ collection_id
headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + API_TOKEN}

lexicons = ['derogatory','watchwords','threatening', 'discriminatory']
keywords = []

def normaliseKeyword(subject):
    res = bool(re.search(r"\s", subject))
    data = []

    if res:
        data.append(subject.replace(' ', ''))
        data.append(subject.replace(' ', '_'))
    else:
       data.append(subject)
    return data


def processJsonFiles(files, BASE_DIR):
    for file in files:
        with open(os.path.join(BASE_DIR, file), 'r') as currentDataFile:
            currentDataEntry = currentDataFile.read()
            keywordObj = json.loads(currentDataEntry)
            keywords.extend(normaliseKeyword(keywordObj['term']))


def runCode():
    for d in lexicons:
        newBase = os.path.join(BASE_DIR, d)
        files = os.listdir(newBase)
        processJsonFiles(files, newBase)

    # process keywords
    data = []

    for keyword in keywords:
        data.append({'hashtag': keyword, 'max_video_count': RESULT_COUNT})

    r = requests.post(url, data=json.dumps(data), headers=headers)
    response = json.loads(r.content)
    data = json.loads(r.content)
    print('fetched data')

    # save code
    if('collection_id' in data):
        print('waiting for completion')
        collection_id = data['collection_id']
        # halt for n minutes, give the collector time to finish
        time.sleep(WAIT_FOR_N)
        r = requests.get(API_BASE_URL + 'dataset?id='+collection_id, headers=headers)
        data = json.loads(r.content)

        while 'status' in data and data['status']=='collecting':
            time.sleep(5*60)
            r = requests.get(API_BASE_URL + 'dataset?id='+collection_id, headers=headers)
            data = json.loads(r.content)
            print(data)

        time.sleep(20)
        print('completed.. downloading and writing file')

        with open(os.path.join(TARGET_FOLDER, collection_id+".json", "w") as f:
            f.write(json.dumps(data, sort_keys=True))

while(True):
    print('execute job')
    runCode()
