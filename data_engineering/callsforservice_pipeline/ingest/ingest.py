import requests
import pandas as pd
import os

import json
import os
import time
from datetime import datetime


# ref https://data.nola.gov/Public-Safety-and-Preparedness/Calls-for-Service-2025/4xwx-sfte/about_data
assetid = os.getenv('ASSETID')
token = os.getenv('APPTOKEN')

records = 320000
pages = int(records/1000)

# Partition key
ingest_date = pd.Timestamp.utcnow().date().isoformat()

def getDataJson(page: float):
    url = f"https://data.nola.gov/api/v3/views/{assetid}/query.json?pageSize=1000&pageNumber={page}"
    r = requests.get(url,headers={
        "X-App-Token": token
        })
    return r.json()


def importData():

    os.makedirs(f"./import_data/ingest_date={ingest_date}",exist_ok=True)
    for page in range(pages):
        with open(f"./import_data/ingest_date={ingest_date}/{page}.json","w") as rdata:
            json.dump(getDataJson(page + 1),rdata,indent=4)
            print(f"Page {page} json created")
        time.sleep(2)


importData()




