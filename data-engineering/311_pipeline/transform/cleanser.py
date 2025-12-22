from datetime import datetime

import requests
import pandas as pd
import json
import logging
import os
import warnings
import time

warnings.filterwarnings(
    "ignore",
    category=FutureWarning,
    module="pandas"
)

logger = logging.getLogger(__name__)
logging.basicConfig(format="%(message)s",level=logging.DEBUG)


def normalize_data(filename: str):
    service_data = []
    with open(f'{filename}','r') as data:
        jsondata = json.load(data)
        for value in jsondata:
            service_data.append([
                jsondata.index(value),
                value.get("nopd_item"),
                value.get("type_"),
                value.get("typetext"),
                value.get("priority"),
                value.get("initialtype"),
                value.get("initialtypetext"),
                value.get("mapx"),
                value.get("mapy"),
                value.get("timecreate"),
                value.get("timedispatch"),
                value.get("timearrive"),
                value.get("timeclosed"),
                value.get("disposition"),
                value.get("dispositiontext"),
                value.get("selfinitiated"),
                value.get("beat"),
                value.get("block_address"),
                value.get("zip"),
                value.get("policedistrict"),
                value.get("location",{}).get("coordinates")[0],
                value.get("location",{}).get("coordinates")[1],
            ])
    return service_data

def createDataframe(data: list):
    newdf = pd.DataFrame(data)
    newdf.columns = ["id",
                     "nopd_item",
                     "type",
                     "typetext",
                     "priority",
                     "initialtype",
                     "initialtypetext",
                     "mapx",
                     "mapy",
                     "timecreate","timedispatch","timearrive","timeclosed","disposition","dispositiontext","selfinitiated","beat","block_address","zip","policedistrict","location_coorx","location_coory"]
    newdf.set_index("id")
    return newdf

# Zipcodes with the value of none
def transformZip(index):
    if index["zip"] == "None":
        return 0
    else:
        return index["zip"]

def modifyFieldsExport(df):
    df["zip"] = df.apply(transformZip,axis=1)
    df["mapx"] = df["mapx"].astype(int)
    df["mapy"] = df["mapy"].astype(int)
    df["beat"] = df["mapy"].astype(object)
    return df 

def treatmissingDate(df): 
    placeholder_date = "2025-01-01T00:00"
    fmt_placeholder = "%Y-%m-%dT%H:%M"
    df["timearrive"].fillna(datetime.strptime(placeholder_date,fmt_placeholder),inplace=True)
    df["timedispatch"].fillna(datetime.strptime(placeholder_date,fmt_placeholder),inplace=True)
    return df



