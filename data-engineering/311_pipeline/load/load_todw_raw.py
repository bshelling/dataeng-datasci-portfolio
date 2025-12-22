from datetime import datetime
from load.cleanser import normalize_data, createDataframe, modifyFieldsExport, treatmissingDate

import requests
import pandas as pd
import json
import logging
import os
import warnings
import time
import sqlalchemy

connection_str = "postgresql+psycopg2://postgres:adminpwd@127.0.0.1:5436/datadb"
db_engine = sqlalchemy.create_engine(connection_str,echo=False)

warnings.filterwarnings(
    "ignore",
    category=FutureWarning,
    module="pandas"
)

logger = logging.getLogger(__name__)
logging.basicConfig(format="%(message)s",level=logging.DEBUG)


records = 320000
pages = int(records/1000)


try: 
    for page in range(pages):
        flat_data = normalize_data(f"./import_data/{page+1}.json")
        newdf = createDataframe(flat_data)
        modifieddf = modifyFieldsExport(newdf)
        finaldf = treatmissingDate(modifieddf)
        finaldf.to_sql(
            name="raw_call_for_service",
            con=db_engine,
            if_exists="append"
        )
        logger.info(f"Data cleaned and saved - file cleaned_0{page}")

        time.sleep(2)

except Exception as err:
    logger.error(err)

