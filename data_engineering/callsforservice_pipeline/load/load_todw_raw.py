from datetime import datetime
from callsforservice_pipeline.transform.cleanser import normalize_data, createDataframe, modifyFieldsExport, treatmissingDate

import requests
import pandas as pd
import json
import logging
import os
import warnings
import time
from datetime import datetime
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

ingest_date = pd.Timestamp.utcnow().date().isoformat()
import_date = "2025-12-29"




try: 
    pagindf = []

    for page in range(pages):
        flat_data = normalize_data(f"./import_data/ingest_date={ingest_date}/{page}.json")
        newdf = createDataframe(flat_data)
        modifieddf = modifyFieldsExport(newdf)
        finaldf = treatmissingDate(modifieddf)
        finaldf['ingest_date'] = ingest_date 

        pagindf.append(finaldf)

        if len(pagindf) == 10:
            pd.concat(pagindf).to_parquet(
                    "./output/311_files/",
                    engine="pyarrow",
                    partition_cols=["ingest_date"],
                    index=False
            ) 
            logger.info(f"Data cleaned and saved - file cleaned_{page}")
            pagindf.clear()
        #time.sleep(2)
except Exception as err:
    logger.error(err)

