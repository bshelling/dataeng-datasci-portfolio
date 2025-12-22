## 2025 New Orleans Calls for Service Data Engineering and Data Science Project

Author: Brandon Shelling
Email: ["email:bshelling@gmail.com"](email)


##### Tools and Languages
- Python
- Jetbrains DataGrip
- Jupyter Lab


##### Libraries
- SQLAchemy
- Pandas
- Numpy
- Requests

#### Week 1
In Week 1, the focus was on building a reliable ingestion and transformation pipeline rather than exploratory analysis.

After selecting an API as the data source, I began by ingesting the raw data. The dataset consisted of approximately 320,000 records, which required handling pagination and partitioned API responses. As a result, the ingestion process produced 320 raw JSON files, each representing a partition of the extracted data (ingest.py).

Once ingestion was complete, the next challenge was consolidating and preparing the data for analytics. The raw JSON files were:

- Merged into a unified dataset
- Flattened to normalize nested fields
- Cleaned to address nulls, inconsistent data types, and formatting issues
- Transformed to align with a structured schema

Finally, the cleaned and transformed data was loaded into a database table using a separate transformation and load script (transformload.py), creating a stable foundation for downstream analytics and modeling.

This week established the Bronze → Silver pipeline, ensuring the data is repeatable, scalable, and ready for further analysis in subsequent weeks.
