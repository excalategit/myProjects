Project Description

The project showcases a pipeline that fetches data from a Postgres database, transforms, and delivers it to BigQuery. 
The intention is to mirror the pipeline requirements involved in fetching transactional data from an OLTP database 
and preparing it for analytics and storage in an OLAP database.

The exercise showcases a more modern data pipeline design and the implications of incorporating a data warehouse 
into the pipeline.

Insights:
- BigQuery has its own UPSERT function called MERGE. It's the same as UPSERT just that it does not utilize a dataframe 
to prepare the data for comparison and loading, rather it uses the raw staging table. Therefore...
- BigQuery's expects staging to already contain cleaned/transformed data. This requires that the data is 
cleaned before loading to staging.
- Due to BiqQuery's restrictions there is more reliance on SQL proficiency to prepare the data to required standards 
e.g. while performing an UPDATE action to tables BigQuery expects the source table column used as the key to contain
unique values. SQL's window function was utilized to address this need.
- There is a need to learn and utilize new methods that allow Python to interact with BigQuery e.g. to_gbp, 
bigquery.Client, QueryJobConfig() etc.
- The requirements to load and update the audit table required an understanding of SQL's stored procedures.
- There are changes in the considerations for the data model design as BigQuery does not enforce primary keys.
- A 'loading time' component was added to the audit table.