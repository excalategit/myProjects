Project Description

The project showcases a pipeline that fetches data from a Postgres database, transforms and delivers to BigQuery. The intention is to mirror the pipeline requirements involved in fetching transactional data from an OLTP database and preparing it for analytics and storage in an OLAP database.

The exercise exposed me to a more modern design and the implications of implementing a data warehouse solution in my pipeline.

Some implications:
- BigQuery has its own UPSERT logic (MERGE) which is based on SQL. Therefore unlike the Postgres implementation where a dataframe is prepared and used as the reference for upsert, BigQuery uses the staging table as the reference. Therefore...
- This implementation expects staging to already contain cleaned/transformed data. This requires that the data is cleaned before loading to staging.
- Because UPSERT does not utilize a pandas dataframe, there is more reliance on SQL proficiency to prepare the reference table to required preference.
- Also when performing an UPDATE/MERGE process BigQuery expects the source table column being used as the key to contain unique values e.g. SQL's window function was utilized to address this need.
- There was the need to learn the new tools for interacting with BigQuery from Python such as to_gbp, bogquery.Client, QueryJobConfig() etc.
- The requirements to load and update the audit table required more investigation and understanding of SQL's stored procedures.
- Changes in the data model design considerations as BigQuery does not enforce primary keys.
- Addition of a 'loading time' component to the audit table.