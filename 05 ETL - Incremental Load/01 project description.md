Project Description

The project covers the initial work of designing a data model and loading data to it, but additionally 
includes the daily utilization of the data pipeline with incremental load, and its implications. The exercise provides 
a practical insight into the concept of incremental load and how the process is audited using audit tables.

Insights gathered:

- There is the need for separate scripts for initial full load and subsequent incremental load executions. 
- There are data transfer requirements from a source system such as information on modified_date and last_load_date. 
- There are also requirements from the receiving staging table such as information on data created date, for audit purposes. 
- Clearer practical understanding of UPSERT (DO UPDATE SET vs. DO NOTHING), when and where they are needed, and why.  
- The staging table is the repository for historical data, while the fact and dimension tables are used for 
collecting unique data.
- The importance of an audit table.
- An appreciation of efficiency and storage considerations in the process.
- A better feel for the daily activities of a data engineer.