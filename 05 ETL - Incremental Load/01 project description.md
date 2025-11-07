Project Description

The project covers the initial work of designing a data model and loading data to it (as usual) but additionally includes the daily utilization of the data pipeline with incremental load, and its implications. The exercise provides a practical insight into the concept of incremental load for the first time. This exercise also introduces auditing into the design.

Some of the insights gathered are:

- An awareness of the need for separate scripts for initial full load and subsequent incremental load executions. 
- Understanding of the requirements from a source system e.g. modified_date, last_successful_load, last_load_date columns etc. 
- Understanding of the requirements from the receiving staging table e.g. created_date, for audit purposes. 
- Clearer understanding of the need for upsert (DO UPDATE SET vs. DO NOTHING), the tables it is required on, and why. 
- An appreciation of efficiency and storage considerations in the process. 
- A realization of staging as the repository for historical data, while fact and dimension tables are used for unique data.
- The importance of an audit table.
- A better feel for the daily activities of a data engineer.