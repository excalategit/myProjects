Project Description

The project fetches data from APIs, combines, transforms, and loads them to a Postgres database modeled as a snowflake schema.

The project involves connections to multiple API datasources and showcases integration of data from different sources. It shows how extraction and transformation designs are on a case-by-case basis since some transformations may be required even before staging the data, rather than after.

The pipeline starts by combining the multiple datasources into a staging table. Next the dimension tables are created and their surrogate keys are copied back to staging table. Finally, the fact table data together with all surrogate keys connecting them to dimension tables are extracted into a fact table.

Highlights of the Project (or improvements over previous demo):

- fetching and combining data from multiple sources (APIs)
- heavier transformation layer activities
- utilization of snowflake schema
- showing awareness of data governance guidelines e.g. masking
- using standard method for hosting, versioning, and presenting projects and code bases e.g. GitHub
	- including a text file for project description
	- keeping credentials safe with environment variables and .gitignore
