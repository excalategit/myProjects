Project Description

The project required an unusual form of the snowflake schema. In this model, the fact table has only one branch to
a dimension table which branches out to sub-dimensions. Also, while dimension to sub-dimension relationships are 
usually logical e.g. City - State - Country, here it is not but rather based on a use case, Product - Review.

Based on the model above, two designs were built, one involving a join table between the dimension and sub-dimension table
and the other not. The first table helped to solidify practical knowledge on table relationships and join table utilization but
was discovered to be inefficient as it will not answer business questions accurately i.e. cannot generate a report of products, their reviews, and corresponding reviewers because there is currently no relationship between users and reviews. This is what
prompted the second, optimized design.

The data pipeline fetches data from an excel file, transforms, and uploads it to a Postgres database.

The design here (based on best practice) loads data to a staging table, transforms
and exports dimension data to their respective tables, fetches the surrogate keys from those tables and
imports them back to the staging table, before finally transforming and exporting fact data together with all 
surrogate keys to the fact table.

This design involving first loading all surrogate keys back to staging is preferred as it avoids table joins while loading surrogate keys to subsequent target tables e.g. to fact table, because all data is already in one place.

Note that this design sometimes require some transformation to be done on the data in staging
before exporting to dimension tables. This is because at the step where surrogate keys are
loaded back to staging, the action is based on a comparison between staging and dimension
table data which will fail if one table is transformed and the other not.