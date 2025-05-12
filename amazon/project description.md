Project Description

This is a simple data pipeline that fetches data from an excel file, transforms and uploads it to a Postgres database.

The design here (based on best practice) is to load data to a staging table, transform
and export dimension table data to their tables, create surrogate keys on those tables and
import the surrogate keys back to the staging table.

Afterward transform and export fact table data together with all surrogate keys to the
fact table.

Note that this design sometimes require some transformation to be done on the data in staging
before exporting to dimension tables. This is because at the step where surrogate keys are
loaded back to staging, the action is based on a comparison between staging and dimension
table data which will fail if one table is transformed and the other not.


In progress: the relationships in this example are not logical, the example is therefore unique in that it is a big exception from the norm. a product dimension might normally be sub-dimensioned to product category, and not review as in this case.
