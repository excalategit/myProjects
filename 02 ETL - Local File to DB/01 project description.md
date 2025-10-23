Project Description

The data pipeline fetches data from an excel file, transforms, and uploads it to a Postgres database.

The project required an unusual form of the snowflake schema. In this model, the fact table has only one branch to
a dimension table which branches out to sub-dimensions. Also, while dimension to sub-dimension relationships are 
usually logical e.g. City - State - Country, here it is not, but is rather based on a use case, Product - Review.

For this use case, two designs were built, one involving a join table between the dimension and sub-dimension tables
and the other not. The first design helped to solidify practical knowledge on table relationships and join table 
utilization but was later discovered to be inefficient as it will not answer business questions accurately i.e. cannot 
generate a report of products, their reviews, and their corresponding reviewers because there is no relationship between
the user and review tables. This prompted the creation of a second improved design to address this.

The design here (based on best practice) first loads data to a staging table, then transforms
and exports dimension data to their respective tables. Next it fetches the surrogate keys from the dimension tables and
imports them back to the staging table. Finally it transforms and exports fact data together with all surrogate keys to 
the fact table.

The method of loading all surrogate keys back to staging in order to populate the fact table is preferred because, this 
way all the data (fact data and surrogate keys) are already in one place, ready for export to the fact table. The 
alternative would involve creating table joins between the fact, staging, and concerned dimension tables in order to 
fetch surrogate keys.

Note that this design requires some transformation to be done on the data in staging before exporting to dimension 
tables. This is because at the step where surrogate keys are loaded back to staging, the action is based on a comparison
between staging and dimension table data which will fail if one table is transformed and the other not.