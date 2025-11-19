Project Description

The project showcases a pipeline that fetches data from a GCS bucket and delivers to BigQuery. The exercise introduced 
a data lake into the pipeline and showcases its BigData implications, such as working with blobs, using a separate file 
for each extraction to the DW, and learning new syntax. The exercise showcases the basic design for a bigdata pipeline 
optimized for daily load.

Implications/insights:
- Data is extracted and loaded to staging in a DW in its raw form without any transformation after which it is 
transformed and loaded to a second, cleaned staging table.
- This is aligned with the ELT concept popular with BigData landscapes and the exercise served as an introduction to it.
In ELT, data is extracted and loaded to repositories where it may be consumed in that form, or it may go through a next 
stage of transformation, if needed.
- The data transformation that may be required when moving data from a data lake to a data warehouse requires solutions 
optimized for BigData (e.g. DataFlow, Spark) than what pandas provides.
- The transformation done in this exercise could therefore be carried out only after the data was already in the DW, due 
to reliance on pandas. Once the data is in the cleaned form, the rest of the pipeline may mirror the same design as the 
earlier 'BigQuery' exercise.
- A loader.py script was created to handle loading data to GCS in readiness for the exercise.
