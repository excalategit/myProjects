Project Description

The project showcases a pipeline that fetches data from a GCS bucket and delivers to BigQuery. The exercise introduced a data lake into my pipeline for the first time and showcases its bigdata implications such as working with blobs, using a separate file for each extraction to the DW, and learning new syntax. The exercise exposed me to the basic design for a bigdata pipeline optimized for daily load.

Some implications/insights:
- Data is extracted and loaded to staging in a DW in its raw form without any transformation after which it is transformed and loaded to a cleaned staging table.
- This is because a transformation between a data lake and a data warehouse (if needed) requires more optimized solutions for bigdata than what pandas provides e.g. DataFlow, Spark
- The transformation is therefore carried out when the data is already in the DW. Once the data is in the cleaned form, the rest of the pipeline may mirror the same design as the BigQuery exercise.
- This is also aligned with the ELT concept which is popular with bigdata realities and served as my introduction to it. Basically data is extracted and loaded to repositories where it may be consumed in that form, or it may go through a next stage of transformation only if needed.
- Because the DL contains unstructured data by design, this has an impact on the extraction-to-DW layer as the script needed to be updated to read .csv and not .sql or .xlsx, as well as other implications.
- A loader.py script was created to handle loading data to GCS in readiness for the exercise.
