Project Description

The project showcases a pipeline that sources its data from APIs. The data is first loaded to a GCS bucket from where it is delivered to BigQuery. 3 APIs are involved therefore requiring a slightly different process for the extraction process.

The exercise made it more clear that there is no fixed or standard pipeline design, rather it is dependent on factors like the technology employed, types and number of datasources, data format, and other realities. In this case BigQuery's newline-delimited JSON format required a different script for the Extract layer.

Some implications/insights:

- 3 different calls were made to GCS for each of the API data.
- Because the DL contains unstructured data by design, the extraction-to-DW layer was updated to read .json and not .sql or .xlsx as usual.
- Because the transformation is carried out when the data is already in the DW, each of the blobs was loaded to BigQuery staging in their raw form, then later transformed, before joining them into one cleaned staging table, adding extra steps to the process.
- 2 loader scripts were created for educational purposes, one for extracting data from the API in the json array format and the other in the newline-delimited (NDSON) format.
- Pandas is still used for data transformation for now, instead of dbt and the likes.

