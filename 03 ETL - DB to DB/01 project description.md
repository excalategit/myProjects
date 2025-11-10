# Project Description

The script showcases a departure from a data transformation process tailored to the dataset being analyzed (or manual 
data transformation) in favour of an automated one. Here, the assessment of the dataset would have been completed at 
the beginning of the project and the transformation plan for the data would have been determined and automated. This 
means that subsequent processing of the data would require minimal manual intervention e.g. specific default values are 
programmed to replace null values depending on the data type of the column they appear under. 

The design completes all transformations before the data is loaded to staging before breaking off to fact & dimension 
tables. No transformation is done after staging except for a final check to remove duplicates  based on business keys 
(last step of data validation). 

The script also tries to follow a basic and practical data transformation path based on best practices of data 
exploration, data restructuring, data cleaning, and data validation. 