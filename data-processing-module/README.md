# Data Ingestion Module
The three tables are going be created using **Glue Pyspark** jobs.

1. Fetch the data from S3 using a dynamic frame.
2. **Glue bookmark feature** will be used to make sure files which are already worked on are not processed again.
3. Transformations filtering will take place here.
4. Final data will be written to Redshift using JDBC connection.

![pesto-2](https://github.com/bhaktavar/pesto-take-home/assets/43117589/8d3bb017-95f7-4aa8-bd5f-c8020a8c7365)
