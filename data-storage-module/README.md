# Data Ingestion Module

We have setup a Redshift cluster to store the data for analytics. 

* This data is optimised for analytics. 
* The user preferences table acts as a lookup table to resolve the many to many relationship between users and bid_requests table.
* The data in clicks_and_conversions table is denormalised and partitioned on the actionType column so that aggregation analytics can be performed optimally.
* We have also added timestamp column as another partition key in this table. The 2 partition keys are interleaved to allow simple queries.
* userId column acts as the central point of connection between the tables.


<img width="898" alt="image" src="https://github.com/bhaktavar/pesto-take-home/assets/43117589/417b4d17-b174-478b-8bfb-d158edf98f7e">
