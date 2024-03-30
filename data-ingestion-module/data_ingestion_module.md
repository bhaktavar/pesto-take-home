# Data Ingestion Module

## Real-time ingestion

We ingest **Ad Impressions** and **Bid Requests** in real-time using a **Kinesis Data Stream**.

- **AWS API** calls write the data to **Kinesis Data Streams**.
- Separate streams are designated for ingesting the two datasets.
- **Lambda** functions are attached to the streams to perform data cleaning and JSON validation.
- **Kinesis Data Streams'** storage functionality allows us to set up our service to create a new **Lambda** instance for every 100 rows (depending on the size of individual records), which then processes these rows.
- We incorporate Dynamic Shard Management in case of a spike in the number of records.
- The **Lambda** function will:
  1. Validate the JSON structure.
  2. Extract relevant information.
  3. Handle data format transformations.
  4. Fix data in case of simple malformations.
  5. Write valid data to an **S3** bucket and invalid data to another **S3** bucket.
- We set up a **CloudWatch** dashboard to compare valid and invalid data and set up alarms to ensure that invalid data is addressed promptly.

![image](https://github.com/bhaktavar/pesto-take-home/assets/43117589/91bc6bc7-464f-45f6-894e-632ec3a029dd)

## Batch ingestion

*Clicks and Conversions* data is received in CSV format. Assuming this means we are receiving this data in batch from the source, we directly store this incoming data in an **S3** bucket.

- The **AWS S3 API** writes the data directly into an **S3** bucket.
- An **S3** trigger event creates a **Lambda** instance, which can perform preliminary processing on this data.
- The **Lambda** is responsible for similar processing as above, including validating and cleaning the data as well as writing valid and invalid data to separate directories in **S3**.
- We also set up **CloudWatch** dashboards and alarms to track data quality.

![pesto1](https://github.com/bhaktavar/pesto-take-home/assets/43117589/406b2ddd-ddb4-4867-9e8c-7046a6bc23ca)


