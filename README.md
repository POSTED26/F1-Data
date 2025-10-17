F1 data pipeline - S3 glue branch

My goal for this project is to ingest data from the openF1 API and then load the data into the bronze level in S3, use glue to transform the data into silver and then evenutally gold.

Plans:
- Pull data from OpenF1 API
- use boto to put into AWS S3 
- implement multi hop architecture (bronze, silver, gold)
- clean data and move to silver
- aggregate and move to gold
- use glue for transform and metadata
- monitor using prometheous and grafana (maybe able to do within s3?)






