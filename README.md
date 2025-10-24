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


Currently working on:
- finishing out bronze to silver transforms with glue job and then automate with lambda function
- clean up projects folders, get rid of files we dont need and structure files within folders
- work on some data aggrigations for gold level. Season driver stats and race results stats.

![alt text](image.png)

