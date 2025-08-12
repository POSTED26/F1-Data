F1 data pipeline

My goal for this project is to get used to using an api and pulling, cleaning, transforming data, and loading it into a postgres database. 

Plans:
- load data in through the F1 api
- clean the data
- tansform the data to a useable format to be able to perform analytics
- load data into a postgres database


Features:
- Pull data from the openF1 api
- Use pandas to take the data and put it into dataFrames
- Do some data cleaning/processing (selecting columns we care about, dealing with null values, etc.)
- Creating a connection to our Postgres database using docker and sqlalchemy
- Create our tables in our database
- Load the tables from our dataFrames



