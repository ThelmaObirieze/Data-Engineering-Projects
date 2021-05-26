** Discuss the purpose of this database in the context of the startup, Sparkify, and their analytical goals.

Over the years, Sparkify has had a need to improve the speed and efficiency of accessing their massive data store and to make insightful business decisions using these data. This database was setup as a datawarehouse where the business can store large the amounts of data they have collected over the years about songs and users so that they can easily analyze these data at different time periods and trends in order to use them for future predictions.

** State and justify your database schema design and ETL pipeline.

Looking at the structure of the tables, the schema design is justified because of the structure of the data we are working with. The users, songs, time and artists tables fits the struture of a dimension table as they contain information about WHO, WHERE, WHEN and WHAT of the data. The songplays, on the other hand, fits the structure of a fact table as it contains 'facts' about the songs played on the Sparkify app.

The ETL pipeline is necessary because it helps to extract the data from the several json files, transform them into the desired format and load them into a centralized data warehouse. The ETL pipeline was able to provide that benefit to the process.
To run the entire process, the new file 'run_script.py' should be used.

** [Optional] Provide example queries and results for song play analysis.

I used below query to get the list of songs that the duration is more than 500 

SELECT song_id, s.artist_id 
    FROM artists AS a 
JOIN songs AS s 
    ON a.artist_id = s.artist_id  
WHERE s.duration > 500;