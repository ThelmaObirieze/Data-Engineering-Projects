import pandas as pd
import re
import psycopg2
from collections import defaultdict
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col



def check_data(spark, table):
    
    # reading the data from parquet
    df_dim_city = spark.read.parquet(table + "/")

    df_dim_city.registerTempTable(table)

    # Checking number of rows in the table
    result = spark.sql("""
    SELECT COUNT(*)
    FROM """ + table + """
    """).show()
    
    print(result)

def check_integrity(spark, fact_immigrations, dim_city, dim_airport, dim_country):

    integrity_cities = fact_immigrations.select(col("state_code")).distinct() \
        .join(dim_city, fact_immigrations["state_code"] == dim_city["state_code"], "left_anti") \
        .count() == 0

    integrity_airports = fact_immigrations.select(col("port_code")).distinct() \
        .join(dim_airport, fact_immigrations["port_code"] == dim_airport["iata_code"], "left_anti") \
        .count() == 0

    integrity_countries = dim_airport.select(col("iso_country")).distinct() \
        .join(dim_country, dim_airport["iso_country"] == dim_country["code"], "right_anti") \
        .count() == 0 

    return integrity_cities #& integrity_airports & integrity_countries #Checks return true or false

if __name__ == "__main__":
    main()