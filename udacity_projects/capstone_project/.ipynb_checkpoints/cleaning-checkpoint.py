import pandas as pd
import re
import psycopg2
from collections import defaultdict
from datetime import datetime, timedelta
from pyspark.sql import SparkSession


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark


def clean_immigration_data(spark, df):
    
    # creating a temp view for immigration
    df.createOrReplaceTempView("immigration_view")

    # read transformed immigration a new dataframe
    df_immigration = spark.sql("SELECT cicid, CAST(i94yr as INT) AS i94yr , CAST(i94mon as INT) AS i94mon , \
    CAST(i94cit as int) i94cit, CAST(i94res as int) i94res, i94port as port_code, i94addr as state_code, \
    cast(i94visa as int) visa_type, cast(i94mode as int) as mode_type , visapost, gender, airline, visatype, \
    date_add(to_date('1960-01-01'), arrdate) AS arrival_date, date_add(to_date('1960-01-01'), depdate) AS departure_date \
    FROM immigration_view")
    
    return df_immigration


def clean_city_data(spark, df):
    # CREATING a temp view for the cities data
    df.createOrReplaceTempView("cities_view")

    # clean and transforming cities data
    Clean_df_cities = spark.sql("""
                                SELECT  City, 
                                        State, 
                                        `Median Age` AS median_age, 
                                        `Male Population` AS male_population, 
                                        `Female Population` AS female_population, 
                                        `Total Population` AS total_population, 
                                        `Foreign-born` AS foreign_born, 
                                        `Average Household Size` AS average_household_size, 
                                        `State Code` AS state_code, 
                                        Race, 
                                        Count
                                FROM cities_view
    """)
    return Clean_df_cities


def clean_airport_data(spark, df):
    # create the temp view for airports
    df.createOrReplaceTempView("airports_view")
    
    # returning desired columns
    clean_df_airports = spark.sql("SELECT iata_code,name, type, iso_country, continent, iso_region, \
    municipality, gps_code FROM airports_view where iata_code is not null")
    
    #loading port data
    with open("./I94_SAS_Labels_Descriptions.SAS") as f:
    content = f.readlines()
    content = [x.strip() for x in content]
    ports = content[302:962]
    splitted_ports = [port.split("=") for port in ports]
    port_codes = [x[0].replace("'","").strip() for x in splitted_ports]
    port_locations = [x[1].replace("'","").strip() for x in splitted_ports]
    port_cities = [x.split(",")[0] for x in port_locations]
    port_states = [x.split(",")[-1] for x in port_locations]
    df_port_locations = pd.DataFrame({"port_code" : port_codes, "port_city": port_cities, "port_state": port_states})
    spark_port = spark.createDataFrame(df_port_locations)
    
    # create temp view for ports
    spark_port.createOrReplaceTempView("ports_view")
    
    #Cleaning and transforming airport df
    clean_df_airports = spark.sql("SELECT iata_code,name, type, iso_country, continent, iso_region, municipality, gps_code, \
    port_city, port_state FROM airports_view a\
    join ports_view p ON a.iata_code = p.port_code\
    where iata_code is not null")

    return clean_df_airports

def clean_country_data (spark, df):
    # creating temp view
    df[['FIPS', 'CLDR display name', 'Capital']].createOrReplaceTempView("countries_view")
    
    ### returning desired df
    Clean_df_countries = spark.sql("""SELECT FIPS as code, `CLDR display name` as country, Capital from countries_view""")
    
    return Clean_df_countries


if __name__ == "__main__":
    main()