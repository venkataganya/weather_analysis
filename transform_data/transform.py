import os
from pyspark.sql import SparkSession
from extract_data.extract import logging

spark = SparkSession.builder.getOrCreate()

def read_data(file_path):
    """
    Return: spark dataframe
    Input: csv filepath
    Desc: this function reads a csv file and outputs in form of a dataframe
    """
    df = spark.read.options(header='True',delimiter=',').csv(file_path)
    logging.info("Read {} complete".format(file_path))
    return df


def transform_dataset(csv_data_df):
    """
    Return: spark dataframe
    Input: csv read-spark dataframe
    Desc: This function cleans and tranforms the raw data to perform further aggregations
    """
    csv_data_df.createOrReplaceTempView("v_csv_data")
    logging.info("transforming data: {} ".format(csv_data_df))
    transform_df = spark.sql("""
                Select 
                    to_date(date,'yyyyMMdd') as temperature_recorded_date,
                    CASE WHEN max_temp_C = '-9999' THEN NULL ELSE cast(cast(max_temp_C as int)/10 as decimal(18,2)) END as max_temp_in_C,
                    CASE WHEN min_temp_C = '-9999' THEN NULL ELSE cast(cast(min_temp_C as int)/10 as decimal(18,2)) END as min_temp_in_C,
                    CASE WHEN precipitation_MM = '-9999' THEN NULL ELSE cast(cast(precipitation_MM as int)/10 as decimal(18,2)) END as precipitation_CM,
                    replace(filename, '.txt', '') as region,
                    filename as file_name,
                    concat_ws('-',cast(to_date(date,'yyyyMMdd') as string),replace(filename, '.txt', '')) as w_key
                from v_csv_data
                            """)
    logging.info("aggregating data: {} complete ".format(transform_df))
    return transform_df


def aggregated_dataset(transform_df):
    """
    Return: spark dataframe
    Input: cleansed spark dataframe
    Desc: This function performs aggregations on cleansed dataset
    """
    transform_df.createOrReplaceTempView("v_transform_df")
    logging.info("aggregating data: {} ".format(transform_df))
    aggregated_df = spark.sql("""
                Select
                    region,
                    concat_ws('-',region, left(temperature_recorded_date,4)) as w_key, 
                    left(temperature_recorded_date,4) as avg_year,
                    cast(sum(CASE WHEN max_temp_in_C IS NOT NULL THEN 1 ELSE 0 END)/count(CASE WHEN max_temp_in_C IS NOT NULL THEN 1 ELSE 0 END) as decimal(18,2))as avg_max_temp,
                    cast(sum(CASE WHEN min_temp_in_C IS NOT NULL THEN 1 ELSE 0 END)/count(CASE WHEN min_temp_in_C IS NOT NULL THEN 1 ELSE 0 END) as decimal(18,2))as avg_min_temp,
                    cast(sum(CASE WHEN precipitation_CM IS NOT NULL THEN 1 ELSE 0 END) as decimal(18,2)) as total_precip_cm
                from v_transform_df
                group by region, left(temperature_recorded_date,4), concat_ws('-',region, left(temperature_recorded_date,4))
                            """)

    logging.info("aggregating data: {} complete ".format(aggregated_df))
    return aggregated_df

def sink_dataset(df,file_name,foldername):
    """
    Return: None
    Input: spark dataframe, subfolder name and a folder name
    Desc: This function saves a dataframe to a path
    """
    current_directory = os.getcwd()
    final_path = os.path.join(current_directory, foldername)
    if not os.path.exists(final_path):
            logging.info("Path {}  does not exist, Creating path {}".format(final_path, final_path))
            os.makedirs(final_path)
    write_path = final_path+"/"+file_name
    logging.info("Writing dataframe {} ".format(df))
    df.coalesce(1).write.options(header='True', delimiter=',').csv(write_path)
    logging.info("Writing dataframe complete {} ".format(df))
    return None





