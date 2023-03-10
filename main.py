import os
from db_write.database_read_write import DbWrite
from extract_data.extract import *
from transform_data.transform import read_data, transform_dataset, aggregated_dataset, sink_dataset

db = DbWrite(db = 'db/WeatherData.db')

def main(url):

    #Extract_Data
    logging.info("Extracting data from - {}".format(url))
    html_data = get_data(url)
    logging.info("Parsing data")
    data = parse_html(html_string = html_data)
    logging.info("Saving raw data")
    get_data_save(ur_data=data)

    #Transform_Data
    final_directory = os.path.join(os.getcwd(), r'raw_files')
    csv_data = read_data(file_path = final_directory)
    logging.info("Transfomation 1 - TXT to CSV")
    transformed_data = transform_dataset(csv_data_df = csv_data)
    logging.info("Aggregating CSV data")
    aggregated_data = aggregated_dataset(transform_df = transformed_data)
    logging.info("Sink tranformed data")
    sink_dataset(df = transformed_data,file_name='transformed_data',foldername=r'enriched_files')
    logging.info("Sink aggregated data")
    sink_dataset(df = aggregated_data,file_name='aggregated_data',foldername=r'enriched_files')

    #Write data
    logging.info("Creating aggregate table")
    db.create_aggregate_table()
    logging.info("Creating transformed table")
    db.create_transformed_table()
    logging.info("Saving aggregated data to the table")
    db.write_data(sub_1='enriched_files', sub_2= 'aggregated_data')
    logging.info("Saving transformed_data data to the table")
    db.write_data(sub_1='enriched_files', sub_2= 'transformed_data')


if __name__ == "__main__":
    main(url="https://github.com/corteva/code-challenge-template/tree/main/wx_data")