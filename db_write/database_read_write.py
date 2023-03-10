import csv
import sqlite3
import os
from extract_data.extract import logging

class DbWrite:
	"""
	Class DbWrite creats a class object to interact with a SQLite database.
	Input: We have to give the path to the .db file for this project is under db folder.
	"""
	def __init__(self,db):
		self.db = db

#Create tables
	def create_transformed_table(self):
		"""
		Return: None
    	Input: None
   	 	Desc: This function makes a creates weather_transformed table in WeatherData db.
		"""
		connection = sqlite3.connect(self.db)
		logging.info("Data base connected: {}".format(self.db))
		cursor = connection.cursor()
		logging.info("Creating table : weather_transformed")
		create_table = '''CREATE TABLE IF NOT EXISTS weather_transformed(
					w_key TEXT PRIMARY KEY NOT NULL,
					temperature_recorded_date TEXT NOT NULL,
					max_temp_in_C INTEGER NOT NULL,
					min_temp_in_C NUMERIC  NULL,
					precipitation_CM NUMERIC  NULL,
					region NUMERIC  NULL,
					file_name NUMERIC  NULL
					);
					'''
		cursor.execute(create_table)
		logging.info("Table created : weather_transformed")
		connection.commit()
		connection.close()
		return None

	def create_aggregate_table(self):
		"""
		Return: None
    	Input: None
   	 	Desc: This function makes a creates weather_aggregate table in WeatherData db.
		"""
		connection = sqlite3.connect(self.db)
		logging.info("Data base connected: {}".format(self.db))
		cursor = connection.cursor()
		logging.info("Creating table : weather_aggregate")
		create_table = '''CREATE TABLE IF NOT EXISTS weather_aggregate(
					w_key TEXT PRIMARY KEY NOT NULL,
					region TEXT NOT NULL,
					avg_year INTEGER NOT NULL,
					avg_max_temp NUMERIC  NULL,
					avg_min_temp NUMERIC  NULL,
					total_precip_cm NUMERIC  NULL
					);
					'''
		cursor.execute(create_table)
		logging.info("Table created : weather_aggregate")
		connection.commit()
		connection.close()
		return None

	def write_data(self, sub_1, sub_2):
		"""
		Return: None
    	Input: subdirectory 1 and subdirectory 2 --> transfomed files storage.
   	 	Desc: This function writes the data we transformed using spark and saved in sub folders
		      sub 1 and sub 2 to the database tables.
		"""
		connection = sqlite3.connect(self.db)
		logging.info("Data base connected: {}".format(self.db))
		cursor = connection.cursor()
		current_directory = os.getcwd()
		final_path = os.path.join(current_directory)+'/'+sub_1+'/'+sub_2
		logging.info("Fetching files from: {}".format(final_path))
		for root,dirs,files in os.walk(final_path):
			for file in files:
				if file.endswith(".csv"):
					f=open(final_path+'/'+file, 'r')
					dr = csv.DictReader(f)
					if sub_2 == "aggregated_data":
						to_db = [(i['w_key'], i['region'], i['avg_year'],i['avg_max_temp'], i['avg_min_temp'],i['total_precip_cm']) for i in dr]
						f.close()
						try:
							cursor.executemany("INSERT INTO weather_aggregate (w_key, region, avg_year, avg_max_temp, avg_min_temp, total_precip_cm) VALUES (?, ?, ?, ?, ?, ?);", to_db)
						except:
							logging.info("Error inserting data into weather_aggregate")
						logging.info("Data successfully written to weather_aggregate")
					if sub_2 == "transformed_data":
						to_db = [(i['w_key'], i['region'], i['temperature_recorded_date'],i['max_temp_in_C'], i['min_temp_in_C'],i['precipitation_CM'],i['file_name']) for i in dr]
						try:
							cursor.executemany("INSERT INTO weather_transformed (w_key, region, temperature_recorded_date, max_temp_in_C, min_temp_in_C, precipitation_CM, file_name) VALUES (?, ?, ?, ?, ?, ?, ?);", to_db)
						except:
							logging.info("Error inserting data into weather_transformed")
						logging.info("Data successfully written to weather_transformed")
		connection.commit()
		connection.close()
		return None

#get data for API
	def get_transformed_data(self, lower, upper):
		"""
		Return: a list of dict vales from the database.
    	Input: lower page boundary and upper page boundary.
   	 	Desc: This function fetches 100 rows of data from weather_transformed .
		"""
		data = []
		connection = sqlite3.connect(self.db)
		connection.row_factory = sqlite3.Row
		cursor = connection.cursor()
		get_data = """
					with cte as (
					Select rowid, w_key, region, temperature_recorded_date, max_temp_in_C, min_temp_in_C, precipitation_CM, file_name
					from weather_transformed
					)
					Select * from cte
					where rowid between ? and ?
					limit 100
					
				"""
		cursor.execute(get_data,(lower,upper,))
		rows = cursor.fetchall()

		for i in rows:
			user = {}
			user["rowid"] = i["rowid"]
			user["w_key"] = i["w_key"]
			user["region"] = i["region"]
			user["temperature_recorded_date"] = i["temperature_recorded_date"]
			user["max_temp_in_C"] = i["max_temp_in_C"]
			user["min_temp_in_C"] = i["min_temp_in_C"]
			user["precipitation_CM"] = i["precipitation_CM"]
			user["file_name"] = i["file_name"]
			data.append(user)
		
		connection.commit()
		connection.close()
		return data	

	def get_aggregate_data(self, lower, upper):
		"""
		Return: a list of dict vales from the database.
    	Input: lower page boundary and upper page boundary.
   	 	Desc: This function fetches 100 rows of data from weather_aggregate .
		"""
		data = []
		connection = sqlite3.connect(self.db)
		connection.row_factory = sqlite3.Row
		cursor = connection.cursor()
		get_data = """
					with cte as (
					Select rowid, w_key, region, avg_year, avg_max_temp, avg_min_temp, total_precip_cm
					from weather_aggregate
					)
					Select * from cte
					where rowid between ? and ?
					limit 100
				"""
		cursor.execute(get_data,(lower,upper,))
		rows = cursor.fetchall()

		for i in rows:
			user = {}
			user["rowid"] = i["rowid"]
			user["w_key"] = i["w_key"]
			user["region"] = i["region"]
			user["avg_year"] = i["avg_year"]
			user["avg_max_temp"] = i["avg_max_temp"]
			user["avg_min_temp"] = i["avg_min_temp"]
			user["total_precip_cm"] = i["total_precip_cm"]
			data.append(user)

		connection.commit()
		connection.close()
		return data

	def get_transformed_data_by_region_date(self,region,temperature_recorded_date):
		"""
		Return: a list of single dict vales from the database based on the filters.
    	Input: the weather station region and a date when the temperature was recorded for filters.
   	 	Desc: This function fetches 1 rows of data from weather_transformed based on the input filters.
		"""
		data = []
		connection = sqlite3.connect(self.db)
		connection.row_factory = sqlite3.Row
		cursor = connection.cursor()
		get_data = """
					Select rowid, * 
					from weather_transformed
					where region =  ? and temperature_recorded_date = ?
					
				"""
		cursor.execute(get_data,(region,temperature_recorded_date,))
		rows = cursor.fetchall()

		for i in rows:
			user = {}
			user["rowid"] = i["rowid"]
			user["w_key"] = i["w_key"]
			user["region"] = i["region"]
			user["temperature_recorded_date"] = i["temperature_recorded_date"]
			user["max_temp_in_C"] = i["max_temp_in_C"]
			user["min_temp_in_C"] = i["min_temp_in_C"]
			user["precipitation_CM"] = i["precipitation_CM"]
			user["file_name"] = i["file_name"]
			data.append(user)
		
		connection.commit()
		connection.close()
		return data
	

	def get_aggregate_data_by_region_year(self,avg_year,region):
		"""
		Return: a list of single dict vales from the database based on the filters
    	Input: the weather station region and a year for the average calculated for filters 
   	 	Desc: This function fetches 1 rows of data from weather_aggregate based on the input filters
		"""
		data = []
		connection = sqlite3.connect(self.db)
		connection.row_factory = sqlite3.Row
		cursor = connection.cursor()
		get_data = """
					Select rowid, * 
					from weather_aggregate
					where avg_year = ? and region = ?
				"""
		cursor.execute(get_data,(avg_year,region,))
		rows = cursor.fetchall()

		for i in rows:
			user = {}
			user["rowid"] = i["rowid"]
			user["w_key"] = i["w_key"]
			user["region"] = i["region"]
			user["avg_year"] = i["avg_year"]
			user["avg_max_temp"] = i["avg_max_temp"]
			user["avg_min_temp"] = i["avg_min_temp"]
			user["total_precip_cm"] = i["total_precip_cm"]
			data.append(user)

		connection.commit()
		connection.close()
		return data

	def get_count_agg(self):
		"""
		Return: a list of single dict vales from the database
    	Input: None
   	 	Desc: This function fetches the total count of records from the table weather_aggregate.
		"""
		data = []
		connection = sqlite3.connect(self.db)
		connection.row_factory = sqlite3.Row
		cursor = connection.cursor()
		get_data = """
					Select count(1) as total_count_aggregate_table
					from weather_aggregate
				"""
		cursor.execute(get_data)

		rows = cursor.fetchone()

		user = {}
		user["total_count_aggregate_table"] = rows["total_count_aggregate_table"]
		data.append(user)
			

		connection.commit()
		connection.close()
		return data
	
	def get_count_transf(self):
		"""
		Return: a list of single dict vales from the database
    	Input: None
   	 	Desc: This function fetches the total count of records from the table weather_aggregate.
		"""
		data = []
		connection = sqlite3.connect(self.db)
		connection.row_factory = sqlite3.Row
		cursor = connection.cursor()
		get_data = """
					Select count(1) as total_count_transformed_table
					from weather_transformed
				"""
		cursor.execute(get_data)

		rows = cursor.fetchone()

		user = {}
		user["total_count_transformed_table"] = rows["total_count_transformed_table"]
		data.append(user)
			

		connection.commit()
		connection.close()
		return data


