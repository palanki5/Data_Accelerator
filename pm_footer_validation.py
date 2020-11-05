import os
import sys
import re
import json
import csv
import fnmatch
import shutil
import subprocess
import glob
from datetime import datetime
import logging
from dateutil import parser
from decimal import Decimal, getcontext, ROUND_FLOOR


from pyspark import SparkConf, SparkContext
from pyspark import HiveContext
from pyspark.sql.types import ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType
from pyspark.sql.types import StringType, NullType, DecimalType, DateType, TimestampType
from pyspark.sql.types import StructType, StructField

def num_of_records(full_path):
	f = open(full_path)
	nr_of_lines = sum(1 for line in f)
	f.close()
	return nr_of_lines

def read_record(file, record_number):
	try:
		line = open(file, "r").readlines()[record_number].rstrip('\r\n')
		return line
	except Exception as ex:
		logger.info("invalid record count")

# validating the footer in input data file
def footer_validation(data_source, table_name, table_attr, path_dict, error_list,logger):
	try:
		file_pattern = table_attr["validation"]["triplet_check"]["file_pattern"]
		header_flag = table_attr["validation"]["record_count_validation"]["header"].lower()
		footer_flag = table_attr["validation"]["record_count_validation"]["footer"].lower()
		rec_cnt_pos_footer = table_attr["validation"]["record_count_validation"]["rec_cnt_pos_footer"]
		amt_sum_pos_footer = table_attr["validation"]["record_count_validation"]["amt_sum_pos_footer"]
		amt_col_file = table_attr["validation"]["record_count_validation"]["amt_col_file"]
		inp_dir_path = path_dict["inp_dir_path"]
		err_dir_path = path_dict["err_dir_path"]
		delimiter = table_attr["delimiter"]
		amount_sum=0
		rec_cnt_in_file = 0
		for filename in os.listdir(inp_dir_path):
			next_batch_seq_num=filename.split('-',4)[2]
			file_pattern = file_pattern.replace("(<batch_seq>)", next_batch_seq_num)
			if filename.endswith(".dat") | filename.endswith(".txt") | filename.endswith(".csv")and fnmatch.fnmatch(filename, file_pattern):    
				filename = inp_dir_path + "/" + filename
				rec_cnt_pos_footer=int(rec_cnt_pos_footer)
				rec_cnt_in_file = num_of_records(filename)
				data_rec = read_record(filename, -1).split(delimiter)[rec_cnt_pos_footer].lstrip("0")
				amt_sum_pos_footer=int(amt_sum_pos_footer)
				data_amount_sum = read_record(filename, -1).split(delimiter)[amt_sum_pos_footer].replace(',','')
				amt_col_file=int(amt_col_file)
				if header_flag == 'y' and footer_flag == 'y':
					rec_cnt_in_file -= 2
					logger.debug("record count without header and footer:" + str(rec_cnt_in_file))
					logger.debug("record count as per footer in inputfile"+data_rec)
					# validating the      record count from the footer and datafile
					if data_rec=='':
						data_rec=0
					if str(data_rec) == str(rec_cnt_in_file):     
						logger.info("Record Count matching in footer and data file for feed")
					else:
						logger.info("Record Count mismatch in footer and data file for feed " + table_name)
						logger.debug("Record Count mismatch in footer and data file for feed " + table_name)
						logger.debug(".txt file Record count: " + str(rec_cnt_in_file))
						logger.debug("footer  Record count: " + str(data_rec))
						# Move all the files respective to this feed to error table
						move_files_to_dst(inp_dir_path, err_dir_path, file_pattern)
						# Make an entry in Error Table..
						err_code = "10"
						err_description = "RecordCount_Mismatch for Feed and footer " + table_name
						invalid_input_files(data_source, table_name, err_code, err_description, error_list)
						return False
				header = 1
				footer = 1
				with open(filename) as f:
					logger.info("skipping the header and footer while calculating the sum")
					for line in f.readlines()[header:-footer if footer else None]:
						amount_sum += float(line.split(delimiter)[amt_col_file].replace(',',''))
					logger.info("calculated sum:"+str(float(amount_sum)))
					logger.info("Total_amount:" +data_amount_sum)
					if float(data_amount_sum) == float(amount_sum):
						logger.info("SUM IS MATCHING")
						return True
					else:
						logger.info("Amount sum mismatch in footer and data file for feed " + table_name)
						logger.debug("Amount sum mismatch in footer and data file for feed " + table_name)
						logger.debug(".txt file sum: " + str(amount_sum))
						logger.debug("footer sum: " + str(data_amount_sum))
						err_code = "11"
						# Move all the files respective to this feed to error table
						move_files_to_dst(inp_dir_path, err_dir_path, file_pattern)
						# Make an entry in Error Table..
						err_description = "Sum_Mismatch for Feed and footer " + table_name
						invalid_input_files(data_source, table_name, err_code, err_description, error_list)
						return False
				if footer_flag == 'y':
					rec_cnt_in_file -= 1
					logger.debug("record count without footer:" + str(rec_cnt_in_file))
					logger.debug("record count as per input file footer:" +str(data_rec))
					# validating the      record count from the footer and datafile
					if str(data_rec) == str(rec_cnt_in_file):     
						logger.debug("Record Count matching in footer and data file for feed")
					else:
						logger.debug("Record Count mismatch in footer and data file for feed " + table_name)
						logger.debug(".txt file Record count: " + str(rec_cnt_in_file))
						logger.debug("footer  Record count: " + str(data_rec))
						# Move all the files respective to this feed to error table
						move_files_to_dst(inp_dir_path, err_dir_path, file_pattern)
					
						# Make an entry in Error Table..
						err_code = "10"
						err_description = "RecordCount_Mismatch for Feed and footer " + table_name
						invalid_input_files(data_source, table_name, err_code, err_description, error_list)
						return False
				header = 0
				footer = 1
				with open(filename) as f:
					# skipping the header and footer while caliculating the sum 
					for line in f.readlines()[header:-footer if footer else None]:
						amount_sum += float(line.split(delimiter)[amt_col_in_file].replace(',',''))
						logger.debug("Total_amount:" +data_amount_sum)
						logger.debug("caliculated sum:"+str(amount_sum))
						
						if float(data_amount_sum) == float(amount_sum):
							logger.info("SUM IS MATCHING")
						else:
							logger.debug("Amount sum mismatch in footer and data file for feed " + table_name)
							logger.debug(".txt file sum: " + str(amount_sum))
							logger.debug("footer sum: " + str(data_amount_sum))
							return False
		return True
	except Exception as ex:
		logger.error("Job failed in footer record validation with below error for feed " + table_name)
		logger.error(ex)
		sys.exit(1)
if __name__ == "__main__":
	log_path='/dbfs/mnt/apps/b2b_datalake/logs/ingestion_payment_manager.log'
	logger = logging.getLogger(__name__)
	logger.setLevel(logging.INFO)
	handler = logging.FileHandler(log_path)
	handler.setLevel(logging.INFO)
	logger.addHandler(handler)
	formatter = logging.Formatter('%(asctime)s : %(name)s : %(levelname)s : %(message)s')
	handler.setFormatter(formatter)
	
	etl_config_file_path = sys.argv[1]
	
	jfile = json.loads(open(etl_config_file_path).read())
	
	# Initialize error list
	error_list = []
	dbfs_path='/dbfs/mnt'
	
	# Populate the properties from the base file
	data_source = jfile["ingestion_config"]["source"]
	app_name = jfile["ingestion_config"]["app"]
	
	
	# Get the hdfs home directory path.. e.g. /data/b2b
	home_dir = jfile["ingestion_config"]["home_dir"]
	# Append Staging Area path to hdfs home dir.. e.g. /data/b2b/staging
	hdfs_staging_dir = home_dir + "/" + "staging"
	# Append source name to hdfs path.. e.g. /user/replicator/flexcab
	source_hdfs_dir = hdfs_staging_dir + "/" + data_source
	# Get the base directory, input output error archive log location from the configuration file
	base_os_path = dbfs_path+jfile["ingestion_config"]["location"]["base_dir"]
	inp_dir_name = jfile["ingestion_config"]["location"]["input_dir"]
	err_dir_name = jfile["ingestion_config"]["location"]["error_dir"]
	src_dir_name = jfile["ingestion_config"]["location"]["unzipped_dir"]
	
	data_path = base_os_path
	inp_dir_path = data_path + "/" + inp_dir_name
	err_dir_path = data_path + "/" + err_dir_name
	src_dir_path = data_path + "/" + src_dir_name
	
	# Construct a dictionary with above values so that it can be passed to functions as needed
	path_dict = {"data_path": data_path,
						"inp_dir_path": inp_dir_path,
						"err_dir_path": err_dir_path,
						"src_dir_path": src_dir_path
						}
	
	table_list = jfile["ingestion_config"]["datafeed"]
	
	
	#check_for_empty_ip_dir(data_source, path_dict)
	
	for table_name in table_list:
		logger.info("FOOTER VALIDATION STARTED FOR "+table_name)
		table_attr = jfile["ingestion_config"]["datafeed"][table_name]
		if footer_validation(data_source, table_name, table_attr, path_dict, error_list,logger):
			logger.info("VALIDATION SUCCESS ")

