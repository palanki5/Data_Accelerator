#!usr/bin/python3
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
import pyspark.sql.types as st
import datetime
import json
import logging
import os
import sys


def read_data(spark, schema, read_option):
    df = spark.read.load(schema = schema, **read_option)
    df = df.withColumn("readstamp", sf.current_timestamp())
    return df

def write_data(df, write_option):
    df.write.save(**write_option)

if __name__ == "__main__":
    # LOGGING CONFIG - instead of specfying here, use a config file in future
    ## Main Logger
    module_logger = logging.getLogger(__name__)
    module_logger.setLevel(logging.INFO)
    ## Stream Handler
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.CRITICAL)
    ## Formatter for stream handler
    stream_formatter = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
    stream_handler.setFormatter(stream_formatter)
    module_logger.addHandler(stream_handler)
    try:
        assert len(sys.argv) == 2, "Incorrect usage. Provide the path to configuration file."
    except AssertionError as e:
        module_logger.critical(e);
    finally:
        sys.exit(2)

    # Creating log directory
    current_time = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    LOG_DIR = os.path.join(os.pardir, os.path.dirname("logs/" + current_time + "/"))
    os.makedirs(LOG_DIR, exist_ok=True)

    # Global config
    LOG_FILE = os.path.join(LOG_DIR, 'ingestion.log')

    ## File Handler for logging
    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setLevel(logging.info)
    ## Formatter for file handler
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(lineno)d - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    module_logger.addHandler(file_handler)

    module_logger.info("Beginning..")
    
    # Reading configuration file
    module_logger.info("Config File")
    try:
        with open(sys.argv[1], 'r') as file:
            config = json.load(file)
    except Exception as e:
        module_logger.critical('Could not read config file! Exiting..')
        module_logger.exception(e)
        sys.exit()
    module_logger.debug("Config file successfully loaded")

    # Spark Configuration
    module_logger.debug("Creating SparkSession object")
    conf = SparkConf().setAll(zip(config['spark']['conf']['key'], config['spark']['conf']['value']))
    try:
        spark = SparkSession.builder.config(conf = conf).getOrCreate()
    except Exception as e:
        module_logger.critical('Cannot create SparkSession! Exiting..')
        module_logger.exception(e)
        sys.exit()

    # Ingestion
    for entity in config['data']:
        schema_file = entity['schema']
        with open(schema_file, 'r') as file:
            schema = st.StructType.fromJson(json.load(file))
        df = read_data(spark, schema, entity['read_option'])
        write_data(df, entity['write_option'])   
