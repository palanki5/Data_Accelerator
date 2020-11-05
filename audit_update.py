# coding=utf-8
import os
import sys
import argparse
import json
import fnmatch
import logging
import zipfile
import shutil
import time
import datetime
import glob

from ingestion_fw.utils.reccountzip import compressedFile
from ingestion_fw.utils.utils import reverse, num_of_records, valid_file_date, read_record, \
    move_files_to_dst, get_file_name_and_ext, \
    create_dir, cleanup_directory, valid_file_extension, \
    insert_record_batch_seq_detail, update_record_batch_seq_detail, \
    get_batch_seq_id_for_this_run, insert_record_into_batch_file_detail
from ingestion_fw.metastore.sequence_pattern import get_next_seq_id
from ingestion_fw.utils.audit_utils import log_audit_entry,ingestion_audit_metrics_dict


batch_seq_id = None
db_connection_prop = None
processed_file_list = []


# Print Start Dags message
def start_dags():
    logger = logging.getLogger(__name__)
    logger.debug("DAG STARTED RUNNING")


# Print end dags message
def end_dags():
    logger = logging.getLogger(__name__)
    logger.debug("DAG HAS COMPLETED SUCCESSFULLY..")


def parse_cmd_args():
    """
    add the command line arguments that are to be parsed.
    :return: the parsed arguments
    """
    cmd_parser = argparse.ArgumentParser(description="Ingestion Framework Insert 'In-Progress' record module")
    cmd_parser.add_argument("etl_config_file_path", help="path to the ETL configuration file for this job")
    cmd_parser.add_argument("db_connection", help="Postgres database connection property file")
    cmd_parser.add_argument("job_id", help="the job id of the run that is currently being processed")
    cmd_args = cmd_parser.parse_args()
    return cmd_args


def main():
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s: [%(levelname)s]: %(name)s: %(message)s")

    # Parse command line arguments.. Input to the script is the full path to the ETL configuration file
    args = parse_cmd_args()

    # Read the ETL JSON property file
    jfile = json.loads(open(args.etl_config_file_path).read())

    # Read the job id from the parameter
    job_id = "'" + args.job_id + "'"  # Appending single quote to make it postgres insert query syntax format

    # Initialize error list
    error_list = []

    # postgres database connection property file
    global db_connection_prop
    db_connection_prop = args.db_connection

    # Populate the properties from the base file
    data_source = jfile["ingestion_config"]["source"]
    feed_id = jfile["ingestion_config"]["feed_id"]
    source_staging_table = jfile["ingestion_config"]["source_staging_table"]

    # Get the home directory path.. e.g. /data/raw
    home_dir = jfile["ingestion_config"]["home_dir"]
    mounted_home_dir = "/dbfs/mnt" + home_dir
    # Append Staging Area path to home dir.. e.g. /data/raw/staging
    staging_dir = mounted_home_dir + "/" + "staging"
    # Append source name to path.. e.g. /data/raw/staging/flexcab
    source_dir = staging_dir + "/" + data_source

    # Remove these below comments.. these are commented for testing purpose so that job execution takes less time
    # Create HDFS home directory if not exists.. Also create source system subdirectory inside hdfs home directory
    create_dir(mounted_home_dir)
    create_dir(staging_dir)
    create_dir(source_dir)

    table_list = jfile["ingestion_config"]["datafeed"]

    start_dags()

    """
    Get the initial batch sequence and sequence pattern from the json configuration...
    If the sequence is by number/sequence, then seq_pattern need not be present in json. default to empty
    If the sequence is by date, then the sequence pattern should be provided in first time load seq pattern attribute..
    The initial sequence provided should match this pattern...
    The length of the number sequence should be provided if the sequence is by number..
    Based on the length the sequence is to be prefixed with zero...
    for e.g. if the next seq id is returned as '2' and the length expected is 8, then it would become
    00000002 so that it matches the file pattern of MICA_B2B_TPR1001_D_00000002_16052017170101.ctl
    These attributes are passed to sequence controller and then next sequence id is abstracted from
    respective class...
    """
    inital_seq_id = jfile["ingestion_config"]["start_sequence"]["initial_seq_id"]
    file_seq_pattern = jfile["ingestion_config"]["start_sequence"].setdefault("seq_pattern", "")
    num_seq_length = jfile["ingestion_config"]["start_sequence"].setdefault("length", "")
    num_seq_length = int(num_seq_length) if num_seq_length else None
    seq_type = jfile["ingestion_config"]["seq_type"].lower()
    series = jfile["ingestion_config"].get("series", "")

    # Get the sequence interval..
    # If seq type is Numeric : set the seq_interval to Null (None)
    # If seq_type is date : If not interval is provided, default to Daily
    #                       If interval is provided, use it.. interval can be monthly, weekly, daily
    #                                                                         monday-friday, tuesday-thursday etc.
    # The interval will be used to calculate how many days should be incremented to look for next sequence in file
    seq_interval = jfile["ingestion_config"].setdefault("seq_interval", "daily").lower() if seq_type == "date"\
        else None

    # Get the next sequence id expected for this run
    next_seq_id = get_next_seq_id(seq_type=seq_type,
                                  feed_id=feed_id,
                                  initial_seq_id=inital_seq_id,
                                  seq_pattern=file_seq_pattern,
                                  series=series,
                                  db_prop_file=db_connection_prop,
                                  seq_interval=seq_interval)

    if next_seq_id is None:
        raise Exception("Previous run is still In-Progress.. Please process the previous batch first")

        # Audit Logging
        metrics=ingestion_audit_metrics_dict()
        metrics.update({"data_source":data_source,"error_msg":"Previous run is still In-Progress. Please process the previous batch first","error_type":"AUDIT UPDATE ERROR"})
        log_dict={"run_id": str(job_id),"key":"'|"+str(feed_id)+"'","event_type": "'Ingestion'",
              "event": "'AUDIT_UPDATE'","source": "'NA'",
              "target": "'NA'","source_type": "'NA'","target_type": "'NA'",
              "status": "'Failed'","metrics": metrics}
        log_audit_entry(log_dict,db_connection_prop)
        sys.exit(1)

    # Pre-Fix leading zeros and convert to string type as seq_id is VARCHAR in control table
    next_seq_id_str = str(next_seq_id).zfill(num_seq_length) if num_seq_length is not None else str(next_seq_id)
    logger.info("audit_update Next Sequence ID Value For This Execution Run..... {0}".format(next_seq_id_str))

    # Insert a record into batch_seq_detail table with status as "In-Progress" for this run
    insert_record_batch_seq_detail(feed_id,
                                   next_seq_id_str,
                                   job_id,
                                   status="'In-Progress'",
                                   prop_file_path=db_connection_prop)

    # Audit Logging
    metrics=ingestion_audit_metrics_dict()
    metrics.update({"data_source":data_source})
    log_dict={"run_id": str(job_id),"key":"'"+next_seq_id_str+"|"+str(feed_id)+"'","event_type": "'Ingestion'",
              "event": "'FILE_VALIDATION'","source": "'NA'",
              "target": "'NA'","source_type": "'NA'","target_type": "'NA'",
              "status": "'Initiated'","metrics": metrics}
    log_audit_entry(log_dict,db_connection_prop)

    # Retrieve the batch seq id for the inserted record. Batch Seq id is a surrogate key in the table...
    # Hence we are not using that column in previous insert statement.
    # We have to retrieve this value to be used later in update and insert query
    global batch_seq_id
    batch_seq_id = get_batch_seq_id_for_this_run(feed_id, prop_file_path=db_connection_prop)

    for table_name in table_list:
        table_attr = jfile["ingestion_config"]["datafeed"][table_name]

        table_path = source_dir + "/" + table_name
        create_dir(table_path)
        source_staging_dir = source_dir + "/" + source_staging_table + "/*" 
        shutil.move(source_staging_dir, table_path)
        #Audit Logging
        metrics=ingestion_audit_metrics_dict()
        metrics.update({"data_source":data_source,"success_msg":"Audit Update Successful"})
        audit_source = str(inp_dir_path+"/"+file_pattern).replace("//","/")
        log_dict={"run_id": str(job_id),"key":"'"+next_seq_id_str+"|"+str(feed_id)+"'","event_type": "'Ingestion'",
          "event": "'Audit_Update'","source": "'"+file_path+"'",
          "target": "'"+table_path+"'","source_type": "'File'","target_type": "'Table'",
          "status": "'Success'","metrics": metrics}
        log_audit_entry(log_dict,db_connection_prop)


    end_dags()

    # Airflow BashOperator by default takes the last line written to stdout into the XCom variable
    # once the bash command completes. Hence printing the next sequence which will be used in the CDC script
    # as an argument via the xcom_pull method
    # This print should be the last statement of the code..
    #print(next_seq_id_str)


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s: [%(levelname)s]: %(name)s: %(message)s")
    logging.getLogger("py4j").setLevel(logging.ERROR)
    main()
