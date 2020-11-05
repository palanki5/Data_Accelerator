# coding=utf-8
import os
import sys
import time
import argparse
import json
import fnmatch
import logging

from metastore.sequence_pattern import get_next_seq_id
from utils.utils import get_file_name_and_ext
from utils.audit_utils import log_audit_entry,ingestion_audit_metrics_dict

def get_file_patterns(jfile, next_batch_seq_id):
    """
    Get the file patterns for all the tables and replace the sequence pattern with next batch seq id
    :param jfile: ETL json configuration file
    :param next_batch_seq_id: next batch seq id
    :return: list with all table's respective file patterns
    """
    logger = logging.getLogger(__name__)
    try:
        table_list = jfile["ingestion_config"]["datafeed"]
        file_pattern_list = []
        for table_name in table_list:
            table_attr = jfile["ingestion_config"]["datafeed"][table_name]
            file_pattern = table_attr["validation"]["triplet_check"]["file_pattern"]
            file_pattern = file_pattern.replace("(<batch_seq>)", next_batch_seq_id). \
                replace("(<date_seq>)", next_batch_seq_id)
            file_pattern_list.append(file_pattern)

        return file_pattern_list
    except Exception as ex:
        logger.error("get_file_patterns():: {0}".format(ex))
        sys.exit(1)


def get_next_seq_id_for_batch(jfile, db_connection_prop):
    """
    Get the next batch seq id for the batch
    :param jfile: ETL json configuration file
    :param db_connection_prop: database connection property file
    :return: Next Batch Seq ID
    """
    logger = logging.getLogger(__name__)
    try:
        # Get the information which are required to get the next batch sequence id
        feed_id = jfile["ingestion_config"]["feed_id"]
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
        seq_interval = jfile["ingestion_config"].setdefault("seq_interval", "daily").lower() if seq_type == "date" \
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
            logger.error("Previous run is still In-Progress.. Please process the previous batch first")
            sys.exit(1)

        # Pre-Fix leading zeros and convert to string type as seq_id is VARCHAR in control table
        next_seq_id_str = str(next_seq_id).zfill(num_seq_length) if num_seq_length is not None else str(next_seq_id)
        logger.info("File Sensor Next Sequence ID Value For This Execution Run..... {0}".format(next_seq_id_str))

        return next_seq_id_str
    except Exception as ex:
        logger.error("get_next_seq_id_for_batch():: {0}".format(ex))
        sys.exit(1)


def parse_cmd_args():
    """
    add the command line arguments that are to be parsed.
    :return: the parsed arguments
    """
    cmd_parser = argparse.ArgumentParser(description="Ingestion Framework File Watcher module")
    cmd_parser.add_argument("etl_config_file_path", help="path to the ETL configuration file for this job")
    cmd_parser.add_argument("db_connection", help="Postgres database connection property file")
    cmd_args = cmd_parser.parse_args()
    return cmd_args


def file_watch_process(jfile, all_tables_file_patterns):
    """
    Keep poking input directory for expected number of the extension files
    :param jfile: ETL json configuration file
    :param all_tables_file_patterns: list having file patterns for all the tables
    :return:
    """
    logger = logging.getLogger(__name__)
    try:
        start_time = time.time()
        period_of_time_to_wait = 10800  # Keep on checking for 3 hours before failing the job

        data_path = jfile["ingestion_config"]["location"]["base_dir"]
        inp_dir_name = jfile["ingestion_config"]["location"]["input_dir"]
        inp_dir_path = data_path + "/" + inp_dir_name

        # Get the File Watcher related information
        expected_ext = jfile["ingestion_config"]["file_sensor"].setdefault("expected_ext", "eot").replace(".", "")
        expected_ext = "." + expected_ext
        expected_ext_count = jfile["ingestion_config"]["file_sensor"].setdefault("expected_ext_count", 0)

        # Audit Logging
        audit_data_source = jfile["ingestion_config"]["source"]
        audit_feed_id = jfile["ingestion_config"]["feed_id"]
        audit_splitter = "|"
        next_batch_seq_id=jfile["ingestion_config"]["next_batch_seq_id"]
        db_connection_prop=jfile["ingestion_config"]["db_connection_prop"]

        # Sleep for 5 minutes until the number of files meet the number of expected files criteria
        while 1:
            matched_patterns = []

            # Check for the files in input directory after each 2 minutes interval
            for element in all_tables_file_patterns:
                file_pattern_with_expected_ext = element + expected_ext.lower()
                for filename in os.listdir(inp_dir_path):
                    file, ext = get_file_name_and_ext(filename)
                    filename_to_be_matched = file + ext.lower()
                    if fnmatch.fnmatch(filename_to_be_matched, file_pattern_with_expected_ext):
                        matched_patterns.append(file_pattern_with_expected_ext)
                if file_pattern_with_expected_ext not in matched_patterns:
                    logger.info("Unable to fetch the files matching the Pattern : {0}".
                                format(file_pattern_with_expected_ext))

            # Audit Logging
            audit_all_file_patterns = audit_splitter.join(all_tables_file_patterns);
            audit_matched_file_patterns=audit_splitter.join(matched_patterns);

            if len(matched_patterns) == expected_ext_count:
                logger.info("file_watch_process():: {0}".format("All data files have been received ..."))

                # Audit Logging
                metrics=ingestion_audit_metrics_dict()
                metrics.update({"data_source":audit_data_source,"success_msg":"All data files have been received",
                                "all_file_patterns":audit_all_file_patterns,"matched_file_patterns":audit_matched_file_patterns})
                log_dict={"run_id": "'file_sensor'","key":"'"+str(next_batch_seq_id)+"|"+str(audit_feed_id)+"'","event_type": "'Ingestion'",
                          "event": "'FILE_SENSOR'","source": "'"+inp_dir_path+"'",
                          "target": "'NA'","source_type": "'Directory'","target_type": "'NA'",
                          "status": "'Success'","metrics": metrics}
                log_audit_entry(log_dict,db_connection_prop)

                sys.exit(0)

            # Audit Logging
            metrics=ingestion_audit_metrics_dict()
            metrics.update({"data_source":audit_data_source,"error_msg":"Data files not received","error_type":"FILE SENSOR ERROR",
                            "all_file_patterns":audit_all_file_patterns,"matched_file_patterns":audit_matched_file_patterns})
            log_dict={"run_id": "'file_sensor'","key":"'"+str(next_batch_seq_id)+"|"+str(audit_feed_id)+"'","event_type": "'Ingestion'",
                      "event": "'FILE_SENSOR'","source": "'"+inp_dir_path+"'",
                      "target": "'NA'","source_type": "'Directory'","target_type": "'NA'",
                      "status": "'Failed'","metrics": metrics}
            log_audit_entry(log_dict,db_connection_prop)

            if time.time() > start_time + period_of_time_to_wait:
                logger.error("file_watch_process():: {0}".format("Data files not received ..."))
                sys.exit(1)
            time.sleep(300)
    except Exception as ex:
        logger.error("file_watch_process():: {0}".format(ex))
        sys.exit(1)


def file_sensor_main():
    """
    File Sensor Main Method
    :return: Successful return after receiving all the required extension files
    """
    logger = logging.getLogger(__name__)
    try:
        args = parse_cmd_args()

        # postgres database connection property file
        db_connection_prop = args.db_connection

        # Read the ETL configuration file for the job
        jfile = json.loads(open(args.etl_config_file_path).read())

        next_batch_seq_id = get_next_seq_id_for_batch(jfile, db_connection_prop)

        # Audit Logging
        audit_data_source = jfile["ingestion_config"]["source"]
        audit_feed_id = jfile["ingestion_config"]["feed_id"]
        jfile["ingestion_config"]["next_batch_seq_id"]=next_batch_seq_id
        jfile["ingestion_config"]["db_connection_prop"]=db_connection_prop

        metrics=ingestion_audit_metrics_dict()
        metrics.update({"data_source":audit_data_source})
        log_dict={"run_id": "'file_sensor'","key":"'"+str(next_batch_seq_id)+"|"+str(audit_feed_id)+"'","event_type": "'Ingestion'",
                  "event": "'FILE_SENSOR'","source": "'NA'",
                  "target": "'NA'","source_type": "'NA'","target_type": "'NA'",
                  "status": "'Initiated'","metrics": metrics}
        log_audit_entry(log_dict,db_connection_prop)

        all_tables_file_patterns = get_file_patterns(jfile, next_batch_seq_id)

        file_watch_process(jfile, all_tables_file_patterns)
    except Exception as ex:
        logger.error("file_sensor_main():: {0}".format(ex))
        sys.exit(1)


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s: [%(levelname)s]: %(name)s: %(message)s")

    file_sensor_main()