# coding=utf-8
import os
import sys
import argparse
import json
import logging
import datetime
import shutil

from ingestion_fw.utils.utils import move_files_to_dst, get_seq_id_for_succesful_run, delete_dir, create_dir
from ingestion_fw.utils.audit_utils import log_audit_entry,ingestion_audit_metrics_dict

# Move every files/folder to trash folder which need to be deleted.
trash_dir = "/dbfs/mnt/ops/deleted-files/"
create_dir(trash_dir)

def parse_cmd_args():
    """
    add the command line arguments that are to be parsed.
    :return: the parsed arguments
    """
    cmd_parser = argparse.ArgumentParser(description="Ingestion Framework Archival module")
    cmd_parser.add_argument("etl_config_file_path", help="path to the ETL configuration file for this job")
    cmd_parser.add_argument("db_connection", help="Postgres database connection property file")
    cmd_args = cmd_parser.parse_args()
    return cmd_args


def archival_process(jfile, table_list, inp_dir_name, archive_dir_name, next_batch_seq_num):
    logger = logging.getLogger(__name__)
    try:
        # Audit Logging
        audit_data_source = jfile["ingestion_config"]["source"]
        audit_feed_id = jfile["ingestion_config"]["feed_id"]
        next_batch_seq_num=jfile["ingestion_config"]["next_batch_seq_num"]
        db_connection_prop=jfile["ingestion_config"]["db_connection_prop"]
        audit_splitter="|"
        audit_table_str=audit_splitter.join(table_list)

        for table_name in table_list:
            table_attr = jfile["ingestion_config"]["datafeed"][table_name]
            file_pattern = table_attr["validation"]["triplet_check"]["file_pattern"]
            file_pattern = file_pattern.replace("(<batch_seq>)", next_batch_seq_num). \
                replace("(<date_seq>)", next_batch_seq_num)
            move_files_to_dst(inp_dir_name, archive_dir_name, file_pattern)
            home_dir = jfile["ingestion_config"]["home_dir"]
            data_source = jfile["ingestion_config"]["source"]
            staging_dir = "/dbfs/mnt" + home_dir + "/" + "staging"
            source_dir = staging_dir + "/" + data_source
            #table_path = source_dir + "/" + table_name + "/" + file_pattern
            table_path = source_dir + "/" + table_name
            #delete_dir(table_path)
            trash_path = os.path.join(trash_dir, datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S-%f'))
            create_dir(trash_path)
            move_files_to_dst(table_path, trash_path, file_pattern)
            logger.info("Archival Process Completed !!! ")

        # Audit Logging
        metrics=ingestion_audit_metrics_dict()
        metrics.update({"data_source":audit_data_source,"success_msg":"Archival Process Completed","table_name":audit_table_str})
        log_dict={"run_id": "'archival'","key":"'"+next_batch_seq_num+"|"+str(audit_feed_id)+"'","event_type": "'Ingestion'",
                  "event": "'ARCHIVAL'","source": "'"+inp_dir_name+"'",
                  "target": "'"+archive_dir_name+"'","source_type": "'Input Directory'","target_type": "'Archive Directory'",
                  "status": "'Success'","metrics": metrics}
        log_audit_entry(log_dict,db_connection_prop)

    except Exception as ex:
        logger.error("archive_main():: {0}".format(ex))

        # Audit Logging
        metrics=ingestion_audit_metrics_dict()
        metrics.update({"data_source":audit_data_source,"error_msg":"Archival Process Failed","table_name":audit_table_str})
        log_dict={"run_id": "'archival'","key":"'"+next_batch_seq_num+"|"+str(audit_feed_id)+"'","event_type": "'Ingestion'",
                  "event": "'ARCHIVAL'","source": "'"+inp_dir_name+"'",
                  "target": "'"+archive_dir_name+"'","source_type": "'Input Directory'","target_type": "'Archive Directory'",
                  "status": "'Failed'","metrics": metrics}
        log_audit_entry(log_dict,db_connection_prop)

        sys.exit(1)


def archive_main():
    """
    Archieve module main()
    :return:
    """
    logger = logging.getLogger(__name__)
    try:
        args = parse_cmd_args()
        logger.info("Archival Process Started !!! ")

        # postgres database connection property file
        db_connection_prop = args.db_connection

        # Read the ETL configuration file for the job
        jfile = json.loads(open(args.etl_config_file_path).read())
        feed_id = jfile["ingestion_config"]["feed_id"]
        data_path = jfile["ingestion_config"]["location"]["base_dir"]
        inp_dir_name = jfile["ingestion_config"]["location"]["input_dir"]
        archive_dir_name = jfile["ingestion_config"]["location"].setdefault("archive_dir", "Archive")
        num_seq_length = jfile["ingestion_config"]["start_sequence"].setdefault("length", "")
        num_seq_length = int(num_seq_length) if num_seq_length else None
        table_list = jfile["ingestion_config"]["datafeed"]

        inp_dir_path = "/dbfs/mnt" + data_path + "/" + inp_dir_name
        archive_dir_path = "/dbfs/mnt" + data_path + "/" + archive_dir_name
        if not os.path.exists(archive_dir_path):
            os.mkdir(archive_dir_path)
            logger.info("Archive dir created as it was not present")

        # Get the latest seq_id with "Success" run from Batch control table.
        next_seq_id = get_seq_id_for_succesful_run(feed_id, prop_file_path=db_connection_prop)
        next_batch_seq_num = str(next_seq_id).zfill(num_seq_length) if num_seq_length is not None else str(next_seq_id)

        # Audit Logging
        audit_data_source = jfile["ingestion_config"]["source"]
        jfile["ingestion_config"]["next_batch_seq_num"]=next_batch_seq_num
        jfile["ingestion_config"]["db_connection_prop"]=db_connection_prop

        metrics=ingestion_audit_metrics_dict()
        metrics.update({"data_source":audit_data_source})
        log_dict={"run_id": "'archival'","key":"'"+next_batch_seq_num+"|"+str(feed_id)+"'","event_type": "'Ingestion'",
                  "event": "'ARCHIVAL'","source": "'NA'",
                  "target": "'NA'","source_type": "'NA'","target_type": "'NA'",
                  "status": "'Initiated'","metrics": metrics}
        log_audit_entry(log_dict,db_connection_prop)

        archival_process(jfile, table_list, inp_dir_path, archive_dir_path, next_batch_seq_num)
    except Exception as ex:
        logger.error("archive_main():: {0}".format(ex))
        sys.exit(1)


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s: [%(levelname)s]: %(name)s: %(message)s")
    logging.getLogger("py4j").setLevel(logging.ERROR)

    archive_main()