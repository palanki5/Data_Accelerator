"""
Ingest Pre-Process is used as the File Names of certain sources are not in the "Standard Format" for the Ingestion FW to process
    Sources Handled:
        1. MAXIM
"""

import os
import sys
import time
import argparse
import json
import fnmatch
import logging
import zipfile
import subprocess

from ingestion_fw.metastore.sequence_pattern import get_next_seq_id


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


def maxim_file_rename(all_tables_file_patterns,jfile,next_batch_seq_id):
    logger = logging.getLogger(__name__)
    try:
        data_path = jfile["ingestion_config"]["location"]["base_dir"]
        inp_dir_path = jfile["ingestion_config"]["location"]["input_dir"]
        arc_dir_name = jfile["ingestion_config"]["location"]["archive_dir"]
        hdfs_home_dir = jfile["ingestion_config"]["hdfs_home_dir"]
        source = jfile["ingestion_config"]["source"]
        
        hdfs_inp_path = hdfs_home_dir + "/land/"+source+"/"+inp_dir_path+"/"
        hdfs_arc_path = hdfs_home_dir + "/land/"+source+"/"+arc_dir_name+"/"
        inp_dir_path = data_path + "/" + inp_dir_path+"/"
        arc_dir_path = data_path + "/" + arc_dir_name+"/"

        currenttablelist = [w.replace(source+'_','').replace('_'+next_batch_seq_id+'*', '').replace('_','-')+'_'+next_batch_seq_id for w in all_tables_file_patterns]

    
        # Create temp directory in the /tmp/ folder on the edge node for the unzip to happen
        unzip_dir_path = "/tmp/ingestion_fw"
        (rc, msg, err) = run_cmd(['mkdir', '-p', unzip_dir_path])
        if rc!=0:
            logger.error("ingest_pre_process():: Tmp Folder Creation Failed {0}".format(ex))
            sys.exit(1)
        (rc, msg, err) = run_cmd(['chmod', '-R', '777', unzip_dir_path])
        if rc!=0:
            logger.error("ingest_pre_process():: Folder Permissions Failed {0}".format(ex))
            sys.exit(1)

        for filename in os.listdir(inp_dir_path):
            for idx, item in enumerate(currenttablelist):
                if currenttablelist[idx] in filename and filename.endswith(".zip") and source in filename:
                    unzip_filename = filename.split(".")
                    src_file = unzip_dir_path+"/" + unzip_filename[0] +"."+ unzip_filename[1]
                    zip_filename = inp_dir_path + filename

                    zip_ref = zipfile.ZipFile(zip_filename)  # create zipfile object
                    zip_ref.extractall(unzip_dir_path)  # Extract file to temp directory local
                    zip_ref.close()  # close file

                    (rc, msg, err) = run_cmd(['hdfs', 'dfs', '-copyFromLocal', '-f', src_file , hdfs_inp_path])
                    if rc != 0:
                        logger.error("ingest_pre_process():: CopyFromLocal Failed {0}".format(ex))
                        sys.exit(1)
                    (rc, msg, err) = run_cmd(['hdfs', 'dfs', '-mv', hdfs_inp_path+filename , hdfs_arc_path])
                    if rc != 0:
                        logger.error("ingest_pre_process():: Move Zip to Archive Failed {0}".format(ex))
                        sys.exit(1)
                    (rc, msg, err) = run_cmd(['rm', src_file])
                    if rc!=0:
                        logger.error("ingest_pre_process():: Remove File Failed {0}".format(ex))
                        sys.exit(1)
        
        for filename in os.listdir(inp_dir_path):
            #file name must contain -,initial_seq_id and source
            for idx, item in enumerate(currenttablelist):
                if currenttablelist[idx] in filename and "-" in filename and source in filename:
                    if filename.endswith(".dat") or filename.endswith(".ctl") or filename.endswith(".eot"):
                        temp = filename.split(".")
                        temp2 = temp[0].replace("-", "_").split("_")
                        newfilename = "_".join(temp2[:-1])
                        (rc, msg, err) = run_cmd(['hdfs','dfs','-mv',hdfs_inp_path+filename,hdfs_inp_path + newfilename + "." + temp[1]])
                        if rc!=0:
                            logger.error("ingest_pre_process():: Renaming Failed {0}".format(ex))
                            sys.exit(1)

    except Exception as ex:
        logger.error("file_sensor_rename_maxim():: {0}".format(ex))
        sys.exit(1)

def run_cmd(args_list):
    """
    Method to execute Linux Commands...
    :param args_list: Linux command separated with each word supplied as arguments list
    :return: s_return: Return code of the linux command executed
             s_output: Output message of the linux command executed
             s_err: Error message of the linux command executed
    """
    logger = logging.getLogger(__name__)
    try:
        logger.debug('Running Command: {0}'.format(' '.join(args_list)))
        proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        s_output, s_err = proc.communicate()
        s_return = proc.returncode
        return s_return, s_output, s_err
    except Exception as ex:
        logger.error("run_cmd():: {0}".format(ex))


def ingest_pre_process():
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

        all_tables_file_patterns = get_file_patterns(jfile, next_batch_seq_id)

        maxim_file_rename(all_tables_file_patterns,jfile,next_batch_seq_id)

    except Exception as ex:
        logger.error("ingest_pre_process():: {0}".format(ex))
        sys.exit(1)


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s: [%(levelname)s]: %(name)s: %(message)s")

    ingest_pre_process()

