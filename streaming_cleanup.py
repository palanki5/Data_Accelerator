import sys
import argparse
import json
import logging
import subprocess
from datetime import datetime, timedelta

from utils.utils import run_cmd


def parse_cmd_args():
    """
    add the command line arguments that are to be parsed.
    :return: the parsed arguments
    """
    cmd_parser = argparse.ArgumentParser(description="streaming cleanup module")
    cmd_parser.add_argument("etl_config_file_path", help="path to the ETL configuration file for this job")
    cmd_args = cmd_parser.parse_args()
    return cmd_args


def repair_table(tbl_nm, db_nm):
    """
    Method to repair hive table, to update partition info
    :param tbl_params: dictionary to construct the target table name
    :return: result of the subprocess.call()
    """
    logger = logging.getLogger(__name__)
    try:
        tgt_tbl = db_nm + "." + tbl_nm
        logger.info("Updating partition info for [" + tgt_tbl + "]")
        msck_cmd = "MSCK REPAIR TABLE " + tgt_tbl
        result = subprocess.call(['hive', '--hiveconf', 'hive.execution.engine=mr',
                                  '--hiveconf', 'hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager',
                                  '-e ' + msck_cmd])
        return result
    except Exception as ex:
        logger.error("repair_table():: {0}".format(ex))
        # Exit the system
        sys.exit(1)


#Analyze Table Compute partition statistics
def analyze_table(tbl_nm, db_nm, partition_col):
    """
    Method to Analyze hive table, to update partition info
    :param tbl_params: dictionary to construct the target table name
    :return: result of the subprocess.call()
    """
    logger = logging.getLogger(__name__)
    try:
        tgt_tbl = db_nm + "." + tbl_nm
        logger.info("Analyze Table for [" + tgt_tbl + "]")
        analyze_cmd = "ANALYZE TABLE " + tgt_tbl + " PARTITION(" + partition_col + ") COMPUTE STATISTICS"
        result = subprocess.call(['hive', '--hiveconf', 'hive.execution.engine=mr',
                                  '--hiveconf', 'hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager',
                                  '-e ' + analyze_cmd])
        return result
    except Exception as ex:
        logger.error("analyze_table():: {0}".format(ex))
        # Exit the system
        sys.exit(1)


# Drop Hive Partitions
def drop_table_partitions(tbl_nm, db_nm, partition_col, partition_val):
    """
    Method to Drop the Hive Table Partitions which have been moved to Archive Path
    :param tbl_params: dictionary to construct the target table name
    :return: result of the subprocess.call()
    """
    logger = logging.getLogger(__name__)
    try:
        tgt_tbl = db_nm + "." + tbl_nm
        logger.info("Dropping Partitions for Table [" + tgt_tbl + "]")
        partition_val = str((datetime.now() + timedelta(-6)).strftime('%Y%m%d')) + "00"
        drop_partition_cmd = "ALTER TABLE " + tgt_tbl + " DROP IF EXISTS PARTITION(" + partition_col + "<'" + \
                               partition_val + "')"
        result = subprocess.call(['hive', '--hiveconf', 'hive.execution.engine=mr',
                                  '--hiveconf', 'hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager',
                                  '-e ' + drop_partition_cmd])
        return result
    except Exception as ex:
        logger.error("drop_table_partitions():: {0}".format(ex))
        # Exit the system
        sys.exit(1)


def archive_main():
    """
    Archieve module main to ()
    :return:
    """
    try:
        logger = logging.getLogger(__name__)
        args = parse_cmd_args()
        logger.info("Streaming Cleanup Process Started !!! ")
        jfile = json.loads(open(args.etl_config_file_path).read())
        source = jfile["ingestion_config"]["source"]
        hdfs_home_dir = jfile["ingestion_config"]["hdfs_home_dir"]
        archive_dir = hdfs_home_dir + "/archive"
        table_list = jfile["ingestion_config"]["datafeed"]
        for table_name in table_list:
            #Getting stream table location path:/data/b2b/curated/streaming/rpa
            hdfs_path = hdfs_home_dir + "/curated/streaming/" + source.lower()
            table_hdfs_path = hdfs_path + "/" + table_name

            #Get seven days old date
            seven_days_old_date = str((datetime.now() + timedelta(-7)).strftime('%Y%m%d'))
            #partition_col_name should be fetched from json file with attribute name stream_tbl_partition_col
            table_attr = jfile["ingestion_config"]["datafeed"][table_name]
            partition_col_name = table_attr["stream_tbl_partition_col"]
            partition_hdfs_path = table_hdfs_path+"/"+partition_col_name+"="+seven_days_old_date+"*"
            (rc, msg, err) = run_cmd(['hdfs', 'dfs', '-test', '-e', partition_hdfs_path])
            if rc != 0:
                logger.info("No Table partition File avaibale for Archiving of table:"+table_name + \
                            " for Date:"+seven_days_old_date)
            else:
                logger.info("Archiving table partition of date:"+seven_days_old_date+" for table:"+table_name)
                #Getting the archival path location as /data/b2b/archive/rpa/table_name
                archive_tbl_path = archive_dir + "/" + source.lower() + "/" + table_name
                tbl_partition_path = archive_tbl_path + "/" + seven_days_old_date
                (rc, msg, err) = run_cmd(['hdfs', 'dfs', '-test', '-e', tbl_partition_path])
                if rc != 0:
                    (rc, msg, err) = run_cmd(['hdfs', 'dfs', '-test', '-e', archive_tbl_path])
                    if rc != 0:
                        logger.info("Creating archival table path for :" + table_name)
                        (rc, msg, err) = run_cmd(['hdfs', 'dfs', '-mkdir', archive_tbl_path])
                        if rc != 0:
                            raise Exception(archive_tbl_path + " directory creation failed!!!")
                        logger.info("Archival table Path Created as: " + archive_tbl_path)
                    else:
                        (rc, msg, err) = run_cmd(['hdfs', 'dfs', '-mkdir', tbl_partition_path])
                        if rc != 0:
                            raise Exception(tbl_partition_path + " directory creation failed!!!")
                        logger.info("Table partition path Created as: " + tbl_partition_path)

                #run move command
                (rc, msg, err) = run_cmd(['hdfs', 'dfs', '-mv', partition_hdfs_path, tbl_partition_path])
                if rc != 0:
                    raise Exception("Table partition movement to Archive directory failed for table : " + table_name)
                else:
                    db_nm = table_attr["streaming_table_db"]
                    drop_table_partitions(table_name, db_nm, partition_col_name, seven_days_old_date)
                    repair_table(table_name, db_nm)
                    analyze_table(table_name, db_nm, partition_col_name)

    except Exception as ex:
        logger.error("archival main():: {0}".format(ex))
        # Exit the system
        sys.exit(1)


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s: [%(levelname)s]: %(name)s: %(message)s")

    archive_main()
