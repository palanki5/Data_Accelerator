# coding=utf-8
import sys
import argparse
import logging
import json
import csv
import hashlib

from datetime import datetime
from ingestion_fw.utils.config import config
from ingestion_fw.utils.utils import ret_struct_fields, cast_to_py_types
from ingestion_fw.utils.utils import get_batch_seq_id_for_this_run, update_record_batch_seq_detail
from ingestion_fw.utils.error_accumulator import ListErrorAccumulator
from ingestion_fw.metastore.dbmanager import DbManager

from ingestion_fw.cdc.merge_strategy_factory import get_merge_strategy

from ingestion_fw.utils.utils import get_seq_id_for_this_run
from ingestion_fw.utils.audit_utils import log_audit_entry, ingestion_audit_metrics_dict
from ingestion_fw.utils.utils import remove_enclosed_quotes

from pyspark.sql import SparkSession
from pyspark import SparkFiles
from pyspark.sql.types import StructType
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import lit, col, array, udf, concat, lower
from pyspark import StorageLevel


def launch_cdc(spark, cdc_params):
    """
    launch the CDC operations for the batch of files configured
    :param spark: the SparkSession
    :param cdc_params: dict with the CDC related parameters
    :return:
    """
    logger = logging.getLogger(__name__)
    table_list = cdc_params['table_list']
    feed_id = cdc_params["feed_id"]
    source = cdc_params["source"]

    # Postgres Database connection property distributed file
    db_conn_prop_file = cdc_params["db_connection"]

    # Create Spark Accumulator for Error logging
    global error_accumulator
    error_accumulator = spark.sparkContext.accumulator({}, ListErrorAccumulator())

    # TODO handle exceptions
    # TODO check for the possibility of staged, active, history dfs to be empty
    logger.info("launch_cdc():: starting CDC for batch_id = " + cdc_params['batch_id'])
    logger.info("launch_cdc():: CDC pattern for batch_id " + cdc_params['batch_id'] + " is " +
                cdc_params['cdc_pattern'])
    current_batch_seq_id = get_batch_seq_id_for_this_run(feed_id, prop_file_path=db_conn_prop_file)

    # Audit Logging
    audit_seq_id = cdc_params['batch_id']
    metrics = ingestion_audit_metrics_dict()
    metrics.update({"data_Source": source})
    log_dict = {"run_id": "'" + str(current_batch_seq_id) + "'",
                "key": "'" + str(audit_seq_id) + "|" + str(feed_id) + "'", "event_type": "'Ingestion'",
                "event": "'CDC'", "source": "'NA'",
                "target": "'NA'", "source_type": "'NA'", "target_type": "'NA'",
                "status": "'Initiated'", "metrics": metrics}

    log_audit_entry(log_dict, db_conn_prop_file)

    logger.info("launch_cdc():: batch seq id for this run is = {0}".format(current_batch_seq_id))

    if current_batch_seq_id == "" or current_batch_seq_id is None:
        logger.error("launch_cdc():: previous run status is either success or failed. please update the status"
                     " to In-Progress in batch_seq_detail table for last run and re-run the job")
        sys.exit(1)

    cdc_params["batch_seq_id"] = current_batch_seq_id

    if len(table_list) > 0:
        table_df_list = []
        table_df = {}
        for table_name in table_list:
            try:
                logger.info("launch_cdc():: initiating table validation for " + table_name)
                table_conf = cdc_params[table_name]

                # Get the batch id passed in cdc params
                batch_id = cdc_params["batch_id"]
                table_conf["batch_id"] = cdc_params["batch_id"]

                # Get the active and history table names and use those while writing into hive tables
                # active_table = table_conf['cdc_config']['active_table']
                # history_table = table_conf['cdc_config']['history_table']
                key_columns = table_conf['cdc_config']['key_cols']

                staged_df = stage_data(spark, cdc_params, table_name)
                b2b_key_identifier = source + '_' + table_name
                # TODO: De-duplicate staged records?
                # Additional attributes are added to the staged data
                staged_df = enrich_staged_data(staged_df, batch_id, key_columns, table_conf, b2b_key_identifier)
                staged_df.persist(StorageLevel.MEMORY_AND_DISK)
                staged_df_size = staged_df.count()
                logger.info(
                    "launch_cdc():: Number of row in table " + table_name + " dataframe is: " + str(staged_df_size))

                table_df.update({table_name: staged_df})
                table_df_list.append(table_df)

                if len(error_accumulator.value) > 0:
                    error_table = cdc_params["ingestion_fw_config"].setdefault("error_table", "")
                    error_df = format_error_bucket(spark, error_accumulator.value)
                    if not error_table:
                        raise Exception("No ERROR Table information provided in ingestion framework config ini file ")
                    write_into_error_table(error_df, error_table)
                    update_record_batch_seq_detail(status="Failed",
                                                   batch_seq_id=current_batch_seq_id,
                                                   prop_file_path=db_conn_prop_file)
                    logger.error("launch_cdc():: batch_id = " + cdc_params['batch_id'] + " table = " + table_name +
                                 " table error found")
                    sys.exit(1)
                logger.info("launch_cdc():: table validation for  " + table_name + " completed successfully")

            except Exception as ex:
                logger.exception(
                    "launch_cdc():: Error while table validation of table " + table_name + ":: {0}".format(ex))
                # Audit Logging
                metrics = ingestion_audit_metrics_dict()
                metrics.update({"error_msg": "File Validation of table has Failed ", "error_type": "CDC Error",
                                "table_name": table_name})
                log_dict = {"run_id": "'" + str(current_batch_seq_id) + "'",
                            "key": "'" + str(audit_seq_id) + "|" + str(feed_id) + "'",
                            "event_type": "'Ingestion'",
                            "event": "'CDC'", "source": "'NA'",
                            "target": "'NA'", "source_type": "'NA'", "target_type": "'NA'",
                            "status": "'Failed'", "metrics": metrics}
                log_audit_entry(log_dict, db_conn_prop_file)

                # need to update Failed status for jdbc import if current cdc_params[table_name]["format"].lower()
                # is jdbc_pull
                if cdc_params[table_name]["format"].lower() == 'jdbc_pull':
                    update_jdbc_pull_audit(
                        current_batch_seq_id=current_batch_seq_id,
                        prop_file_path=db_conn_prop_file
                    )

                # In case of failure, update the batch seq detail table with status as failed
                update_record_batch_seq_detail(status="Failed",
                                               batch_seq_id=current_batch_seq_id,
                                               prop_file_path=db_conn_prop_file)
                sys.exit(1)
        # for table in table_df_list :
        #   for table_name,staged_df in table.iteritems():
        logger.info("launch_cdc():: All cdc tables are validated !!! Now going for CDC process")
        audit_home_dir = cdc_params["home"]
        audit_seq_id = cdc_params['batch_id']
        for table_name, staged_df in table_df.iteritems():
            try:

                audit_file_pattern = cdc_params[table_name]["file_pattern"]
                audit_file_pattern = audit_file_pattern.replace("(<batch_seq>)", str(current_batch_seq_id)).replace(
                    "(<date_seq>)", str(current_batch_seq_id))

                audit_stg_file = audit_home_dir + "/" + "staging" + "/" + source + "/" + table_name + "/" + audit_file_pattern

                # Audit Logging
                metrics = ingestion_audit_metrics_dict()
                metrics.update({"data_source": source, "table_name": table_name})
                log_dict = {"run_id": "'" + str(current_batch_seq_id) + "'",
                            "key": "'" + str(audit_seq_id) + "|" + str(feed_id) + "'",
                            "event_type": "'Ingestion'",
                            "event": "'CDC'", "source": "'" + audit_stg_file + "'",
                            "target": "'" + table_name + "'", "source_type": "'Staged_DF'", "target_type": "'Table'",
                            "status": "'Initiated'", "metrics": metrics}
                logger.info("log_dict: {}".format(str(log_dict)))

                log_audit_entry(log_dict, db_conn_prop_file)

                # Create Dataframe of the 'Active' table. It is assumed that the Active table has already been created.
                table_conf = cdc_params[table_name]
                act_tab = table_conf['cdc_config']['active_table']
                active_table = act_tab.replace("env", cdc_params["ingestion_fw_config"]["env"]).replace("<", "") \
                    .replace(">", "") if "<env>" in act_tab else act_tab
                hist_tab = table_conf['cdc_config'].setdefault('history_table', "")
                history_table = hist_tab.replace("env", cdc_params["ingestion_fw_config"]["env"]).replace("<", "") \
                    .replace(">", "") if "<env>" in hist_tab else hist_tab

                cdc_pattern = cdc_params['cdc_pattern'].lower()
                # For Active View Merge Strategy, physical active table will not be present
                if cdc_pattern == 'delta_load_av':
                    active_df = None
                else:
                    active_df = spark.table(active_table)

                # Get the MergeStrategy appropriate for the cdc pattern
                merge_strategy = get_merge_strategy(cdc_pattern, table_conf, spark)

                if cdc_pattern == 'one_time_load':
                    # Use the history table to determine the active records
                    logger.info("launch_cdc():: creating active snapshot for table " + table_name +
                                " for CDC pattern " + 'one_time_load')
                    # It is assumed that the history table has already been created
                    history_df = spark.table(history_table)
                    # Append the staged records to the History dataframe.
                    upd_history_df = history_df.unionAll(staged_df)
                    logger.debug("launch_cdc():: batch_id = " + cdc_params['batch_id'] + " table = " + table_name +
                                 " staged records added to history table")
                    records_for_history_df = merge_strategy.get_records_for_history(staged_df)

                    # Audit Logging
                    metrics = ingestion_audit_metrics_dict()
                    metrics.update({"data_source": source, "cdc_pattern": cdc_pattern,
                                    "table_name": table_name, "success_msg": "History DF Created Successfully"})
                    log_dict = {"run_id": "'" + str(current_batch_seq_id) + "'",
                                "key": "'" + str(audit_seq_id) + "|" + str(feed_id) + "'",
                                "event_type": "'Ingestion'",
                                "event": "'CDC'", "source": "'" + audit_stg_file + "'",
                                "target": "'" + history_table + "'", "source_type": "'Staged_DF'",
                                "target_type": "'History_DF'",
                                "status": "'Completed'", "metrics": metrics}

                    log_audit_entry(log_dict, db_conn_prop_file)

                    new_active_df = merge_strategy.get_records_for_active(upd_history_df, active_df, cdc_params)

                    # Audit Logging
                    metrics = ingestion_audit_metrics_dict()
                    metrics.update({"data_source": source, "cdc_pattern": cdc_pattern,
                                    "table_name": table_name, "success_msg": "Active DF Created Successfully"})
                    log_dict = {"run_id": "'" + str(current_batch_seq_id) + "'",
                                "key": "'" + str(audit_seq_id) + "|" + str(feed_id) + "'",
                                "event_type": "'Ingestion'",
                                "event": "'CDC'", "source": "'" + audit_stg_file + "'",
                                "target": "'" + active_table + "'", "source_type": "'Staged_DF'",
                                "target_type": "'Active_DF'",
                                "status": "'Completed'", "metrics": metrics}

                    log_audit_entry(log_dict, db_conn_prop_file)
                elif cdc_pattern == 'exclude_append':
                    logger.info("launch_cdc():: creating active snapshot for table " + table_name +
                                " for CDC pattern " + cdc_params['cdc_pattern'])
                    modified_staged_df = merge_strategy.get_records_for_history(staged_df)
                    records_for_history_df = modified_staged_df

                    # Audit Logging
                    metrics = ingestion_audit_metrics_dict()
                    metrics.update({"data_source": source, "cdc_pattern": cdc_pattern,
                                    "table_name": table_name, "success_msg": "History DF Created Successfully"})
                    log_dict = {"run_id": "'" + str(current_batch_seq_id) + "'",
                                "key": "'" + str(audit_seq_id) + "|" + str(feed_id) + "'",
                                "event_type": "'Ingestion'",
                                "event": "'CDC'", "source": "'" + audit_stg_file + "'",
                                "target": "'" + history_table + "'", "source_type": "'Staged_DF'",
                                "target_type": "'History_DF'",
                                "status": "'Completed'", "metrics": metrics}

                    log_audit_entry(log_dict, db_conn_prop_file)

                    new_active_df = merge_strategy.get_records_for_active(modified_staged_df , active_df, cdc_params)

                    # Audit Logging
                    metrics = ingestion_audit_metrics_dict()
                    metrics.update({"data_source": source, "cdc_pattern": cdc_pattern,
                                    "table_name": table_name, "success_msg": "Active DF Created Successfully"})
                    log_dict = {"run_id": "'" + str(current_batch_seq_id) + "'",
                                "key": "'" + str(audit_seq_id) + "|" + str(feed_id) + "'",
                                "event_type": "'Ingestion'",
                                "event": "'CDC'", "source": "'" + audit_stg_file + "'",
                                "target": "'" + active_table + "'", "source_type": "'Staged_DF'",
                                "target_type": "'Active_DF'",
                                "status": "'Completed'", "metrics": metrics}

                    log_audit_entry(log_dict, db_conn_prop_file)
                else:
                    logger.info("launch_cdc():: creating active snapshot for table " + table_name +
                                " for CDC pattern " + cdc_params['cdc_pattern'])
                    records_for_history_df = merge_strategy.get_records_for_history(staged_df)

                    # Audit Logging
                    metrics = ingestion_audit_metrics_dict()
                    metrics.update({"data_source": source, "cdc_pattern": cdc_pattern,
                                    "table_name": table_name, "success_msg": "History DF Created Successfully"})
                    log_dict = {"run_id": "'" + str(current_batch_seq_id) + "'",
                                "key": "'" + str(audit_seq_id) + "|" + str(feed_id) + "'",
                                "event_type": "'Ingestion'",
                                "event": "'CDC'", "source": "'" + audit_stg_file + "'",
                                "target": "'" + history_table + "'", "source_type": "'Staged_DF'",
                                "target_type": "'History_DF'",
                                "status": "'Completed'", "metrics": metrics}

                    log_audit_entry(log_dict, db_conn_prop_file)

                    new_active_df = merge_strategy.get_records_for_active(staged_df, active_df, cdc_params)

                    # Audit Logging
                    metrics = ingestion_audit_metrics_dict()
                    metrics.update({"data_source": source, "cdc_pattern": cdc_pattern,
                                    "table_name": table_name, "success_msg": "Active DF Created Successfully"})
                    log_dict = {"run_id": "'" + str(current_batch_seq_id) + "'",
                                "key": "'" + str(audit_seq_id) + "|" + str(feed_id) + "'",
                                "event_type": "'Ingestion'",
                                "event": "'CDC'", "source": "'" + audit_stg_file + "'",
                                "target": "'" + active_table + "'", "source_type": "'Staged_DF'",
                                "target_type": "'Active_DF'",
                                "status": "'Completed'", "metrics": metrics}

                    log_audit_entry(log_dict, db_conn_prop_file)

                # Write to History moved before Write to Active. ## Active View and Active Persistent Strategy
                logger.info("launch_cdc():: Writing into History table for " + table_name)
                merge_strategy.write_into_history_table(records_for_history_df, history_table)

                # Audit Logging
                metrics = ingestion_audit_metrics_dict()
                if history_table:
                    audit_history_df = spark.table(history_table)
                    audit_hist_cnt = audit_history_df.count()
                else:
                    audit_hist_cnt = 0
                metrics.update({"data_source": source, "cdc_pattern": cdc_pattern,
                                "success_msg": "History Table Loaded Successfully",
                                "table_name": table_name, "source_row_count": staged_df_size,
                                "target_row_count": audit_hist_cnt})
                log_dict = {"run_id": "'" + str(current_batch_seq_id) + "'",
                            "key": "'" + str(audit_seq_id) + "|" + str(feed_id) + "'",
                            "event_type": "'Ingestion'",
                            "event": "'CDC'", "source": "'" + audit_stg_file + "'",
                            "target": "'" + history_table + "'", "source_type": "'Staged_DF'",
                            "target_type": "'History_Table'",
                            "status": "'Completed'", "metrics": metrics}

                log_audit_entry(log_dict, db_conn_prop_file)

                logger.info("launch_cdc():: Writing into active table for " + table_name)
                merge_strategy.write_into_active_table(spark, new_active_df, active_table)
                # Audit Logging
                metrics = ingestion_audit_metrics_dict()
                if active_df == None:
                    audit_actv_cnt = 0
                else:
                    audit_active_df = spark.table(active_table)
                    audit_actv_cnt = audit_active_df.count()

                metrics.update({"data_source": source, "cdc_pattern": cdc_pattern,
                                "success_msg": "CDC Process Completed Successfully",
                                "table_name": table_name, "source_row_count": staged_df_size,
                                "target_row_count": audit_actv_cnt})
                log_dict = {"run_id": "'" + str(current_batch_seq_id) + "'",
                            "key": "'" + str(audit_seq_id) + "|" + str(feed_id) + "'",
                            "event_type": "'Ingestion'",
                            "event": "'CDC'", "source": "'" + audit_stg_file + "'",
                            "target": "'" + active_table + "'", "source_type": "'Staged_DF'",
                            "target_type": "'Active_Table'",
                            "status": "'Completed'", "metrics": metrics}

                log_audit_entry(log_dict, db_conn_prop_file)

                logger.info("launch_cdc():: CDC process for " + table_name + " has completed successfully")

            except Exception as ex:
                logger.error("launch_cdc():: Error while CDC process of table " + table_name + "::{0}".format(ex))
                # Audit Logging
                metrics = ingestion_audit_metrics_dict()
                metrics.update({"error_msg": "Error while CDC process of table ", "error_type": "CDC Error",
                                "table_name": table_name})
                log_dict = {"run_id": "'" + str(current_batch_seq_id) + "'",
                            "key": "'" + str(audit_seq_id) + "|" + str(feed_id) + "'",
                            "event_type": "'Ingestion'",
                            "event": "'CDC'", "source": "'NA'",
                            "target": "'NA'", "source_type": "'NA'", "target_type": "'NA'",
                            "status": "'Failed'", "metrics": metrics}

                log_audit_entry(log_dict, db_conn_prop_file)
                # need to update Failed status for jdbc import if current cdc_params[table_name]["format"].lower()
                # is jdbc pull
                if cdc_params[table_name]["format"].lower() == 'jdbc_pull':
                    update_jdbc_pull_audit(
                        current_batch_seq_id=current_batch_seq_id,
                        prop_file_path=db_conn_prop_file
                    )

                # In case of failure, update the batch seq detail table with status as failed
                update_record_batch_seq_detail(status="Failed",
                                               batch_seq_id=current_batch_seq_id,
                                               prop_file_path=db_conn_prop_file)
                raise Exception("launch_cdc():: Error while CDC process of table " + table_name + "::{0}".format(ex))

        # Invalidate the cache in Spark by running 'REFRESH TABLE tableName'
        spark.sql("REFRESH TABLE " + active_table)
        spark.sql("REFRESH TABLE " + history_table)

        update_record_batch_seq_detail(status="Success",
                                       batch_seq_id=current_batch_seq_id,
                                       prop_file_path=db_conn_prop_file)
    else:
        logger.info("launch_cdc():: There are no tables configured for CDC")


def format_error_bucket(spark, err_bucket):
    """
    Formats the error acculumator to covert the information to csv format
    :param err_bucket: dictionary of error - {'column_name': [{error_1}, {error_2}]}
    :return:
    """
    logger = logging.getLogger(__name__)
    try:
        err_docs = []
        for err_record_list in err_bucket.values():
            for err_record in err_record_list:
                # since the accumulator is initialized per executor and if the jobs have multiple tasks
                # err_record can be a list of dict instead of dict, so we need to iterate accordingly
                # E.g., assume the data is split in two executors each with an error record for the same column then
                # in executor 1 if adding error record for col1, inside ListErrorAccumulator when the first record
                # is added, it doesn't find an entry so accumulator will be {'col1':[{'emp_id':'123'}]}
                # in executor 2 if adding error record for col1, inside ListErrorAccumulator when the first record
                # is added, it doesn't find an entry so accumulator will be {'col1':[{'emp_id':'123'}]}
                # Finally when the driver attempts to collect and merge or compute the value of accumulator (acc.value)
                # the expected value will become as follows:
                # executor1 = {'col1':[{'emp_id':'123'}]}
                # executor2 = {'col1':[{'emp_id':'123'}]}
                # in the driver when acc.value is invoked
                # as per method def addInPlace in ListErrorAccumulator(err_accumulator.py),
                # final_dict = {'col1':[{'emp_id':'123'}]}
                # new_dict = {'col1':[{'emp_id':'123'}]}
                # if final_dict.get('col1'):
                #     val = final_dict.get('col1')
                #     val.append(new_dict.get('col1'))
                #     final_dict['col1'] = val
                # final_dict/ acc.val = {'col1': [{'emp_id': '123'}, [{'emp_id': '123'}]]}
                # and when we call the final_dict.values() = [[{'emp_id': '123'}, [{'emp_id': '123'}]]]
                # which results in the list of dicts.....
                if isinstance(err_record, list):
                    for err_record_item in err_record:
                        err_docs.append(err_record_item)
                else:
                    # All the error records for the column are collected in a single executor or there is only one
                    # error record for this column
                    err_docs.append(err_record)

                from collections import OrderedDict
                from pyspark.sql import Row

                # Convert the err_docs which is list of dictionary into RDD
                err_rdd = spark.sparkContext.parallelize(err_docs) \
                    .map(lambda x: Row(**OrderedDict(sorted(x.items()))))
                from pyspark.sql.types import StructField, StringType

                err_schema = StructType([
                    StructField('actual_value', StringType(), True),
                    StructField('batch_id', StringType(), True),
                    StructField('batch_seq_id', StringType(), True),
                    StructField('col_name', StringType(), True),
                    StructField('data_type', StringType(), True),
                    StructField('err_msg', StringType(), True),
                    StructField('feed_id', StringType(), True),
                    # StructField('given_value', StringType(), True),
                    StructField('record', StringType(), True),
                    StructField('run_time', StringType(), True),
                    StructField('source', StringType(), True)
                ])

                err_df = spark.createDataFrame(err_rdd, err_schema)
        return err_df
    except Exception as ex:
        logger.error("format_error_bucket():: {0}".format(ex))


def enrich_staged_data(staged_df, batch_id, key_columns, table_conf, b2b_key_identifier):
    """
    Augment the Staged DataFrame with additional columns - batch_id, update_ts_col
    :param staged_df: the Staged Dataframe
    :param batch_id: the batch number
    :param update_ts_col: the update
    :return: the staged DataFrame with the added columns
    """

    # Enrich the dataframe with B2B Batch ID
    staged_df = staged_df.withColumn("b2b_batch_id", lit(batch_id))

    # Enrich the dataframe with B2B Insert Timestamp
    staged_df = staged_df.withColumn("b2b_insert_timestamp", current_timestamp())

    # Enrich the dataframe with B2B Key
    from pyspark.sql.types import StringType

    concat_udf = udf(lambda cols: "_".join([str(x) if x is not None else "" for x in cols]), StringType())
    hashKey = udf(hashKeyGenerator)

    staged_df = staged_df.withColumn('primary_key',
                                     lower(concat(lit(b2b_key_identifier), lit('_'), concat_udf(array(key_columns)))))
    staged_df = staged_df.withColumn('b2b_key', hashKey(col('primary_key')))
    staged_df = staged_df.drop('primary_key')

    return staged_df


def create_spark_session(cdc_params):
    app_name = cdc_params['app_name']
    source_table = cdc_params['source']
    spark = SparkSession.builder.appName(
        "Ingestion Job [" + app_name + "] " + " -- CDC -- " + source_table).enableHiveSupport().getOrCreate()

    return spark


def hashKeyGenerator(data):
    encodeData = hashlib.md5(data.encode())
    return encodeData.hexdigest()


def remove_headerfooter(data_rdd, header_flag, footer_flag):
    if header_flag == 'y' and footer_flag == 'y':
        size = data_rdd.count()
        return data_rdd.zipWithIndex().filter(lambda vi: (vi[1] > 0) & (vi[1] < size - 1)).keys()
    elif header_flag == 'y':
        return data_rdd.zipWithIndex().filter(lambda vi: (vi[1] > 0)).keys()
    elif footer_flag == 'y':
        size = data_rdd.count()
        return data_rdd.zipWithIndex().filter(lambda vi: (vi[1] < size - 1)).keys()
    else:
        return data_rdd


def stage_data(spark, cdc_params, table_name):
    """
    Create a DataFrame on the staged data. Fixed-length/delimiter-separated data files are parsed and converted to
    structured form with the appropriate type conversions.
    :param sc: SparkContext
    :param sqlcontext: SqlContext
    :param cdc_params: a dictionary having parameters about the staged data
            (1) the path to the staged data
            (2) the data format: fixed-length or delimiter-separated
            (3) if delimiter-separated, the delimiter character
            (4) if fixed-length and delimiter-separated,
                the path to the csv configuration file that has the field definitions
    :param table_name: Table Name for which CDC is running
    :return: stg_df: Staged dataframe
    """
    logger = logging.getLogger(__name__)
    # Get the data format of the table. whether fixedwidth or delimited. accordingly invoke the function
    # to create the dataframe
    file_format = cdc_params[table_name]["format"].lower()

    if file_format == "fixedwidth":
        stg_df = process_fixedwidth_file(spark, table_name, cdc_params)
    elif file_format == "delimited":
        stg_df = process_delimited_file(spark, table_name, cdc_params)
    elif file_format == "streaming":
        stg_df = process_streaming_table(spark, table_name, cdc_params)
    elif file_format == "jdbc_pull":
        stg_df = process_jdbc_import_table(spark, table_name, cdc_params)
    elif file_format == "excel_csv":
        stg_df = process_excel_csv(spark, table_name, cdc_params)
    else:
        logger.info("Invalid file/table format supplied for table {0}".format(table_name))

    return stg_df


def process_fixedwidth_file(spark, table_name, cdc_params):
    """
    Method to process fixedwidth file to create the data frame
    :param sc: SparkContext
    :param sqlcontext: SqlContext
    :param cdc_params: a dictionary having parameters about the staged data
            (1) the path to the staged data
            (2) the data format: fixed-length or delimiter-separated
            (3) if delimiter-separated, the delimiter character
            (4) if fixed-length and delimiter-separated,
                the path to the csv configuration file that has the field definitions
    :param table_name: Table Name for the CDC run
    Other important fields used are:
    tbl_schema: schema of the above csv file provided in a dictionary
                  schema is {column_name: [data_type, col_start_position, col_end_position]}
    dt_pattern: list of columns which are of Date or Timestamp data type and their
                       respective data format expected as agreed in Interface. These informations are
                       provided in the configuration json file
                       e.g.
                        "date_pattern": {
                               "ord_sta_dt": "%d.%m.%Y",
                               "ord_end_dt": "%d.%m.%Y"
                        }
    error_accumulator: Spark Accumulator having the error information
    """
    logger = logging.getLogger(__name__)
    try:
        home_dir = cdc_params["home"]
        data_source = cdc_params["source"]
        # Get the batch id from the cdc params and replace the file pattern seq with the batch id
        # Get the file name to be processed from the staging path..
        # If the staging directory has multiple files it will pick only the file with respect to this run
        batch_id = cdc_params["batch_id"]
        file_pattern = cdc_params[table_name]["file_pattern"]
        file_pattern = file_pattern.replace("(<batch_seq>)", batch_id).replace("(<date_seq>)", batch_id)

        stg_file = home_dir + "/" + "staging" + "/" + data_source + "/" + table_name + "/" + file_pattern
        tbl_schema = ("column_name", "data_type", "start_position", "length")
        config_file_path = cdc_params[table_name]["config_path"]
        header_flag = cdc_params[table_name]["header"]
        footer_flag = cdc_params[table_name]["footer"]

        # Get the date patterns for the columns defined as timestamp or date
        # and set default to empty dictionary if not provided in configuration
        dt_pattern = cdc_params[table_name].get("date_pattern", {})

        # Whether to store the error data record in accumulator or not..
        # Y -->       Store the error record in the accumulator
        # N --> Don't Store the error record in the accumulator
        ingestion_rec_log_ind = cdc_params["ingestion_fw_config"].setdefault("record_logging", "Y").lower()
        table_lvl_rec_log_ind = cdc_params[table_name]["error_logging"].setdefault("record_logging", "Y").lower()

        # If no indicator is set at table level, then use the indicator set in ingestion_fw.ini file
        record_logging = table_lvl_rec_log_ind if table_lvl_rec_log_ind else ingestion_rec_log_ind

        metadata = []
        with open(config_file_path) as csvfile:
            reader = csv.DictReader(csvfile, tbl_schema)
            for row in reader:
                output = {row["column_name"].lower().strip(): [row["data_type"].lower().strip(),
                                                               row["start_position"].strip(),
                                                               row["length"].strip()]}
                metadata.append(output)

        json_dict = json.loads(json.dumps(metadata))

        widths = []
        tgt_cols = []
        for x in json_dict:
            for key in x:
                list_of_values = x[key]
                temp_list = []
                # Prepare a list of lists data structure which will contain the start position and length of column
                start_pos = int(list_of_values[1])
                length = int(list_of_values[2])
                end_pos = start_pos + length
                temp_list.append(start_pos)
                temp_list.append(end_pos)
                widths.append(temp_list)

                # Prepare column dictionary to pass to prepare the StructType schema string
                col_dict = {u'name': unicode(key.lower()), u'type': unicode(list_of_values[0].lower())}
                tgt_cols.append(col_dict.copy())

        # create target df schema
        struct_field_arr = map(lambda x: ret_struct_fields(x), tgt_cols)
        tgt_ds_schema = StructType(struct_field_arr)

        if header_flag == 'y' or footer_flag == 'y':
            data1 = spark.sparkContext.newAPIHadoopFile(stg_file,
                                                        "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                                                        "org.apache.hadoop.io.LongWritable",
                                                        "org.apache.hadoop.io.Text",
                                                        conf={"textinputformat.record.delimiter": "\n"}).map(
                lambda l: l[1])
            data = data1.map(lambda x: split_data(widths, x, cdc_params, err_bucket=error_accumulator))
            rdd = remove_headerfooter(data, header_flag, footer_flag)
        else:
            data1 = spark.sparkContext.newAPIHadoopFile(stg_file,
                                                        "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                                                        "org.apache.hadoop.io.LongWritable",
                                                        "org.apache.hadoop.io.Text",
                                                        conf={"textinputformat.record.delimiter": "\n"}).map(
                lambda l: l[1])
            rdd = data1.map(lambda x: split_data(widths, x, cdc_params, err_bucket=error_accumulator))

        # Remove enclosing characters
        flag = cdc_params["remove_quotes"]
        rdd_cleaned = remove_enclosed_quotes(rdd, flag)

        # Convert the RDD to new RDD by casting the column values to python data types
        rdd_mapped = rdd_cleaned.map(lambda x: cast_to_py_types(row=x,
                                                                tgt_col=tgt_cols,
                                                                err_bucket=error_accumulator,
                                                                cdc_params=cdc_params,
                                                                record_log_flag=record_logging,
                                                                dt_pttrn=dt_pattern)).filter(lambda x: x)
        rdd_mapped.cache()
        # schema_with_error_fields = StructType(struct_field_arr)
        # Convert the above RDD to a dataframe
        df = spark.createDataFrame(rdd_mapped, tgt_ds_schema)

        return df
    except Exception as ex:
        logger.error("process_fixedwidth_file():: ingestion failed in fixedwidth file "
                     "function for table {0} {1}".format(table_name, ex))


def process_delimited_file(spark, table_name, cdc_params):
    """
    Method to process delimited file to create the data frame
    :param sc: SparkContext
    :param sqlcontext: SqlContext
    :param cdc_params: a dictionary having parameters about the staged data
            (1) the path to the staged data
            (2) the data format: fixed-length or delimiter-separated
            (3) if delimiter-separated, the delimiter character
            (4) if fixed-length and delimiter-separated,
                the path to the csv configuration file that has the field definitions
    :param table_name: Table Name for the CDC run
    Other important fields used are:
    tbl_schema: schema of the above csv file provided in a dictionary
                  schema is {column_name: [data_type, col_start_position, col_end_position]}
    dt_pattern: list of columns which are of Date or Timestamp data type and their
                       respective data format expected as agreed in Interface. These informations are
                       provided in the configuration json file
                       e.g.
                        "date_pattern": {
                               "ord_sta_dt": "%d.%m.%Y",
                               "ord_end_dt": "%d.%m.%Y"
                        }
    error_accumulator: Spark Accumulator having the error information
    """
    logger = logging.getLogger(__name__)
    try:
        home_dir = cdc_params["home"]
        data_source = cdc_params["source"]

        # Get the batch id from the cdc params and replace the file pattern seq with the batch id
        # Get the file name to be processed from the staging path..
        # If the staging directory has multiple files it will pick only the file with respect to this run
        batch_id = cdc_params["batch_id"]
        file_pattern = cdc_params[table_name]["file_pattern"]
        file_pattern = file_pattern.replace("(<batch_seq>)", batch_id).replace("(<date_seq>)", batch_id)

        stg_file = home_dir + "/" + "staging" + "/" + data_source + "/" + table_name + "/" + file_pattern
        tbl_schema = ("column_name", "data_type")
        config_file_path = cdc_params[table_name]["config_path"]
        header_flag = cdc_params[table_name]["header"]
        footer_flag = cdc_params[table_name]["footer"]
        delimiter = cdc_params[table_name].get("delimiter", '').lower().encode("utf-8").decode('string-escape')

        # Get the date patterns for the columns defined as timestamp or date
        # and set default to empty dictionary if not provided in configuration
        dt_pattern = cdc_params[table_name].get("date_pattern", {})

        # Whether to store the error data record in accumulator or not..
        # Y -->       Store the error record in the accumulator
        # N --> Don't Store the error record in the accumulator
        ingestion_rec_log_ind = cdc_params["ingestion_fw_config"].setdefault("record_logging", "Y").lower()
        table_lvl_rec_log_ind = cdc_params[table_name]["error_logging"].setdefault("record_logging", "Y").lower()

        # If no indicator is set at table level, then use the indicator set in ingestion_fw.ini file
        record_logging = table_lvl_rec_log_ind if table_lvl_rec_log_ind else ingestion_rec_log_ind

        metadata = []
        with open(config_file_path) as csvfile:
            reader = csv.DictReader(csvfile, tbl_schema)
            for row in reader:
                output = {row["column_name"].lower().strip(): row["data_type"].lower().strip()}
                metadata.append(output)

        json_dict = json.loads(json.dumps(metadata))

        tgt_cols = []
        for x in json_dict:
            for key in x:
                # Prepare column dictionary to pass to prepare the StructType schema string
                col_dict = {u'name': unicode(key.lower()), u'type': unicode(x[key].lower())}
                tgt_cols.append(col_dict.copy())

        # create target df schema
        struct_field_arr = map(lambda x: ret_struct_fields(x), tgt_cols)
        tgt_ds_schema = StructType(struct_field_arr)

        if header_flag == 'y' or footer_flag == 'y':
            data1 = spark.sparkContext.newAPIHadoopFile(stg_file,
                                                        "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                                                        "org.apache.hadoop.io.LongWritable",
                                                        "org.apache.hadoop.io.Text",
                                                        conf={"textinputformat.record.delimiter": "\n"}).map(
                lambda l: l[1])
            data = data1.map(lambda x: x.split(delimiter))
            rdd = remove_headerfooter(data, header_flag, footer_flag)
        else:
            data1 = spark.sparkContext.newAPIHadoopFile(stg_file,
                                                        "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                                                        "org.apache.hadoop.io.LongWritable",
                                                        "org.apache.hadoop.io.Text",
                                                        conf={"textinputformat.record.delimiter": "\n"}).map(
                lambda l: l[1])
            rdd = data1.map(lambda x: x.split(delimiter))

        # Remove enclosing characters
        flag = cdc_params["remove_quotes"]
        rdd_cleaned = remove_enclosed_quotes(rdd, flag)

        # Convert the RDD to new RDD by casting the column values to python data types
        rdd_mapped = rdd_cleaned.map(lambda x: cast_to_py_types(row=x,
                                                                tgt_col=tgt_cols,
                                                                err_bucket=error_accumulator,
                                                                cdc_params=cdc_params,
                                                                record_log_flag=record_logging,
                                                                dt_pttrn=dt_pattern)).filter(lambda x: x)
        rdd_mapped.cache()

        # Convert the above RDD to a dataframe
        df = spark.createDataFrame(rdd_mapped, tgt_ds_schema)
        return df
    except Exception as ex:
        logger.error("process_delimited_file():: ingestion failed in delimited file "
                     "processing for table {0} {1}".format(table_name, ex))


def process_streaming_table(spark, table_name, cdc_params):
    """
    Method to process streaming table to create the data frame
    :param sc: SparkContext
    :param sqlcontext: SqlContext
    :param cdc_params: a dictionary having parameters about the staged data
            (1) the path to the staged data
            (2) the data format: streaming table
            (3) if delimiter-separated, the delimiter character
            (4) if fixed-length and delimiter-separated,
                the path to the csv configuration file that has the field definitions
    :param table_name: Table Name for the CDC run
    Other important fields used are:
    tbl_schema: schema of the above csv file provided in a dictionary
                  schema is {column_name: [data_type, col_start_position, col_end_position]}
    dt_pattern: list of columns which are of Date or Timestamp data type and their
                       respective data format expected as agreed in Interface. These informations are
                       provided in the configuration json file
                       e.g.
                        "date_pattern": {
                               "ord_sta_dt": "%d.%m.%Y",
                               "ord_end_dt": "%d.%m.%Y"
                        }
    error_accumulator: Spark Accumulator having the error information
    """
    logger = logging.getLogger(__name__)
    try:

        tbl_schema = ("column_name", "data_type")
        config_file_path = cdc_params[table_name]["config_path"]

        # Get the date patterns for the columns defined as timestamp or date
        # and set default to empty dictionary if not provided in configuration
        dt_pattern = cdc_params[table_name].get("date_pattern", {})

        # Whether to store the error data record in accumulator or not..
        # Y -->       Store the error record in the accumulator
        # N --> Don't Store the error record in the accumulator
        ingestion_rec_log_ind = cdc_params["ingestion_fw_config"].setdefault("record_logging", "Y").lower()
        table_lvl_rec_log_ind = cdc_params[table_name]["error_logging"].setdefault("record_logging", "Y").lower()

        # If no indicator is set at table level, then use the indicator set in ingestion_fw.ini file
        record_logging = table_lvl_rec_log_ind if table_lvl_rec_log_ind else ingestion_rec_log_ind

        metadata = []
        with open(config_file_path) as csvfile:
            reader = csv.DictReader(csvfile, tbl_schema)
            for row in reader:
                output = {row["column_name"].lower().strip(): row["data_type"].lower().strip()}
                metadata.append(output)

        json_dict = json.loads(json.dumps(metadata))

        tgt_cols = []
        col_list = ''
        for x in json_dict:
            for key in x:
                # Prepare column dictionary to pass to prepare the StructType schema string
                col_dict = {u'name': unicode(key.lower()), u'type': unicode(x[key].lower())}
                tgt_cols.append(col_dict.copy())
                col_list = col_list + ',' + key

        # create target df schema
        struct_field_arr = map(lambda x: ret_struct_fields(x), tgt_cols)
        tgt_ds_schema = StructType(struct_field_arr)

        # Convert the stage_df to new RDD by casting the column values to python data types
        streaming_table_db = cdc_params[table_name]["streaming_table_db"]
        active_table_db = cdc_params[table_name]["active_table_db"]
        stream_dt_col = cdc_params[table_name]["stream_dt_col"]
        stream_tbl_partition_col = cdc_params[table_name]["stream_tbl_partition_col"]
        stage_table = streaming_table_db + "." + table_name
        active_table = active_table_db + "." + table_name

        # Filter staged_df on basis of last date
        last_date_query = "select max(from_unixtime(unix_timestamp(" + stream_dt_col + "),'yyyyMMddHH')) as max_tm from " + active_table

        last_date_df = spark.sql(last_date_query).collect()[0]
        last_date_field = last_date_df["max_tm"] if last_date_df["max_tm"] else '1900-01-01 00:00:00'

        frmt_last_date = datetime.datetime.strptime(last_date_field, "%Y-%m-%d %H:%M:%S").strftime('%Y%m%d%H')
        logger.info("Extracting Data from Stream Table after :" + last_date_field)

        stage_query = "select " + col_list[1:] + " from " + stage_table + " where " \
                      + stream_tbl_partition_col + " >= '" + frmt_last_date + "'"

        staged_df = spark.sql(stage_query)
        staged_df = staged_df.filter(col(stream_dt_col) > last_date_field)

        # rdd creation from staged DF
        rdd = staged_df.rdd.map(list)
        rdd_mapped = rdd.map(lambda x: cast_to_py_types(row=x,
                                                        tgt_col=tgt_cols,
                                                        err_bucket=error_accumulator,
                                                        cdc_params=cdc_params,
                                                        record_log_flag=record_logging,
                                                        dt_pttrn=dt_pattern)).filter(lambda x: x)
        rdd_mapped.cache()

        # Convert the above RDD to a dataframe
        df = spark.createDataFrame(rdd_mapped, tgt_ds_schema)
        return df
    except Exception as ex:
        logger.error("process_streaming_table():: ingestion failed in streaming table "
                     "processing for table {0} {1}".format(table_name, ex))


def process_jdbc_import_table(spark, table_name, cdc_params):
    """
    Method to process data from rdbms tables to create dataframe.
    :param spark: SparkSession
    :param table_name: Table Name for the CDC run
    :param cdc_params: a dictionary having parameters about the staged data
            (1) the path to the staged data
            (2) the data format: streaming table
            (3) if delimiter-separated, the delimiter character
            (4) if fixed-length and delimiter-separated,
                the path to the csv configuration file that has the field definitions
    Other important fields used are:
    src_conf: All the information related to the jdbc connection are provided in an ini file like username, password
                 etc.,
    jdbc_config : All the details related to the source tables are provided in the ETL Json file
                    e.g.
                    "jdbc_config": {
                    "server_connection"
                    "src_db_name":
                    "src_tbl_name":
                    "incremental_column":
                    }
    :return: Returns staged dataframe
    """
    logger = logging.getLogger(__name__)
    db_conn_prop_file = cdc_params['db_connection']

    try:
        config_file_path = cdc_params[table_name]["config_path"]

        # Read the RDBMS pull related configuration from the json file.. If not provided fail the job
        src_jdbc_config = cdc_params[table_name].setdefault("jdbc_config", None)

        if src_jdbc_config is None:
            raise Exception("Please provide configuration details for the source RDBMS system.. ")

        rdbms_pull_config_file = "/dbfs/mnt" + src_jdbc_config.setdefault("jdbc_pull_config_path",
                                                                          "/apps/b2b_data_ingestion_fw/config/jdbc_pull/"
                                                                          "jdbc_pull.ini")
        connection_id = src_jdbc_config.setdefault("connection_id", None)
        src_conf = config(filename=rdbms_pull_config_file, section=connection_id)
        src_db_type = src_conf["server_type"]
        src_port_num = src_conf["port_num"]
        src_host_nm = src_conf.setdefault("hostname", None)
        src_db_nm = src_jdbc_config.setdefault("src_db_name", None)
        src_table_nm = src_jdbc_config.setdefault("src_table_name", None)
        src_nm = cdc_params["source"]
        load_type = src_jdbc_config.setdefault("load_type", None)
        incremental_column = src_jdbc_config.setdefault("incremental_column", None)
        dt_pattern = cdc_params[table_name].get("date_pattern", {})
        ingestion_rec_log_ind = cdc_params["ingestion_fw_config"].setdefault("record_logging", "Y").lower()
        table_lvl_rec_log_ind = cdc_params[table_name]["error_logging"].setdefault("record_logging", "Y").lower()

        # If no indicator is set at table level, then use the indicator set in ingestion_fw.ini file
        record_logging = table_lvl_rec_log_ind if table_lvl_rec_log_ind else ingestion_rec_log_ind

        # Please provide mandatory source details for the jdbc connection
        if None in (src_host_nm, src_port_num, src_db_nm, src_table_nm):
            raise Exception("Please provide mandatory src_host_nm, port_num, src_db_name, src_table_nm values..")
        connection_url = "jdbc:{0}://{1}:{2};database={3}".format(src_db_type,
                                                                  src_host_nm,
                                                                  src_port_num,
                                                                  src_db_nm)
        dbmanager = DbManager(db_conn_prop_file)

        tbl_schema = ("column_name", "data_type")
        metadata = []
        with open(config_file_path) as csvfile:
            reader = csv.DictReader(csvfile, tbl_schema)
            for row in reader:
                output = {row["column_name"].lower().strip(): row["data_type"].lower().strip()}
                metadata.append(output)

        json_dict = json.loads(json.dumps(metadata))

        tgt_cols = []
        col_list = ''
        for x in json_dict:
            for key in x:
                # Prepare column dictionary to pass to prepare the StructType schema string
                col_dict = {u'name': unicode(key.lower()), u'type': unicode(x[key].lower())}
                tgt_cols.append(col_dict.copy())
                col_list = col_list + ',' + key

        # create target df schema
        struct_field_arr = map(lambda x: ret_struct_fields(x), tgt_cols)
        tgt_ds_schema = StructType(struct_field_arr)

        jdbc_connection_properties = {
            "user": src_conf["user"],
            "password": src_conf["password"],
            "driver": src_conf["driver"]
        }

        dict = {}
        last_modified = None

        if load_type is None:
            raise Exception("Please provide either full_load or delta_load in the ETL JSON under jdbc_config tag")
        elif load_type == "full_load":
            logger.info("Getting full data from source table")
            dict["import_data_df"] = spark.read.jdbc(connection_url,
                                                     table=src_table_nm,
                                                     properties=jdbc_connection_properties)
        elif load_type == "delta_load":
            logger.info("Getting delta data from source table")
            last_modified_query = "SELECT MAX(date_trunc('second', import_rec_end_time::TIMESTAMP)) " \
                                  " FROM batch_jdbc_import_details " \
                                  " WHERE status = 'success' " \
                                  "   AND source_name = '{0}'" \
                                  "   AND src_db_name = '{1}'" \
                                  "   AND src_tbl_name = '{2}'" \
                                  " GROUP BY source_name, src_db_name, src_tbl_name".format(src_nm,
                                                                                            src_db_nm,
                                                                                            src_table_nm)
            last_modified = dbmanager.execute_query(last_modified_query)
            last_modified = last_modified[0][0].strftime("%Y-%m-%d %H:%M:%S") if last_modified else \
                "1900-01-01 00:00:00"
            query = "(select * from {0} where {1} > '{2}' ) as jdbc".format(src_table_nm,
                                                                            incremental_column,
                                                                            last_modified)
            dict["import_data_df"] = spark.read.jdbc(connection_url,
                                                     table=query,
                                                     properties=jdbc_connection_properties)
        extract_columns = src_jdbc_config.setdefault("extract_columns", None)
        dict["stg_df"] = dict["import_data_df"].select(extract_columns)

        # rdd creation from staged DF
        # converting all columns to string in order to handle date time fields
        casted_cols_list = [col(column).cast('string') for column in dict["stg_df"].columns]
        dict["stg_df"] = dict["stg_df"].select(casted_cols_list)
        logger.info("All the columns have been casted to string type")

        rdd = dict["stg_df"].rdd.map(list)

        # Convert the RDD to new RDD by casting the column values to python data types
        rdd_mapped = rdd.map(lambda x: cast_to_py_types(row=x,
                                                        tgt_col=tgt_cols,
                                                        err_bucket=error_accumulator,
                                                        cdc_params=cdc_params,
                                                        record_log_flag=record_logging,
                                                        dt_pttrn=dt_pattern)).filter(lambda x: x)

        rdd_mapped.cache()
        # Convert the above RDD to a dataframe
        df = spark.createDataFrame(rdd_mapped, tgt_ds_schema)
        # Get the required details to insert record into the batch_jdbc_import_details table
        imported_rec_count = dict["import_data_df"].count()
        batch_seq_id = cdc_params["batch_seq_id"]

        # maintaing the same batch_seq_id for batch_seq_detail as well as jdbc control tables
        logger.debug("Checking the batch_seq_id is same in batch_seq_detail and batch_jdbc_import_details {}"
                     .format(batch_seq_id))

        batch_id = cdc_params["batch_id"]
        if imported_rec_count is not 0:
            new_max_date = "'" + (dict["import_data_df"].select(incremental_column).rdd.max()[0]).strftime(
                "%Y-%m-%d %H:%M:%S") + "'"
            new_min_date = "'" + (dict["import_data_df"].select(incremental_column).rdd.min()[0]).strftime(
                "%Y-%m-%d %H:%M:%S") + "'"
        else:
            new_max_date = "'" + last_modified + "'"
            new_min_date = "'" + last_modified + "'"
        insert_column = ["batch_seq_id",
                         "batch_id",
                         "source_name",
                         "src_db_name",
                         "src_tbl_name",
                         "import_rec_start_time",
                         "import_rec_end_time",
                         "records_imported",
                         "status",
                         "created_by"
                         ]
        insert_values = [batch_seq_id,
                         "'" + batch_id + "'",
                         "'" + src_nm + "'",
                         "'" + src_db_nm + "'",
                         "'" + src_table_nm + "'",
                         new_min_date,
                         new_max_date,
                         imported_rec_count,
                         "'success'",
                         "'spark_jdbc_pull_job'"
                         ]
        dbmanager.insert_record(table_name='batch_jdbc_import_details', columns=insert_column, values=insert_values)
        dbmanager.close_connection()
        return df
    except Exception as ex:
        logger.error("process_jdbc_import_table():: ingestion failed in jdbc table processing for table {0} {1}".
                     format(table_name, ex))
        current_batch_seq_id = cdc_params["batch_seq_id"]
        update_record_batch_seq_detail(status="Failed",
                                       batch_seq_id=current_batch_seq_id,
                                       prop_file_path=db_conn_prop_file)
        sys.exit(1)


def process_excel_csv(spark, table_name, cdc_params):
    """
    Process Delimited file which are in Excel csv format. This function is to handle multiline records present in the
    data file
    :param spark: Spark Context
    :param table_name: Table name for which CDC is being performed
    :param cdc_params: cdc parameters for the table
    :return: Staged dataframe
    """
    logger = logging.getLogger(__name__)
    db_conn_prop_file = cdc_params['db_connection']

    try:
        home_dir = cdc_params["home"]
        data_source = cdc_params["source"]

        # Get the batch id from the cdc params and replace the file pattern seq with the batch id
        # Get the file name to be processed from the staging path..
        # If the staging directory has multiple files it will pick only the file with respect to this run
        batch_id = cdc_params["batch_id"]
        file_pattern = cdc_params[table_name]["file_pattern"]
        file_pattern = file_pattern.replace("(<batch_seq>)", batch_id).replace("(<date_seq>)", batch_id)

        stg_file = home_dir + "/" + "staging" + "/" + data_source + "/" + table_name + "/" + file_pattern

        tbl_schema = ("column_name", "data_type")
        config_file_path = cdc_params[table_name]["config_path"]
        delimiter = cdc_params[table_name].get("delimiter", ',').lower().encode("utf-8").decode('string-escape')
        header = True if cdc_params[table_name]["header"].lower() == 'y' else False
        # header = True if header_flag == "y" else False
        # footer = cdc_params[table_name]["footer"]

        quote_char = cdc_params[table_name]["quote_char"]
        escape_char = cdc_params[table_name]["escape_char"]

        # Get the date patterns for the columns defined as timestamp or date
        # and set default to empty dictionary if not provided in configuration
        dt_pattern = cdc_params[table_name].get("date_pattern", {})

        # Whether to store the error data record in accumulator or not..
        # Y -->       Store the error record in the accumulator
        # N --> Don't Store the error record in the accumulator
        ingestion_rec_log_ind = cdc_params["ingestion_fw_config"].setdefault("record_logging", "Y").lower()
        table_lvl_rec_log_ind = cdc_params[table_name]["error_logging"].setdefault("record_logging", "").lower()

        # If no indicator is set at table level, then use the indicator set in ingestion_fw.ini file
        record_logging = table_lvl_rec_log_ind if table_lvl_rec_log_ind else ingestion_rec_log_ind

        metadata = []
        with open(config_file_path) as csvfile:
            reader = csv.DictReader(csvfile, tbl_schema)
            for row in reader:
                output = {row["column_name"].lower().strip(): row["data_type"].lower().strip()}
                metadata.append(output)

        json_dict = json.loads(json.dumps(metadata))

        tgt_cols = []
        for x in json_dict:
            for key in x:
                # Prepare column dictionary to pass to prepare the StructType schema string
                col_dict = {u'name': unicode(key.lower()), u'type': unicode(x[key].lower())}
                tgt_cols.append(col_dict.copy())

        # create target df schema
        struct_field_arr = map(lambda x: ret_struct_fields(x), tgt_cols)
        tgt_ds_schema = StructType(struct_field_arr)

        # data = sqlcontext.read.csv(hdfs_stg_file, sep=delimiter, header=False,
        data = spark.read.csv(stg_file, sep=delimiter, header=header,
                              inferSchema=False, quote=quote_char, escape=escape_char,
                              multiLine=True, mode='DROPMALFORMED').rdd.map(list)

        # Convert the RDD to new RDD by casting the column values to python data types
        rdd_mapped = data.map(lambda x: cast_to_py_types(row=x,
                                                         tgt_col=tgt_cols,
                                                         err_bucket=error_accumulator,
                                                         cdc_params=cdc_params,
                                                         record_log_flag=record_logging,
                                                         dt_pttrn=dt_pattern)).filter(lambda x: x)
        rdd_mapped.cache()

        # Convert the above RDD to a dataframe
        df = spark.createDataFrame(rdd_mapped, tgt_ds_schema)
        # df = sqlcontext.createDataFrame(rdd_mapped, tgt_ds_schema)
        return df
    except Exception as ex:
        logger.error("process_excel_csv():: Ingestion failed in excel csv function for the table {0} {1}".
                     format(table_name, ex))
        current_batch_seq_id = cdc_params["batch_seq_id"]
        update_record_batch_seq_detail(status="Failed",
                                       batch_seq_id=current_batch_seq_id,
                                       prop_file_path=db_conn_prop_file)
        sys.exit(1)


def update_jdbc_pull_audit(current_batch_seq_id, prop_file_path):
    """
    update the batch_jdbc_import_details table with current batch status as Failed
    Reason:
        Earlier there was no association with batch_seq_detail status and batch_jdbc_import_details status
        example: jdbc control table gets updated to success as soon as the jdbc pull happens and the same is not getting
                 updated to failed when it fails writing to hive staging which will result in 0 rows upon rerun of current
                 batch.

        So this updates the status as Failed if job fails in any other part of CDC
    :param current_batch_seq_id: current running unique batch_seq_id
    :param prop_file_path: ini file path for db connect
    :return: None
    """
    try:
        # creating an object for DbManager class
        dbmanager = DbManager(prop_file_path)

        # preparing the query to update the current seq in batch_jdbc_import_details as failed
        query = "update batch_jdbc_import_details set status = 'Failed' where batch_seq_id = {}".format(
            current_batch_seq_id)

        dbmanager.execute_query(query)
        dbmanager.close_connection()

    except Exception as ex:
        raise Exception("update_jdbc_pull_audit()  failed ::  {}".format(ex))


def read_config_property(ingestion_fw_property):
    """
    Read the ingestion_fw.ini file from config directory and parse the section ingestion_fw
    :param ingestion_fw_property: Ingestion framework property file
    :return: dictionary : {'record_logging': 'N', 'error_table': 'db_b2b_error.data_ingestion_error'}
    """
    params = config(filename=ingestion_fw_property, section='ingestion_fw')
    return params


def read_cdc_params(etl_cfile):
    """
    Read the parameters required for CDC.
    :param etl_cfile: the ETL configuration file for this job
    :return: the CDC parameters as a dictionary
    """
    cdc_params = {}
    cdc_params['source'] = etl_cfile["ingestion_config"]["source"]
    cdc_params['app_name'] = etl_cfile["ingestion_config"]["app"]
    cdc_params['feed_id'] = etl_cfile["ingestion_config"]["feed_id"]
    cdc_params['home'] = "/mnt" + etl_cfile["ingestion_config"]["home_dir"]
    cdc_params['remove_quotes'] = etl_cfile["ingestion_config"].get("remove_quotes", "N")

    # Populate CDC params for all tables
    cdc_params["table_list"] = []
    tables = etl_cfile["ingestion_config"]["datafeed"]

    for table_nm in tables:
        table_attr = etl_cfile["ingestion_config"]["datafeed"][table_nm]
        cdc_params["table_list"].append(table_nm)
        cdc_params[table_nm] = {}
        cdc_params[table_nm]["format"] = table_attr["format"]
        if cdc_params[table_nm]["format"] in ["fixedwidth", "delimited", "excel_csv"]:
            cdc_params[table_nm]["header"] = table_attr["validation"]["record_count_validation"].get("header",
                                                                                                     "N").lower()
            cdc_params[table_nm]["footer"] = table_attr["validation"]["record_count_validation"].get("footer",
                                                                                                     "N").lower()
        # Get excel csv pattern related information
        if cdc_params[table_nm]["format"] in ["excel_csv"]:
            cdc_params[table_nm]["quote_char"] = table_attr.get("quote_char", '"')
            cdc_params[table_nm]["escape_char"] = table_attr.get("escape_char", '\\')

        if cdc_params[table_nm]["format"] in ["jdbc_pull"]:
            cdc_params[table_nm]["file_pattern"] = ""
        else:
            cdc_params[table_nm]["file_pattern"] = table_attr["validation"]["triplet_check"]. \
                get("file_pattern", "")
        cdc_params[table_nm]["config_path"] = "/dbfs/mnt" + table_attr["config_path"]
        cdc_params[table_nm]["streaming_table_db"] = table_attr.setdefault("streaming_table_db", {})
        cdc_params[table_nm]["active_table_db"] = table_attr.setdefault("active_table_db", {})
        cdc_params[table_nm]["stream_dt_col"] = table_attr.setdefault("stream_dt_col", {})
        cdc_params[table_nm]["stream_tbl_partition_col"] = table_attr.setdefault("stream_tbl_partition_col", {})
        cdc_params[table_nm]["date_pattern"] = table_attr.setdefault("date_pattern", {})
        if cdc_params[table_nm]["format"].lower() in ("delimited", "excel_csv"):
            cdc_params[table_nm]["delimiter"] = table_attr["delimiter"]
        cdc_params[table_nm]["cdc_config"] = table_attr["cdc_config"]
        cdc_params[table_nm]["error_logging"] = table_attr.setdefault("error_logging", {})
        cdc_params[table_nm]["jdbc_config"] = table_attr.setdefault("jdbc_config", None)
    return cdc_params


def parse_cmd_args():
    """
    add the command line arguments that are to be parsed.
    :param parser: the ArgumentParser instance
    :return: the parsed arguments
    """
    cmd_parser = argparse.ArgumentParser(description="Ingestion Framework CDC module")
    cmd_parser.add_argument("etl_config_file_path", help="path to the ETL configuration file for this job")
    cmd_parser.add_argument("db_connection", help="Postgres database connection property file")
    cmd_parser.add_argument("cdc_pattern", nargs='?', default="", help="if this one time load/back load "
                                                                       "value is one_time_load")
    # cmd_parser.add_argument("batch_id", help="the Id or number of the batch that is currently being processed")
    cmd_args = cmd_parser.parse_args()
    return cmd_args


def write_into_error_table(error_df, error_table):
    """
    Write One time data to the hive history table
    :param error_df: DataFrame having the details from the error accumulator
    :param error_table: error table name (dbname.tablename)
    :return: None
    """
    # error_df.write.format("parquet").mode('append').saveAsTable(error_table)
    error_df.write.format("hive").mode('append').saveAsTable(error_table)


def split_data(widths, record, cdc_params, err_bucket):
    """
    Method to split the record for fixed length file in to columns based on the
    length information provided in the csv file
    :param widths: a list of lists having the starting position of column and end position of column
                   e.g. [[0,5],[6,13]]
    :param record: A record of the data represented as a tuple (row_data)
    :return fields: Return the fields in a list after splitting
    """
    logger = logging.getLogger(__name__)
    try:
        fields = []
        last_pos = widths[-1][1]

        if last_pos == len(record):
            for col_range in widths:
                start_position = col_range[0]
                end_position = col_range[1]
                field_value = record[start_position:end_position]
                fields.append(field_value)
            return fields
        else:
            logger.debug("split_data():: Record length is not matched :: " + record)
            feed_id = cdc_params["feed_id"]
            source = cdc_params["source"]
            seq_num = cdc_params["batch_id"]
            batch_seq_id = cdc_params["batch_seq_id"]
            err_bucket += {
                record: {'col_name': record,
                         'given_value': last_pos,
                         'actual_value': len(record),
                         'err_msg': "Record lenth is not matched",
                         'batch_id': seq_num,
                         'batch_seq_id': batch_seq_id,
                         'feed_id': feed_id,
                         'source': source,
                         'run_time': str(datetime.now()),
                         'record': record}
            }

    except Exception as ex:
        logger.error("split_data():: Error while split data record :: " + record + ":: {0}".format(ex))


def cdc_main():
    """
    CDC module main()
    :return:
    """
    args = parse_cmd_args()

    # Read the ETL configuration file for the job

    etl_cfile = json.loads(open(args.etl_config_file_path).read())
    cdc_params = read_cdc_params(etl_cfile)
    cdc_params['cdc_pattern'] = args.cdc_pattern

    # Control Table related changes
    cdc_params['db_connection'] = args.db_connection

    # Get seq_id for this run i.e. batch_id
    feed_id = cdc_params["feed_id"]
    db_conn_prop_file = cdc_params['db_connection']
    current_seq_id = get_seq_id_for_this_run(feed_id, prop_file_path=db_conn_prop_file)
    cdc_params['batch_id'] = current_seq_id

    # Read the ingestion fw property ini file to get the properties from ingestion_fw section
    ingestion_fw_config = read_config_property(cdc_params['db_connection'])

    cdc_params["ingestion_fw_config"] = ingestion_fw_config

    # Create the Spark SQLContext / HiveContext
    spark = create_spark_session(cdc_params)
    launch_cdc(spark, cdc_params)


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s: [%(levelname)s]: %(name)s: %(message)s")
    logging.getLogger("py4j").setLevel(logging.ERROR)

    cdc_main()
