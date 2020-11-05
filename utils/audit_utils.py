# coding=utf-8
import os
import sys
import re
import fnmatch
import shutil
import subprocess
import glob
import logging
import json

from datetime import datetime
from dateutil import parser
from decimal import Decimal, getcontext, ROUND_FLOOR
from ingestion_fw.metastore.dbmanager import DbManager

def log_audit_entry(log_dict,prop_file_path):
    """
    :return: None
    """
    logger = logging.getLogger(__name__)
    try:
        #logger.info("Inside Audit Util")
        dbmanager = DbManager(prop_file_path)
        table_name = "b2bdp_elt_audit"
        if log_dict["metrics"] == 'NULL':
            metrics=log_dict["metrics"]
        else:
            metrics="'"+json.dumps(log_dict["metrics"])+"'"
        insert_column = ["run_id","key","event_type","event","source","target","source_type","target_type","status","metrics","event_ts"]
        insert_values = [log_dict["run_id"],log_dict["key"],log_dict["event_type"],log_dict["event"],log_dict["source"],log_dict["target"],log_dict["source_type"],log_dict["target_type"],log_dict["status"],metrics,"CURRENT_TIMESTAMP"]
        #logger.info(insert_values)

        dbmanager.insert_record(table_name=table_name, columns=insert_column, values=insert_values)
        dbmanager.close_connection()
    except Exception as ex:
        raise Exception("insert_record_into_ingestion_step():: {0}".format(ex))


def ingestion_audit_metrics_dict():
    """
    This function will define a pre-defined standard metrics struture
        > Ease of extractng meaningfull information from Audit views.
	> Consistent use of Audit Metrics and naming convention.
	> Ease of use during new development/logging.
        > Metric Dict structure can evolve based on use-cases [fields can be appended]
        > Use the Dict Structure, to populate the metrics required for specific Audit activities.
    return: Initialized Dict structure to log audit metrics
    """

    metrics_dict = {}
    #File Sensor metrics
    metrics_dict.update({"all_file_patterns":"","matched_file_patterns":""})

    #File Validation  metrics
    metrics_dict.update({"data_source":"","dat_file_count":"","ctrl_file_count":""})

    #CDC Metrics
    metrics_dict.update({"cdc_pattern":"","table_name":""})

    #Common Metrics (Ingestion, IDM, Reporting Layer)
    metrics_dict.update({"source_row_count":"","target_row_count":""})

    #Common Success/Error Capture Metrics
    metrics_dict.update({"error_row_count":"","error_msg":"","error_type":"","success_msg":""})

    return metrics_dict