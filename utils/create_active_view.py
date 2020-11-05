"""
 Standalone Spark Submit Utility to Create Active Views.

 Lists all the tables in History DB, Reads pk and ts_col from Input JSON, creates hql for active views, creates views in the active DB

 Spark Submit cmd line:
 ----------------------
        Format:  spark-submit --master yarn-client  create_active_view.py <Input Json> <History Table Name> <Create View Flg> <Active Table Name> <redirect cons o/p to log>
        -------
	Sample:  spark-submit --master yarn-client  create_active_view.py  inp_all.json db_b2b_curated_maxim_historical Y db_b2b_curated_maxim_active 2>spark.log
        -------

 Input CMD line Params:
 ----------------------
	1. INPUT JSON
	2. HISTORY DB
	3. Create View Flag. 'Y' or 'V' : If 'Y' view has to be created by the script. If 'V' it will create and then Select from view to Validate.
	4. ACTIVE DB (DB where the Active view is to be Created)

 Output:
 -------
	Folder : ~<working_dir>/<source>
	-------	1. Failure Report.Report Name: <source>_failure.rpt. If there are Failures. Error Description for each record.
		2. Success Report.Report Name: <source>_success.rpt If view created on Active DB. Each View Created will be listed.
		3. If Create View Flag is 'N':
			a. Consolidated Create View hql for All the tables in the history table
			b. Individual hql files for each table
			c. Failure Report. If there is an issue connecting to History DB
		4. If Create View Flag is 'N':
			a. Consolidated Create View hql for All the tables in the history table
			b. Individual hql files for each table
			c. Failure Report if the table/view exists with the same name
			d. Success Report if View is created in the Active DB
 Logical Components:
 -------------------
	<Search component for more details>
	1. Initialization_Setup
	2. Create_View_HQL_File
	3. Create_View_on_Active_DB
"""

import io
import argparse
import logging
import subprocess
import sys
import json
from datetime import datetime
from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext


def main():
    """
    1. Initialization_Setup

	a. Read arg params. b. Read JSON c. Spark Session Creation d.Output folder and Rpt Creation e.Capture start and end time
    """

    # Capture Start time of the Batch
    start_time = str(datetime.now())

    ## Initialize from arg values
    args = parse_cmd_args()
    jsonfilename = args.jsonfilename
    hist_dbName = args.hist_dbName
    is_create_view = args.is_create_view
    actv_dbName = args.actv_dbName

    ## Read the Input JSON
    json_params = read_json(jsonfilename)

    # Initialization and Spark Session Creation
    conf = SparkConf().setAppName("create view")
    sc = SparkContext(conf=conf)
    spark = HiveContext(sc)
    logger = logging.getLogger(__name__)

    # Create Success and Failure Reports. In the SOURCE working folder
    source = json_params["source"]
    (rc, msg, err) = run_cmd(['mkdir', '-p', 'MAXIM'])
    if rc != 0:
        logger.info("Source Folder Creation Failed")
        sys.exit(1)
    (rc, msg, err) = run_cmd(['chmod', '-R', '777', source])
    if rc != 0:
        logger.info("Source Folder Permissions Failed")
        sys.exit(1)

    failure_rpt_name = source + "/" + source + "_failure.rpt"
    msg = "-- Failure Report : Create View --\n"
    msg = msg + "  Start Time : " + start_time + "\n"
    msg = msg + " Input Params -- JSON:" + jsonfilename + ", history DB:" + hist_dbName + ", Active DB:" + actv_dbName + ", Create Flag:" + is_create_view + "\n\n"
    write_to_file(msg, failure_rpt_name, 'w')
    success_rpt_name = source + "/" + source + "_success.rpt"
    msg = "-- Success Report : Create View --\n"
    msg = msg + "  Start Time : " + start_time + "\n"
    msg = msg + " Input Params -- JSON:" + jsonfilename + ", history DB:" + hist_dbName + ", Active DB:" + actv_dbName + ", Create Flag:" + is_create_view + "\n\n"
    write_to_file(msg, success_rpt_name, 'w')

    # Throw Error when Mandatory Inputs are not Present
    if jsonfilename == "" or hist_dbName == "":
        msg = "Error :: JSON and History DB Name are Mandatory\n"
        write_to_file(msg, failure_rpt_name, 'a')
        sys.exit(1)
    elif is_create_view == "Y" and actv_dbName == "":
        msg = "Error :: If Create View is Y, then Active DB Name is Mandatory "
        write_to_file(msg, failure_rpt_name, 'a')
        sys.exit(1)

    """
    2. Create_View_HQL_File
	a. Creates view HQL by listing all tables in the history DB. (Assumption: There are no views)
        b. Write to failure report if unable to access history DB
        c. Returns Create view String List.
    """
    createViewList = create_view_files(spark, hist_dbName, json_params)

    """
    3. Create_View_on_Active_DB
	a. Create View String will be Input for create.
	b. Check if the object already exists. If yes, write to failure report.
	c. if object doesn't exist, create view and write to success report.

    """
    if is_create_view in ('Y', 'V'):
        create_view_on_db(spark, createViewList, actv_dbName, json_params, is_create_view)

    """
    Capture End Time in rpt
    """
    end_time = str(datetime.now())
    msg = "\n-- End of Report --\n"
    msg = msg + "  End Time : " + end_time + "\n"
    write_to_file(msg, failure_rpt_name, 'a')
    write_to_file(msg, success_rpt_name, 'a')


"""
def read_json()
	To Parse and read Input JSON
	Input: Name of the JSON file
        Output: Parsed dictionary object
"""


def read_json(jsonfilename):
    datastore = {}
    try:
        if jsonfilename:
            with open(jsonfilename, 'r') as jsonfile:
                datastore = json.load(jsonfile)
    except Exception as ex:
        logger.error("Error Reading Input JSON :: Failed with error {0}".format(ex))
        sys.exit(1)

    currenttablelist = list(datastore["ingestion_config"]["datafeed"].keys())
    params = {}

    # Populate JSON params for all tables
    params["source"] = datastore["ingestion_config"]["source"]
    params["table_list"] = []
    tables = datastore["ingestion_config"]["datafeed"]

    for table_nm in tables:
        table_attr = datastore["ingestion_config"]["datafeed"][table_nm]
        params["table_list"].append(table_nm)
        params[table_nm] = {}
        params[table_nm]["cdc_config"] = table_attr["cdc_config"]

    return params


"""
def write_to_file()
	To Write or Append to a file
	Input: String to Write, File Name, Action [w/a]
"""


def write_to_file(write_str, filename, action):
    with open(filename, action) as outfile:
        outfile.write(write_str)


"""
def parse_cmd_args()
	To Parse cmd line arguments
	Output: Parsed arg list
"""


def parse_cmd_args():
    cmd_parser = argparse.ArgumentParser(description="Create View for Active View and Active Persistent Approach")
    cmd_parser.add_argument("jsonfilename", default="", help="Input JSON File to retrieve the Primary Key and TS_COL ")
    cmd_parser.add_argument("hist_dbName", default="", help="History Database Name ")
    cmd_parser.add_argument("is_create_view", default="N", help=" 'Y' is View has to be created")
    cmd_parser.add_argument("actv_dbName", default="", help="Active Database Name if view has to be created ")

    cmd_args = cmd_parser.parse_args()
    return cmd_args


"""
def create_view_files():
	To Create HQL files for Active View Creation on all the tables of the History DB.
	Input: spark,hist_dbName,json_params
	Output: Create View List for all the tables in the History table.
"""


def create_view_files(spark, hist_dbName, json_params):
    seperator = ','
    source = json_params["source"]
    cons_filename = source + "/" + source + "_CREATE_ACTVIEW_ALL.hql"
    DBInfo = {}
    createViewList = {}
    createViewDBList = {}

    # Initialize Failure report name.
    failure_rpt_name = source + "/" + source + "_failure.rpt"

    # Get tables list from history table
    try:
        spark.sql('use ' + hist_dbName)
    except Exception as ex:
        msg = "create_view_files():: Unable to Access History Database : " + hist_dbName
        write_to_file(msg, failure_rpt_name, 'a')
        sys.exit(1)

    df_hist_tables = spark.sql('show tables')
    hist_tables = ([row['tableName'] for row in df_hist_tables.collect()])

    DBInfo['hist_dbName'] = hist_dbName
    DBInfo['tablelist'] = hist_tables
    DBInfo['tabledetails'] = {}

    # For each table, get column list and create view based on the inputs
    write_to_file("-- CREATE ACTIVE VIEW FOR :: " + source + "\n", cons_filename, 'w')

    for table in hist_tables:
        try:
            df_table = spark.sql('select * from ' + table)
            tableInfo = df_table.columns
            active_view = table.upper().replace("_HISTORICAL", "")

            # Getting the key_cols and update_ts_col from the INPUT JSON.
            pk_col = json_params[active_view]["cdc_config"]["key_cols"]
            ts_col = json_params[active_view]["cdc_config"]["update_ts_col"]

            # To be changed to active_view!!!!!!!
            # temp=json_params[active_view]["cdc_config"]["active_view"]
            temp = json_params[active_view]["cdc_config"]["active_table"]
            temp1 = temp.split(".")
            actv_dbName = temp1[0]
            active_view = temp1[1]

            # Populate String to capture the Create View DDL
            init_str = "\n------------------------------------------------------------------------------------------------------------\n"
            init_str = "\n" + init_str + "--      Create View for Table :: " + table.upper() + "      --" + init_str
            createViewStr = "CREATE OR REPLACE VIEW " + active_view.upper() + " as \n SELECT \n "
            createViewStr = createViewStr + "  " + seperator.join(tableInfo) + "\n FROM \n"
            createViewStr = createViewStr + "   (SELECT *, ROW_NUMBER() OVER (PARTITION BY " + seperator.join(pk_col)
            createViewStr = createViewStr + " ORDER BY " + ts_col
            createViewStr = createViewStr + " DESC) RN FROM "
            createViewStr = createViewStr + hist_dbName + "." + table + " ) tmp\n WHERE RN = 1 \n\n"

            # Appending to Consolidate File
            out_str = init_str + createViewStr
            write_to_file(out_str, cons_filename, 'a')

            # Writing to Individual File
            ind_filename = source + "/" + source + "_" + active_view.upper() + ".hql"
            out_str = init_str + "USE " + actv_dbName + "\n\n " + createViewStr
            write_to_file(out_str, ind_filename, 'w')

            # Return Create View Dic Object.
            createViewList.update({active_view: createViewStr})
            # createViewDBList.update({active_view:actv_dbName})
        except Exception as ex:
            msg = "create_view_files():: Unable to Create Script for :: " + table + "\n"
            write_to_file(msg, failure_rpt_name, 'a')

    return createViewList


"""
def create_view_on_db()
	Create Active View in the Input Active View DB. If object already exists, it will be writing to Failure, else will create view and write to success
	Input: spark,createViewList,actv_dbName,json_params
"""


def create_view_on_db(spark, createViewList, actv_dbName, json_params, is_create_view):
    source = json_params["source"]
    try:
        spark.sql('use ' + actv_dbName)
    except Exception as ex:
        msg = "create_view_on_db():: Unable to Access Active Database : " + actv_dbName
        write_to_file(msg, failure_rpt_name, 'a')
        sys.exit(1)

    df_actv_tables = spark.sql('show tables')
    actv_tables = ([row['tableName'].upper() for row in df_actv_tables.collect()])

    failure_rpt_name = source + "/" + source + "_failure.rpt"
    success_rpt_name = source + "/" + source + "_success.rpt"
    for table in createViewList:
        if table in actv_tables:
            write_to_file(table + " :: View or Table with same name exists. ", failure_rpt_name, 'a')
            """
            # This is Special handling to drop tables!!!.. Use Carefully!
            try:
                spark.sql("Drop Table "+table)
                write_to_file("N Dropped!!! \n",failure_rpt_name,'a')
            except Exception as ex:
                spark.sql("Drop View "+table)
                write_to_file("N Dropped!!! \n",failure_rpt_name,'a')
            write_to_file("\n\n",failure_rpt_name,'a')
            """
        else:
            try:
                spark.sql(createViewList[table])
                write_to_file(table + ":: Created ", success_rpt_name, 'a')

                if is_create_view == 'V':
                    try:
                        df_act_v_select = spark.sql("Select 1 from " + table + " Limit 1")
                        df_act_v_select.collect()
                        write_to_file(" and Validated ", success_rpt_name, 'a')
                    except Exception as ex:
                        write_to_file(table + " :: Validation of Active View Failed ")
            except Exception as ex:
                write_to_file(table + " :: Unable to Create Active View \n", failure_rpt_name, 'a')
            write_to_file("\n\n", success_rpt_name, 'a')


"""
def run_cmd()
	Runs Operating system commands.
	Input: Argument list of OS command line
"""


def run_cmd(args_list):
    """
    Method to execute Linux Commands...
    :param args_list: Linux command separated with each word supplied as arguments list
    :return: s_return: Return code of the linux command executed
             s_output: Output message of the linux command executed
             s_err: Error message of the linux command executed
    """
    try:
        logger.debug('Running Command: {0}'.format(' '.join(args_list)))
        proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        s_output, s_err = proc.communicate()
        s_return = proc.returncode
        return s_return, s_output, s_err
    except Exception as ex:
        logger.error("run_cmd():: {0}".format(ex))


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s: [%(levelname)s]: %(name)s: %(message)s")

    main()
