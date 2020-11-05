# coding=utf-8
import os
import sys
import re
import fnmatch
import shutil
import subprocess
import glob
import logging

from datetime import datetime
from dateutil import parser
from decimal import Decimal, getcontext, ROUND_FLOOR
from ingestion_fw.metastore.dbmanager import DbManager

# Decimal pattern matching
decimal_regex = 'decimal.(?P<precision>\d{1,2}).(?P<scale>\d{1,2}).'
str_date_format = "%Y-%m-%d"
IntFields = ["tinyint", "smallint", "int"]
LongFields = ["bigint"]
StringFields = ["string", "varchar", "char"]
FloatFields = ["float", "double"]
DateFields = ["timestamp", "date"]


def reverse(string):
    """
    Method to reverse a string.. This is required to extract date value from the file name and then do date validation
    :param string: string value to be reversed
    :return: reversed string
    """
    string = string[::-1]
    return string


def num_of_records(full_path):
    """
    Method to count the number of records in a file...
    :param full_path: full path of the file for which number of records to be counted
    :return: number of records in the file
    """
    f = open(full_path)
    nr_of_lines = sum(1 for line in f)
    f.close()
    return nr_of_lines


def valid_file_date(date_text, file_date_format):
    """
    Method to validate the date format in the file name...
    :param date_text: the date value extracted from the file name in string format
    :param file_date_format: the date format expected in the file name. This is passed in the configuration file
    :return: True: if the date format validation is successful
             False: for exceptions, print the exception message and return False
    """
    logger = logging.getLogger(__name__)
    try:
        if date_text == datetime.strptime(date_text, file_date_format).strftime(file_date_format):
            return True
    except Exception as ex:
        logger.error("utils.valid_file_date():: {0}".format(ex))
        return False


# Read specific record from a file
def read_record(file, record_number):
    """
    Method to read a specific record from the file...
    :param file: the file name for which a specific record is to be read
    :param record_number: the record number to be read
    :return: returns the record for the record number passed
    """
    logger = logging.getLogger(__name__)
    try:
        line = open(file, "r").readlines()[record_number].rstrip('\r\n')
        return line
    except Exception as ex:
        logger.error("utils.read_record():: {0}".format(ex))


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
        logger.error("utils.run_cmd():: {0}".format(ex))


def get_file_name_and_ext(file_name):
    """
    Extract the filename and extension from the provided file name. including double extension like
    (.dat.gz or .tar.gz)
    :param file_name: file path
    :return: filename and extension
    """
    file, ext = os.path.splitext(file_name)
    if ext.lower() in ['.gz', '.bz2']:
        ext = os.path.splitext(file)[1] + ext
        file = os.path.splitext(file)[0]
    return file, ext


def move_files_to_dst(src_dir_path, dst_dir_path, file_pattern):
    """
    Method to move files from source directory to destination directory in linux OS directories ...
    Multiple files can be moved with this function
    :param src_dir_path: source directory path to read the file pattern
    :param dst_dir_path: destination directory to move the files
    :param file_pattern: file pattern to look for in source directory which need to be moved to destination
    :return: True: Return True for successful file move
             False: For exception and error case Return False
    """
    logger = logging.getLogger(__name__)
    try:
        for file in os.listdir(src_dir_path):
            if fnmatch.fnmatch(file.lower(), file_pattern.lower()):
                src = src_dir_path + "/" + file
                dst = dst_dir_path + "/" + file
                shutil.move(src, dst)
        return True
    except Exception as ex:
        logger.error("utils.move_files_to_dst():: {0}".format(ex))
        return False


def copy_file_to_dst(src_dir_path, dst_dir_path, filename):
    """
    Method to copy files from source directory to destination directory in linux OS directories ...
    One file at a time will be copied here
    :param src_dir_path: source directory path to read the file
    :param dst_dir_path: destination directory to copy the files
    :param filename: file name to look for in source directory which need to be copied to destination
    :return: True: Return True for successful file copy
             False: For exception and error case Return False
    """
    logger = logging.getLogger(__name__)
    try:
        src = src_dir_path + "/" + filename
        dst = dst_dir_path
        shutil.copy(src, dst)
        return True
    except Exception as ex:
        logger.error("utils.copy_file_to_dst():: {0}".format(ex))
        return False


# Cleanup Directory
def cleanup_directory(dir):
    """
    Method to Clean up all the files in a linux directory ...
    :param dir: Directory name which need to be cleaned up
    :return: True: Return True for successful cleanup
    """
    directory = dir + "/*"
    r = glob.glob(directory)
    for i in r:
        os.remove(i)
    return True


def create_dir(path):
    """
    Method to create a directory...
    :param path: directory name to be created
    :return: rc: Return the return code
    """
    logger = logging.getLogger(__name__)
    try:
        if not os.path.exists(path):
            os.makedirs(path)
    except Exception as ex:
        logger.error("utils.create_dir():: {0}".format(ex))


def delete_dir(path):
    """
    Method to delete a HDFS directory...
    :param hdfs_path: HDFS directory name to be deleted
    :return: rc: Return the return code
    """
    logger = logging.getLogger(__name__)
    for file in glob.glob(path):
        try:
            os.remove(file)
        except Exception as ex:
            logger.error("utils.delete_dir():: {0}".format(ex))


def move_files_in_dbfs_location(table_name, inp_path, dst_path):
    """
    Method to move files from source HDFS location to target HDFS location
    :param table_name: the table name to log the error information
    :param inp_path: source HDFS path
    :param dst_path: destination HDFS path
    """
    logger = logging.getLogger(__name__)
    try:
        for file in os.listdir(inp_path):
            shutil.move(inp_path + "/" + file, dst_path)
    except Exception as ex:
        raise Exception("File transfer from HDFS stage to Archive failed for feed " + table_name)
        logger.error("utils.move_files_in_hdfs_location():: {0}".format(ex))


def valid_file_extension(filename, extensions):
    """
    Method to validate the file extension with the extension provided in configuration
    :param filename: filename in which the extension has to be validated
    :param extensions: list of expected extensions for the file pattern provided in the configuration file
    :return: rc: Return the return code
    """
    logger = logging.getLogger(__name__)
    extensions = list(extensions)

    for i, ext in enumerate(extensions):
        if len(ext) < 3:
            logger.error("Invalid file extension provided in configuration")
            sys.exit(1)

        if ext[0] != ".":
            extensions[i] = "." + ext
    # Check if the file extension is any of the extensions provided in configuration
    # if satisfied, below returns True else will return False
    return any(filename.lower().endswith(e.lower()) for e in extensions)


def split_data(widths, record):
    """
    Method to split the record for fixed length file in to columns based on the
    length information provided in the csv file
    :param widths: a list of lists having the starting position of column and end position of column
                   e.g. [[0,5],[6,13]]
    :param record: A record of the data represented as a tuple (row_data)
    :return fields: Return the fields in a list after splitting
    """
    fields = []
    for col_range in widths:
        start_position = col_range[0]
        end_position = col_range[1]
        field_value = record[start_position:end_position]
        fields.append(field_value)
    return fields


def ret_struct_fields(column, col_not_null=None):
    """
    Method accepts column metadata {type:'string', name='name'} and returns a matching StructField to be used with
    Spark Data Frame
    :param column: column metadata {type:'string', name='name'}
    :param col_not_null: list of columns to be made non null ... [Adding not null constraint on the column]
    :return: Returns a StructType ...e.g:StructType([StructField("name", StringType(), True)
    """
    from pyspark.sql.types import StructField
    from pyspark.sql.types import ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType
    from pyspark.sql.types import StringType, NullType, DecimalType, DateType, TimestampType

    # mapping SQL type name to pyspark.sql type object
    type_mappings = {
        "tinyint": ByteType(),
        "smallint": ShortType(),
        "int": IntegerType(),
        "bigint": LongType(),
        "string": StringType(),
        "varchar": StringType(),
        "char": StringType(),
        "timestamp": TimestampType(),
        "date": DateType(),
        "float": FloatType(),
        "double": DoubleType(),
        "decimal": DecimalType()
    }

    col_type = column["type"].lower()
    col_name = column["name"].lower()

    # check if column type is decimal with precision and scale
    pattern = re.compile(decimal_regex)
    if pattern.match(col_type):
        decimal_info = re.search(pattern=pattern, string=col_type)
        return StructField(col_name,
                           DecimalType(int(decimal_info.group('precision')), int(decimal_info.group('scale'))),
                           nullable=True)

    if col_not_null:
        for col in col_not_null:
            if col_name == col:
                return StructField(col_name, type_mappings.get(col_type, NullType()), nullable=False)
    return StructField(col_name, type_mappings.get(col_type, NullType()), nullable=True)


def cast_to_py_types(row, tgt_col, err_bucket, cdc_params, record_log_flag, dt_pttrn=None):
    """
    Method to convert the data to respective python types as per the base schema specified in the tgt_col
    :param row: A record of the data represented as a tuple (row_data)
    :param tgt_col: is a list of dicts with field name and type corresponding to each field or column in the row.
                    The field type is derived from the base hive schema.
                    e.g: [{'type': 'string', 'name': 'col1'}, {'type': 'timestamp', 'name': 'col2'}]
    :param err_bucket: spark accumulator, instance of DQAccumulatorParam to capture all the error messages.
                       This accumulator has been designed to hold 10 error messages for each column in the dataset
    :param seq_num: Batch id information to be stored in error bucket
    :param cdc_params: Table parameters in form of a dictionary
    :param dt_pttrn: is a dictionary with column name as the key and the date-pattern as the value, for timestamp
                     and date fileds, the respective pattern will be used, if provided in the json config..
                     else we will employ the prediction logic to automatically format the date/timestamp based.
    :return: A tuple of columns with typed values

    casting to float(column) before casting to actual data type is necessary..
    e.g. int(float(column)) is necessary.. because the column value is actually of str data type.
         i.e. print(type(column)) gives <type 'str'>
    and the float casting is necessary because of the below
    int(5.0) # result is 5
    int('5.0') # result is below error
      Traceback (most recent call last):
      File "<stdin>", line 1, in <module>
      ValueError: invalid literal for int() with base 10: '5.0'
    int(float('5.0')) # result is 5
    """
    dt_pttrn = dict((key.lower(), value) for key, value in dt_pttrn.items()) if bool(dt_pttrn) else {}
    outarr, col_name, col_type, col_val = [], '', '', ''
    try:
        # logger = logging.getLogger(__name__)
        for idx, column in enumerate(row):
            col_type, col_name = tgt_col[idx]['type'].lower(), tgt_col[idx]['name'].lower()
            col_val = column
            if column is None:
                outarr.append(None)
            elif col_type in StringFields:
                # skip any non utf-8 chars
                column = (re.sub(r'[^\x00-\x7F]+', '', column)).encode('utf-8', errors='ignore') if column else ''
                column = column.replace('\r', '').encode('utf-8', errors='ignore') if column else ''
                outarr.append('') if column.isspace() or len(column) == 0 else outarr.append(column)
            elif (len(str(column)) == 0 or str(column).isspace()) and (col_type in IntFields or
                                                                       col_type in LongFields or
                                                                       col_type in FloatFields or
                                                                       'decimal' in col_type):
                # BDPU389,Added this statement to fix long/int/float do not handle empty value error.
                outarr.append(None)
            elif (len(str(column)) == 0 or str(column).isspace()) and col_type not in DateFields:
                # this is necessary due to the following issue with parquet files
                # https://issues.apache.org/jira/browse/PARQUET-136
                # https://issues.apache.org/jira/browse/HIVE-11558
                outarr.append('')
            elif len(str(column)) == 0 or str(column).isspace():
                outarr.append(None)
            else:
                if col_type in IntFields:
                    outarr.append(int(float(column)))
                elif col_type in LongFields:
                    outarr.append(long(float(column)))
                elif 'decimal' in col_type:
                    pattern = re.compile(decimal_regex)
                    if pattern.match(col_type):
                        decimal_info = re.search(pattern=pattern, string=col_type)
                        getcontext().prec = int(decimal_info.group('precision'))
                    else:
                        # The decimal type has no precision and scale, set to default value from hive & spark
                        # i.e., precision=10 and scale=0
                        getcontext().prec = 10
                    # The precision is applied only when an arithmetic operation is performed
                    # hence multiplying by 1 so that the actual value remains unchanged.
                    # ROUND_FLOOR: Always round down towards negative infinity.
                    getcontext().rounding = ROUND_FLOOR
                    outarr.append(Decimal.from_float(float(column)) * Decimal(1))
                elif col_type in FloatFields:
                    outarr.append(float(column))
                elif col_type in DateFields:
                    if col_type == "timestamp":
                        if bool(dt_pttrn) and col_name in dt_pttrn:
                            dt_val = datetime.strptime(column, dt_pttrn[col_name])
                            if dt_val.year < 1900:
                                dt_val = datetime(1900, dt_val.month, dt_val.day, dt_val.hour, dt_val.minute,
                                                  dt_val.second)
                            outarr.append(dt_val)
                        else:
                            outarr.append(format_ts(column))
                    else:
                        # Date: YYYY-MM-DD
                        if bool(dt_pttrn) and col_name in dt_pttrn:
                            date_val = datetime.strptime(column, dt_pttrn[col_name]).date()
                            outarr.append(date_val)
                        else:
                            outarr.append(format_date(column))
        if len(outarr) == len(tgt_col):
            return tuple(outarr)
        else:
            # Record is malformed possibly a unexpected new line character in one of the columns
            raise Exception('Row columns doesnt match the tgt schema, possibly an unexpected new line character '
                            'in one of the columns')
    except Exception as ex:
        feed_id = cdc_params["feed_id"]
        source = cdc_params["source"]
        seq_num = cdc_params["batch_id"]
        batch_seq_id = cdc_params["batch_seq_id"]
        error_record = row if record_log_flag == "y" else "NA"

        err_bucket += {
            col_name: {'col_name': col_name,
                       'data_type': col_type,
                       'actual_value': col_val,
                       'err_msg': ex.message,
                       'batch_id': seq_num,
                       'batch_seq_id': batch_seq_id,
                       'feed_id': feed_id,
                       'source': source,
                       'run_time': str(datetime.now()),
                       'record': error_record}
        }


def format_date(dt_val):
    """
    Method to auto format the date if the matching patterns are not provided by the user
    :param dt_val: date time value as a string
    :return: datetime.strptime.date() i.e., date object
    """
    if re.match("^[0-9]*$", dt_val) and len(dt_val) > 14:  # Has micro seconds
        return datetime.strptime(dt_val, '%Y%m%d%H%M%S%f').date()
    else:
        return datetime.strptime(str(parser.parse(dt_val).date()), str_date_format).date()


def format_ts(dt_val):
    """
    Method to auto format the timestamp if the matching patterns are not provided by the user
    :param dt_val: date time value as a string
    :return: datetime.datetime i.e., time stamp object
    """
    if re.match('\d{8}-\d{6}.\d{6}', dt_val) or re.match('\d{8}-\d{6}.\d{3}', dt_val):
        dt_new = datetime.strptime(unicode(dt_val), '%Y%m%d-%H%M%S.%f')
    elif re.match('\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{3}', dt_val):
        dt_new = datetime.strptime(unicode(dt_val), '%Y-%m-%d %H:%M:%S.%f')
    elif re.match('\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', dt_val):
        dt_new = datetime.strptime(unicode(dt_val), '%Y-%m-%d %H:%M:%S')
    elif re.match('\d{8}-\d{6}', dt_val):
        dt_new = datetime.strptime(unicode(dt_val), '%Y%m%d-%H%M%S')
    elif re.match('\d{8}$', dt_val):
        dt_new = datetime.strptime(unicode(dt_val), '%Y%m%d')
    elif re.match('\d{4}-\d{2}-\d{2}-\d{2}.\d{2}.\d{2}.\d{6}', dt_val):
        dt_new = datetime.strptime(unicode(dt_val), '%Y-%m-%d-%H.%M.%S.%f')
    elif re.match("^[0-9]*$", dt_val):
        if len(dt_val) >= 17:  # Has micro seconds
            dt_new = datetime.strptime(dt_val, '%Y%m%d%H%M%S%f')
        else:
            dt_new = datetime.strptime(dt_val, '%Y%m%d%H%M%S')
    else:
        date_field = re.sub('(\d+)[T|\sT\s]+(\d+)', r'\1 \2', dt_val)
        date_part = (date_field.split(' ')[0]).strip()
        # Replace T from the timestamp if exists and extract the time part
        # and replace . if present with :
        # i.e., HH.MM.SS will be converted to HH:MM:SS only if the time_part is seperated by .
        # for microseconds. HH:MM:SS.fff will be used as is
        if len((date_field.split(' ')[1].split('.'))) > 2:
            time_part = ((date_field.split(' ')[1]).replace('.', ':', 2)).strip()
        else:
            time_part = ' '.join(date_field.split(' ')[1:]).strip() if len(
                date_field.split(' ')) > 2 else (
                date_field.split(' ')[1]).strip()

        # concatenate date_part and time_part to create datetime string
        dt_datetime = date_part + ' ' + time_part
        dt_new = parser.parse(dt_datetime)

    # If the date time has microseconds, convert it to target timestamp with microseconds
    # i.e., Timestamp: yyyy-mm-dd hh:mm:ss[.f...].
    if dt_new.microsecond > 0:
        return datetime.strptime(unicode(dt_new), '%Y-%m-%d %H:%M:%S.%f')
    else:
        # If the date time doesnt have microseconds, convert it to target timestamp
        # with microseconds i.e., Timestamp: yyyy-mm-dd hh:mm:ss.
        return datetime.strptime(unicode(dt_new), '%Y-%m-%d %H:%M:%S')


def insert_record_batch_seq_detail(feed_id, next_seq_id, job_id, status, prop_file_path):
    """
    Insert a new record for this batch run with status as "In-Prgoress"
    :param feed_id: feed id for this source
    :param next_seq_id: new sequence number to be inserted
    :param status: status of the run (In-Progress/Completed/Failed)
    :return: None.. Inserts a record into the batch_seq_details table
    """
    logger = logging.getLogger(__name__)
    try:
        dbmanager = DbManager(prop_file_path)
        table_name = "batch_seq_detail"
        insert_column = ["FEED_ID",
                         "SEQ_ID",
                         "BATCH_DATE",
                         "JOB_ID",
                         "STATUS",
                         "START_TIME",
                         "END_TIME",
                         "CREATE_TIMESTAMP",
                         "CREATED_BY"]
        insert_values = [feed_id,
                         "'" + next_seq_id + "'",
                         "CURRENT_DATE",
                         job_id,
                         status,
                         "CURRENT_TIMESTAMP",
                         "NULL",
                         "CURRENT_TIMESTAMP",
                         "'system'"]
        dbmanager.insert_record(table_name=table_name, columns=insert_column, values=insert_values)
        dbmanager.close_connection()
    except Exception as ex:
        logger.error("insert_in_progress_record_batch_seq_detail():: {0}".format(ex))
        sys.exit(1)


def update_record_batch_seq_detail(status, batch_seq_id, prop_file_path):
    """
    Update batch seq details table with the status of this batch seq number
    :param status: success/failure
    :param batch_seq_id: batch seq id for this run
    :return: None.. Updates record into the batch_seq_details table
    """
    logger = logging.getLogger(__name__)
    try:
        dbmanager = DbManager(prop_file_path)
        table_name = "batch_seq_detail"
        status = "'" + status + "'"
        update_column_dict = [{"status": status}, {"end_time": "CURRENT_TIMESTAMP"}]
        where_cond = "BATCH_SEQ_ID = " + str(batch_seq_id)
        dbmanager.update_record(table_name=table_name,
                                update_column=update_column_dict,
                                where_clause=where_cond)
        dbmanager.close_connection()
    except Exception as ex:
        logger.error("update_record_batch_seq_detail():: {0}".format(ex))
        sys.exit(1)


def get_batch_seq_id_for_this_run(feed_id, prop_file_path):
    """
    Retrieve the batch seq id for the inserted record. Batch Seq id is a surrogate key in the table...
    Hence we are not using that column in previous insert statement.
    We have to retrieve this value along with other values to store in a dictionary
    :param feed_id: feed id for this batch
    :param next_seq_id: sequence number inserted in the table -- currently removed
    :return: dictionary with {batch_seq_id: [feed_id, status]}
    """
    """
    Execute the below SQL Query on Batch_Seq_Detail Table
        e.g. SELECT BATCH_SEQ_ID,
                    FEED_ID,
                    STATUS
               FROM BATCH_SEQ_DETAIL
              WHERE FEED_ID = 201
                AND SEQ_ID = 20180810
                AND STATUS = 'In-Progress'
    """
    logger = logging.getLogger(__name__)
    try:
        dbmanager = DbManager(prop_file_path)
        get_batch_seq_id_query = "SELECT BATCH_SEQ_ID, FEED_ID, STATUS FROM BATCH_SEQ_DETAIL WHERE FEED_ID = " + \
                                 str(feed_id) + " AND STATUS = 'In-Progress'"  # " AND SEQ_ID = " + str(next_seq_id)
        result = dbmanager.execute_query(sql_query=get_batch_seq_id_query)
        dbmanager.close_connection()
        batch_seq_id = ""
        for row in result:
            batch_seq_id = row[0]
        return batch_seq_id
    except Exception as ex:
        logger.error("get_batch_seq_id_for_this_run():: {0}".format(ex))
        sys.exit(1)


def get_seq_id_for_this_run(feed_id, prop_file_path):
    """
        Retrieve the seq id for the succesful inserted record.
        :param feed_id: feed id for this batch
        :return: seq_id: sequence number inserted in the table
        """
    """
    Execute the below SQL Query on Batch_Seq_Detail Table
        e.g. SELECT SEQ_ID,
                    FEED_ID,
                    STATUS
               FROM BATCH_SEQ_DETAIL
              WHERE FEED_ID = 201
                AND STATUS = 'In-Progress'
                order by end_time desc limit 1
    """
    logger = logging.getLogger(__name__)
    try:
        dbmanager = DbManager(prop_file_path)
        get_seq_id_query = "SELECT SEQ_ID, FEED_ID, STATUS FROM BATCH_SEQ_DETAIL WHERE FEED_ID = " + \
                           str(feed_id) + " AND STATUS = 'In-Progress' order by end_time desc limit 1"
        result = dbmanager.execute_query(sql_query=get_seq_id_query)
        seq_id = ""
        for row in result:
            seq_id = row[0]
        return seq_id
    except Exception as ex:
        logger.error("get_seq_id_for_this_run():: {0}".format(ex))
        sys.exit(1)


def get_seq_id_for_succesful_run(feed_id, prop_file_path):
    """
    Retrieve the seq id for the succesful inserted record.
    :param feed_id: feed id for this batch
    :return: seq_id: sequence number inserted in the table
    """
    """
    Execute the below SQL Query on Batch_Seq_Detail Table
        e.g. SELECT SEQ_ID,
                    FEED_ID,
                    STATUS
               FROM BATCH_SEQ_DETAIL
              WHERE FEED_ID = 201
                AND STATUS = 'Success'
                order by end_time desc
    """
    logger = logging.getLogger(__name__)
    try:
        dbmanager = DbManager(prop_file_path)
        get_seq_id_query = "SELECT SEQ_ID, FEED_ID, STATUS FROM BATCH_SEQ_DETAIL WHERE FEED_ID = " + \
                           str(feed_id) + " AND STATUS = 'Success' " \
                                          "order by end_time desc limit 1"  # " AND SEQ_ID = " + str(next_seq_id)
        result = dbmanager.execute_query(sql_query=get_seq_id_query)
        dbmanager.close_connection()
        seq_id = ""
        for row in result:
            seq_id = row[0]
        return seq_id
    except Exception as ex:
        logger.error("get_seq_id_for_latest_successful_run():: {0}".format(ex))
        sys.exit(1)


def insert_record_into_batch_file_detail(batch_seq_id, file_name, file_processed_status, failed_stage,
                                         prop_file_path):
    """
    Insert a success/failure record into batch_file_detail table
    :param batch_seq_id: batch seq id for this run
    :param file_name: current file name for this batch
    :param file_processed_status: file processed status whether success or failure
    :param stage: if status is failed, the stage name where the failure occurred
    :return: None
    """
    logger = logging.getLogger(__name__)
    try:
        dbmanager = DbManager(prop_file_path)
        table_name = "batch_file_detail"
        insert_column = ["BATCH_SEQ_ID",
                         "FILE_NAME",
                         "FILE_PROCESSED_STATUS",
                         "FAILED_STAGE"]
        insert_values = [batch_seq_id,
                         file_name,
                         file_processed_status,
                         failed_stage]
        dbmanager.insert_record(table_name=table_name, columns=insert_column, values=insert_values)
        dbmanager.close_connection()
    except Exception as ex:
        logger.error("insert_record_into_batch_file_detail():: {0}".format(ex))
        sys.exit(1)


def remove_enclosed_quotes(rdd, flag):
    """
    To remove enclosed char in the values
    :param rdd: takes rdd
    :param flag: takes flag whether to remove double quotes
    :return: returns cleandup RDD
    """
    logger = logging.getLogger(__name__)
    quote_char = '"'
    if flag == "Y":
        logger.info("Removing enclosed double quotes starting...")
        return rdd.map(lambda x: tuple([v.strip(quote_char) for v in list(x)]))
    else:
        return rdd
