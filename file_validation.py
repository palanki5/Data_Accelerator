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

# Move every files/folder to trash folder which need to be deleted.
trash_dir = "/dbfs/mnt/ops/deleted-files/"
create_dir(trash_dir)


# Print Start Dags message
def start_dags():
    logger = logging.getLogger(__name__)
    logger.debug("Filevalidation STARTED RUNNING")


# Print end dags message
def end_dags():
    logger = logging.getLogger(__name__)
    logger.debug("Filevalidation HAS COMPLETED SUCCESSFULLY..")


# When job get failed, update batch seq table with failed status and clear staging area
def error_rollback(processed_file_list):
    """
    Rollback all the staging directories where files have been copied
    :param processed_file_list: List of the files for which successful validation has already been done
    :return: None
    """
    # clear staging area
    for stg_file in processed_file_list:
        #os.remove(stg_file)
        trash_path = os.path.join(trash_dir, datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S-%f'))
        create_dir(trash_path)
        for data in glob.glob(stg_file):
            shutil.move(data, trash_path)

    # update batch_seq tables with status failed:
    update_record_batch_seq_detail(status="Failed",
                                   batch_seq_id=batch_seq_id,
                                   prop_file_path=db_connection_prop)


def parse_cmd_args():
    """
    add the command line arguments that are to be parsed.
    :return: the parsed arguments
    """
    cmd_parser = argparse.ArgumentParser(description="Ingestion Framework File Validation module")
    cmd_parser.add_argument("etl_config_file_path", help="path to the ETL configuration file for this job")
    cmd_parser.add_argument("db_connection", help="Postgres database connection property file")
    cmd_parser.add_argument("job_id", help="the job id of the run that is currently being processed")
    cmd_args = cmd_parser.parse_args()
    return cmd_args


def no_file_error(err_code, err_description, data_source):
    """
    Method to Write Error log when there are no files in Input Directory...
    :param err_code: Error code to be populated
    :param err_description: Error description message
    :param data_source: data source name for which error occurred
    :return: exit the job if there are no input files in the input directory
    """
    # os.system(log_path % (err_code, err_description, data_source))
    logger = logging.getLogger(__name__)
    logger.error("Found no files for " + data_source + " in Input Directory " + err_code + " " + err_description)
    sys.exit(1)


def invalid_input_files(data_source, table_name, err_code, err_description,
                        file, stage, error_list):
    """
    Method to Write Error log for all other scenarios...
    :param data_source: data source name for which error occurred
    :param table_name: table name for which error occurred
    :param err_code: Error code to be populated
    :param err_description: Error description message
    :param file: filename where error occurred
    :param stage: stage in which error occurred
    :param error_list: a list of dictionaries to hold all the error codes and messages and table name occurred
                        for this run
    :return: error_list to be returned.. Error handling mechanism has to be handled here..
    """
    # os.system(log_path % (err_code, err_description, data_source))
    logger = logging.getLogger(__name__)
    logger.error("Incorrect file validation for " + data_source + " " + err_code + " " + err_description)

    # Create a temp dictionary and add the error information to the dictionary
    error_output = {}
    error_output["data_source"] = data_source
    error_output["table_name"] = table_name
    error_output["error_code"] = err_code
    error_output["error_desc"] = err_description
    error_output["filename"] = file
    error_output["stage"] = stage

    # Append the temp dictionary to the list of dictionary data structure element
    error_list.append(error_output)


def check_for_empty_ip_dir(data_source, path_dict):
    """
    Method to check if the input directory is empty for the source system..
    If empty, then no files were received for the source system,
    Log an entry in Error Table and Fail the Job and Exit
    :param data_source: the data source name to log the error information
    :param path_dict: a dictionary having the list of the OS directory paths
    """
    logger = logging.getLogger(__name__)
    try:
        inp_dir_path = path_dict["inp_dir_path"]
        if os.listdir(inp_dir_path) == []:
            logger.debug("NO FILES FOUND IN INPUT DIRECTORY..... !!")

            # Make an entry in Error Table...
            err_code = "1"
            err_description = "No-Files-In-Input-Directory"
            no_file_error(err_code, err_description, data_source)
    except Exception as ex:
        logger.error("check_for_empty_ip_dir():: {0}".format(ex))
        sys.exit(1)


def valid_file_ext(data_source, table_name, table_attr, path_dict, file_pattern, error_list):
    """
    Method to check the file extension for this triplet...
    If the extension doesn't match the list provided in configuration, then move this triplet
    to Error directory and make an entry in Error Table and fail the job
    :param data_source: the data source name to log the error information
    :param table_name: the table name to log the error information
    :param table_attr: Table properties to be read from the configuration json
    :param path_dict: a dictionary having the list of the OS directory paths
    :param error_list: error list where the error messages and code need to be appended
    :return: True: Return True for successful validation
             False: Return False for unsuccessful validation
    """
    logger = logging.getLogger(__name__)
    try:
        inp_dir_path = path_dict["inp_dir_path"]
        err_dir_path = path_dict["err_dir_path"]

        file_extensions = table_attr["validation"]["triplet_check"]["file_extensions"]

        for file in os.listdir(inp_dir_path):
            if fnmatch.fnmatch(file.lower(), file_pattern.lower()):
                if not valid_file_extension(file, file_extensions):
                    # Move all the triplet files respective to this feed to error table
                    move_files_to_dst(inp_dir_path, err_dir_path, file_pattern)

                    # Make an entry in Error Table..
                    err_code = "2"
                    err_description = "Invalid file extension for feed " + table_name
                    invalid_input_files(data_source, table_name, err_code, err_description,
                                        file=file_pattern, stage="File Extenstion Check", error_list=error_list)

                    return False
        return True
    except Exception as ex:
        logger.error("valid_file_ext():: {0}".format(ex))
        error_rollback(processed_file_list)
        return False


def one_file_foreach_ext(data_source, table_name, table_attr, path_dict, file_pattern, error_list):
    """
    Method to check for each file extension if we have received at least one file or not...
    For mismatch, send all the files to error directory
    :param data_source: the data source name to log the error information
    :param table_name: the table name to log the error information
    :param table_attr: Table properties to be read from the configuration json
    :param path_dict: a dictionary having the list of the OS directory paths
    :param error_list: error list where the error messages and code need to be appended
    :return: True: Return True for successful validation
             False: Return False for unsuccessful validation
    """
    logger = logging.getLogger(__name__)

    try:
        inp_dir_path = path_dict["inp_dir_path"]
        err_dir_path = path_dict["err_dir_path"]

        file_extensions = table_attr["validation"]["triplet_check"]["file_extensions"]

        # Dictionary to capture how many files received for each extension provided in configuration
        ext_dict = {}

        for file in os.listdir(inp_dir_path):
            if fnmatch.fnmatch(file.lower(), file_pattern.lower()):
                for ext in file_extensions:
                    match = 0
                    if fnmatch.fnmatch(file.lower(), ("*" + ext.lower())):
                        match += 1
                        ext_dict.update({ext: match})

        for ext in file_extensions:
            if ext not in ext_dict.keys():
                # Move all the triplet files respective to this feed to error table
                move_files_to_dst(inp_dir_path, err_dir_path, file_pattern)

                # Make an entry in Error Table..
                err_code = "2"
                err_description = "All extension files not received " + table_name
                invalid_input_files(data_source, table_name, err_code, err_description,
                                    file=file_pattern, stage="One file for each extension", error_list=error_list)

                logger.error("Didn't receive files for extension " + ext + " for feed " + table_name)
                return False
        return True
    except Exception as ex:
        logger.error("one_file_foreach_ext():: {0}".format(ex))
        error_rollback(processed_file_list)
        return False


def valid_file_date_format(data_source, table_name, table_attr, path_dict, file_pattern, error_list):
    """
    Method to check the date extracted from the file name and validate with date format...
    :param data_source: the data source name to log the error information
    :param table_name: the table name to log the error information
    :param table_attr: Table properties to be read from the configuration json
    :param path_dict: a dictionary having the list of the OS directory paths
    :param error_list: error list where the error messages and code need to be appended
    :return: True: Return True for successful validation
             False: Return False for unsuccessful validation
    """
    logger = logging.getLogger(__name__)

    try:
        inp_dir_path = path_dict["inp_dir_path"]
        err_dir_path = path_dict["err_dir_path"]

        file_date_format = table_attr["validation"].setdefault("file_date_format", None)

        # Scenario where filename will not have any timestamp value
        # e.g. QCCRAH.ADBOR.EDW.TAB003.D190909.DAT
        if file_date_format is None:
            return True

        if file_date_format not in (None, ""):
            for filename in os.listdir(inp_dir_path):
                file, ext = get_file_name_and_ext(filename)
                filename_to_be_matched = file + ext.lower()
                if fnmatch.fnmatch(filename_to_be_matched, file_pattern):
                    rem_ext = filename.split('.', 2)[0]
                    date_str = reverse(reverse(rem_ext).split('_', 2)[0])

                    if not valid_file_date(date_str, file_date_format):
                        # Move all the triplet files respective to this feed to error table
                        move_files_to_dst(inp_dir_path, err_dir_path, file_pattern)

                        # Make an entry in Error Table..
                        err_code = "7"
                        err_description = "Invalid date format for feed " + table_name
                        invalid_input_files(data_source, table_name, err_code, err_description,
                                            file=file_pattern, stage="File Date Format Check", error_list=error_list)

                        return False
        return True
    except Exception as ex:
        logger.error("valid_file_date_format():: {0}".format(ex))
        error_rollback(processed_file_list)
        return False


def triplet_check(data_source, table_name, table_attr, path_dict, file_pattern, error_list):
    """
    Method to check the triplet for each table for the source system...
    :param data_source: the data source name to log the error information
    :param table_name: the table name to log the error information
    :param table_attr: Table properties to be read from the configuration json
    :param path_dict: a dictionary having the list of the OS directory paths
    :param error_list: error list where the error messages and code need to be appended
    :return: True: Return True for successful validation
             False: Return False for unsuccessful validation
    """
    logger = logging.getLogger(__name__)
    try:
        inp_dir_path = path_dict["inp_dir_path"]
        err_dir_path = path_dict["err_dir_path"]
        file_count = table_attr["validation"]["triplet_check"]["file_count"]

        # Count the number of files received for this feed
        number_of_files = 0

        for filename in os.listdir(inp_dir_path):
            file, ext = get_file_name_and_ext(filename)
            filename_to_be_matched = file + ext.lower()
            if fnmatch.fnmatch(filename_to_be_matched, file_pattern):
                number_of_files += 1

        # Check if the complete triplet set has been received or not..
        # if mismatch move the files to error directory and make an entry in Error Table...
        if number_of_files != file_count:
            logger.debug("File Count mismatch for table " + table_name + " ......")
            logger.debug("Expected {0} files, but have received only {1} files...".format(file_count,
                                                                                          number_of_files))
            logger.debug("This triplet will be moved to error directory")

            # Move all the triplet files respective to this feed to error table
            move_files_to_dst(inp_dir_path, err_dir_path, file_pattern)

            # Make an entry in Error Table..
            err_code = "8"
            err_description = "FileCount_Mismatch for Feed " + table_name
            invalid_input_files(data_source, table_name, err_code, err_description,
                                file=file_pattern, stage="Triplet Check", error_list=error_list)

            return False
        return True
    except Exception as ex:
        logger.error("triplet_check():: {0}".format(ex))
        error_rollback(processed_file_list)
        return False


def record_count_match(data_source, table_name, table_attr, path_dict, file_pattern, error_list):
    """
    Method to validate the record count provided in ctl file against actual number of records
    received in the data file....
    :param data_source: the data source name to log the error information
    :param table_name: the table name to log the error information
    :param table_attr: Table properties to be read from the configuration json
    :param path_dict: a dictionary having the list of the OS directory paths
    :param error_list: error list where the error messages and code need to be appended
    :return: True: Return True for successful validation
             False: Return False for unsuccessful validation
    """
    logger = logging.getLogger(__name__)
    try:
        header_flag = table_attr["validation"]["record_count_validation"].get("header", 'N').lower()
        footer_flag = table_attr["validation"]["record_count_validation"].get("footer", 'N').lower()
        rec_cnt_pos_ctl = table_attr["validation"]["record_count_validation"]["rec_cnt_pos_ctl"]
        rec_num_in_ctl = int(table_attr["validation"]["record_count_validation"].get("rec_num_in_ctl", "1")) - 1
        file_count = table_attr["validation"]["triplet_check"]["file_count"]
        audit_feed_id= table_attr["audit_feed_id"]
        audit_job_id=table_attr["audit_job_id"]
        audit_seq_id=table_attr["audit_seq_id"]
        inp_dir_path = path_dict["inp_dir_path"]
        err_dir_path = path_dict["err_dir_path"]

        # Read Control file and extract the value provided in the range in the conf file
        # If control file has header, then extract the value from 2nd record
        # else extract the value from 1st record
        rec_cnt_in_file = 0
        rec_cnt_in_ctl = 0
        for filename in os.listdir(inp_dir_path):
            file, ext = get_file_name_and_ext(filename)
            filename_to_be_matched = file + ext.lower()
            if fnmatch.fnmatch(filename_to_be_matched, file_pattern):
                # Extract the number of records provided in Control file
                if filename.lower().endswith(".ctl"):
                    filename = inp_dir_path + "/" + filename
                    ctl_rec = read_record(filename, rec_num_in_ctl)

                    if ctl_rec is None:
                        # Make an entry in Error Table..
                        err_code = "9"
                        err_description = "Control file Empty for Feed " + table_name
                        invalid_input_files(data_source, table_name, err_code, err_description,
                                            file=file_pattern, stage="Record Count MisMatch", error_list=error_list)
                        return False

                    if ":" in rec_cnt_pos_ctl:
                        start_pos = int(rec_cnt_pos_ctl.split(":", 2)[0])
                        end_pos = int(rec_cnt_pos_ctl.split(":", 2)[1])
                        rec_cnt_in_ctl = int(ctl_rec[start_pos:end_pos])
                    else:
                        # String-escape is used to handle unicode character as field delimiter
                        # In such cases the respective value should be passed in json should be of format as below
                        # table_attr = {"format": "delimited", "delimiter": "\\xc2\\xa6"}
                        # The above value is for the broken pipe (¦) character as field delimiter
                        data_file_delimiter = table_attr.setdefault("delimiter", "").decode('string-escape')
                        ctl_file_delimiter = table_attr["validation"]["record_count_validation"].\
                            setdefault("delimiter", data_file_delimiter).decode('string-escape')
                        if not ctl_file_delimiter:
                            logger.error("Delimiter value not provided for table {} in either control or data file.."
                                         .format(table_name))

                            # Make an entry in Error Table..
                            err_code = "5"
                            err_description = "Control file delimiter not provided for table " + table_name
                            invalid_input_files(data_source, table_name, err_code, err_description,
                                                file=file_pattern, stage="Control file delimiter not provided",
                                                error_list=error_list)

                            # Rollback the processed files
                            return False
                        else:
                            rec_cnt_in_ctl = int(ctl_rec.split(ctl_file_delimiter)[int(rec_cnt_pos_ctl) - 1])

                # Count the number of records if the data file is a compressed file
                if filename.lower().endswith(".zip") | \
                   filename.lower().endswith(".dat.gz") | \
                   filename.lower().endswith(".bz2"):
                    filename = inp_dir_path + "/" + filename
                    compressedfilename = compressedFile(filename)
                    rec_cnt_in_file = int(compressedfilename.countLines())
                    # Subtract 1 from record count if there is header and footer
                    rec_cnt_in_file = rec_cnt_in_file - 1 if header_flag == 'y' else rec_cnt_in_file
                    rec_cnt_in_file = rec_cnt_in_file - 1 if footer_flag == 'y' else rec_cnt_in_file

                # Count the number of records if the data file is uncompressed file
                if filename.lower().endswith(".dat") | \
                   filename.lower().endswith(".txt") | \
                   filename.lower().endswith(".csv"):
                    filename = inp_dir_path + "/" + filename
                    # Databricks Runtime 5.5 and below doesnot support local api for file I/O if file size is >2 GB
                    #https: // docs.databricks.com / data / databricks - file - system.html  # access-dbfs-using-local-file-apis
                    #implemting file row count through Spark way. Since Databricks already import Spark, no need of import it.
                    #rec_cnt_in_file = num_of_records(filename)
                    rec_cnt_in_file = spark.read.csv(filename.lstrip("/dbfs")).count()
                    # Subtract 1 from record count if there is header and footer
                    rec_cnt_in_file = rec_cnt_in_file - 1 if header_flag == 'y' else rec_cnt_in_file
                    rec_cnt_in_file = rec_cnt_in_file - 1 if footer_flag == 'y' else rec_cnt_in_file

        # If record count in ctl and dat file don't match, move the triplet to error directory
        if file_count == 3 and (rec_cnt_in_ctl != rec_cnt_in_file):
            logger.debug("Record Count mismatch in ctl and data file for feed " + table_name)
            logger.debug("data file Record count: " + str(rec_cnt_in_file))
            logger.debug("ctl  file Record count: " + str(rec_cnt_in_ctl))

            # Move all the triplet files respective to this feed to error table
            move_files_to_dst(inp_dir_path, err_dir_path, file_pattern)

            # Make an entry in Error Table..
            err_code = "6"
            err_description = "RecordCount_Mismatch for Feed " + table_name + ">>AUDIT:Ctl_rec-Dat_rec,"+str(rec_cnt_in_ctl)+","+str(rec_cnt_in_file)
            invalid_input_files(data_source, table_name, err_code, err_description,
                                file=file_pattern, stage="Record Count MisMatch", error_list=error_list)

            return False

        #Audit Logging
        metrics=ingestion_audit_metrics_dict()
        audit_source = str(inp_dir_path+"/"+file_pattern).replace("//","/")
        metrics.update({"data_source":data_source,"ctrl_file_count":rec_cnt_in_ctl,"dat_file_count":rec_cnt_in_file,"success_msg":"Record Count Matched"})
        log_dict={"run_id": str(audit_job_id),"key":"'"+audit_seq_id+"|"+str(audit_feed_id)+"'","event_type": "'Ingestion'",
                  "event": "'FILE_VALIDATION'","source": "'"+str(audit_source)+"'",
                  "target": "'NA'","source_type": "'File'","target_type": "'NA'",
                  "status": "'Success'","metrics": metrics}
        log_audit_entry(log_dict,db_connection_prop)

        return True
    except Exception as ex:
        logger.error("record_count_match():: {0}".format(ex))
        error_rollback(processed_file_list)
        return False


def move_files_to_output(data_source, table_name, path_dict,
                         table_path, file_pattern, error_list):
    """
    Method to move files copy the data file from landing path to staging path
    :param data_source: the data source name to log the error information
    :param table_name: the table name to log the error information
    :param path_dict: a dictionary having the list of the OS directory paths
    :param table_path: staging path where the data file needs to be copied
    :param next_batch_seq_num: sequence number to be processed in the run
    :param error_list: error list to be captured
    :return: True: Return True for successful file copy to staging area
             False: Return False for unsuccessful file copy to staging area
    """
    logger = logging.getLogger(__name__)
    try:
        inp_dir_path = path_dict["inp_dir_path"]
        src_dir_path = path_dict["src_dir_path"]
        err_dir_path = path_dict["err_dir_path"]

        # Create temp directory in the /tmp/ folder on the edge node for the unzip to happen
        unzip_dir_path = src_dir_path
        create_dir(unzip_dir_path)

        # If the data file is in zipped format, copy the file to src directory.. Unzip the file in src directory
        # Then copy the file to staging location and delete the file from src directory
        for filename in os.listdir(inp_dir_path):
            file, ext = get_file_name_and_ext(filename)
            filename_to_be_matched = file + ext.lower()
            if fnmatch.fnmatch(filename_to_be_matched, file_pattern):
                if filename.lower().endswith(".zip"):
                    try:
                        zipfile_path = inp_dir_path + "/" + filename
                        zip_ref = zipfile.ZipFile(zipfile_path)  # create zipfile object
                        zip_ref.extractall(unzip_dir_path)  # Extract file to temp directory local
                        zip_ref.close()  # close file

                        # Move the file from temp directory to staging staging
                        for unzipped_filename in os.listdir(unzip_dir_path):
                            if fnmatch.fnmatch(unzipped_filename.lower(), file_pattern.lower()):
                                src_file = unzip_dir_path + "/" + unzipped_filename
                                dst_path = table_path
                                try:
                                    shutil.copy(src_file, dst_path)
                                except Exception as ex:
                                    # Move all the triplet files respective to this feed to error table
                                    move_files_to_dst(inp_dir_path, err_dir_path,
                                                      file_pattern.lower())
                                    # Make an entry in Error Table..
                                    err_code = "4"
                                    err_description = "File Transfer to staging directory " + table_name
                                    invalid_input_files(data_source, table_name, err_code, err_description,
                                                        file=file_pattern, stage="Copy to staging directory - Failed",
                                                        error_list=error_list)

                                    logger.error(
                                        "File transfer to staging directory failed for feed {0} with error {1}".format(table_name, ex))
                                    # Delete the unzipped file from the temp directory
                                    #os.remove(src_file)
                                    trash_path = os.path.join(trash_dir,
                                                              datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S-%f'))
                                    create_dir(trash_path)
                                    for data in glob.glob(src_file):
                                        shutil.move(data, trash_path)
                                    return False

                                # Delete the unzipped file from the temp directory
                                #os.remove(src_file)
                                trash_path = os.path.join(trash_dir,
                                                          datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S-%f'))
                                create_dir(trash_path)
                                for data in glob.glob(src_file):
                                    shutil.move(data, trash_path)

                        # Exit the process and return
                        return True
                    except Exception as ex:
                        # Move all the triplet files respective to this feed to error table
                        move_files_to_dst(inp_dir_path, err_dir_path, file_pattern)
                        # Cleanup SRC directory
                        #cleanup_directory(src_dir_path)
                        trash_path = os.path.join(trash_dir,
                                                  datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S-%f'))
                        create_dir(trash_path)
                        for data in glob.glob(src_dir_path):
                            shutil.move(data, trash_path)
                        # Make an entry in Error Table..
                        err_code = "4"
                        err_description = "Invalid zip file format for " + table_name + " Error :: {0}".format(ex)
                        invalid_input_files(data_source, table_name, err_code, err_description,
                                            file=file_pattern, stage="Copy to staging directory - Invalid zip file",
                                            error_list=error_list)

                        logger.error("Invalid zip file format for " + filename + " Error :: {0}".format(ex))
                        return False

        # For other types of unzipped files received from source, copy the files to HDFS directly from input path
        for filename in os.listdir(inp_dir_path):
            file, ext = get_file_name_and_ext(filename)
            filename_to_be_matched = file + ext.lower()
            if fnmatch.fnmatch(filename_to_be_matched, file_pattern):
                if not filename.lower().endswith(".ctl") and not filename.lower().endswith(".eot") and \
                        not filename.lower().endswith(".zip") and not filename.lower().endswith(".bz2"):
                    src_path = inp_dir_path + "/" + filename
                    dst_path = table_path
                    try:
                        #shutil.copy(src_path, dst_path)
                        #dbutils library to copy files from Input to staging directory,since shutil or any OS command
                        #is getting failed for large file size.
                        dbutils.fs.cp(src_path.lstrip("/dbfs"), dst_path.lstrip("/dbfs"))
                    except Exception as ex:
                        # Make an entry in Error Table..
                        err_code = "4"
                        err_description = "File Transfer to staging directory " + table_name
                        logger.error("File transfer to staging directory failed for feed {0} with error ".
                                     format(table_name, ex))
                        invalid_input_files(data_source, table_name, err_code, err_description,
                                            file=file_pattern, stage="Copy to staging directory - Failed",
                                            error_list=error_list)
                        move_files_to_dst(inp_dir_path, err_dir_path, file_pattern)
                        return False
        return True
    except Exception as ex:
        logger.error("move_files_to_output():: {0}".format(ex))
        error_rollback(processed_file_list)


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

    # Get the home directory path.. e.g. /data/b2b
    home_dir = jfile["ingestion_config"]["home_dir"]
    mounted_home_dir = "/dbfs/mnt" + home_dir
    # Append Staging Area path to home dir.. e.g. /data/b2b/staging
    staging_dir = mounted_home_dir + "/" + "staging"
    # Append source name to path.. e.g. /data/b2b/staging/flexcab
    source_dir = staging_dir + "/" + data_source

    # Remove these below comments.. these are commented for testing purpose so that job execution takes less time
    # Create home directory if not exists.. Also create source system subdirectory inside home directory
    create_dir(mounted_home_dir)
    create_dir(staging_dir)
    create_dir(source_dir)

    # Get the base directory, input output error archive log location from the configuration file
    base_os_path = jfile["ingestion_config"]["location"]["base_dir"]
    inp_dir_name = jfile["ingestion_config"]["location"]["input_dir"]
    err_dir_name = jfile["ingestion_config"]["location"]["error_dir"]
    src_dir_name = jfile["ingestion_config"]["location"]["unzipped_dir"]

    # Construct landing directory and input output directory paths
    # src_path has been appeneded with feed_id so that if there are multiple jobs under one data source
    # running in parallel, doesn't impact the src directory..
    # e.g. /home/sfxuser/SIT/Replicator/FLEXCAB/src_201
    #      /home/sfxuser/SIT/Replicator/FLEXCAB/src_601 etc.
    data_path = "/dbfs/mnt" + base_os_path
    inp_dir_path = data_path + "/" + inp_dir_name
    err_dir_path = data_path + "/" + err_dir_name
    src_dir_path = data_path + "/" + src_dir_name

    # Construct a dictionary with above values so that it can be passed to functions as needed
    path_dict = {"data_path": data_path,
                 "inp_dir_path": inp_dir_path,
                 "err_dir_path": err_dir_path,
                 "src_dir_path": src_dir_path
                 }

    table_list = jfile["ingestion_config"]["datafeed"]

    start_dags()
    check_for_empty_ip_dir(data_source, path_dict)

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
        raise Exception("Previous run is still In-Progress.. Please process the previous batch first")

        # Audit Logging
        metrics=ingestion_audit_metrics_dict()
        metrics.update({"data_source":data_source,"error_msg":"Previous run is still In-Progress. Please process the previous batch first","error_type":"FILE VALIDATION ERROR"})
        log_dict={"run_id": str(job_id),"key":"'|"+str(feed_id)+"'","event_type": "'Ingestion'",
              "event": "'FILE_VALIDATION'","source": "'NA'",
              "target": "'NA'","source_type": "'NA'","target_type": "'NA'",
              "status": "'Failed'","metrics": metrics}
        log_audit_entry(log_dict,db_connection_prop)
        sys.exit(1)

    # Pre-Fix leading zeros and convert to string type as seq_id is VARCHAR in control table
    next_seq_id_str = str(next_seq_id).zfill(num_seq_length) if num_seq_length is not None else str(next_seq_id)
    logger.info("FileValidation Next Sequence ID Value For This Execution Run..... {0}".format(next_seq_id_str))

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
        table_attr["audit_feed_id"] = feed_id
        table_attr["audit_job_id"] = job_id
        table_attr["audit_seq_id"] = next_seq_id_str
        file_pattern = table_attr["validation"]["triplet_check"]["file_pattern"]

        # Replace the file pattern with the actual sequence number expected for this run
        file_pattern = file_pattern.replace("(<batch_seq>)", next_seq_id_str). \
            replace("(<date_seq>)", next_seq_id_str)

        # Construct the table path and create the table directory if not present
        # e.g. /data/b2b/staging/Flexcab/tab05063
        table_path = source_dir + "/" + table_name

        create_dir(table_path)

        file_path = table_path + "/" + file_pattern

        global processed_file_list
        processed_file_list.append(file_path)

        if valid_file_ext(data_source, table_name, table_attr, path_dict, file_pattern, error_list) and \
                one_file_foreach_ext(data_source, table_name, table_attr, path_dict, file_pattern, error_list) and \
                triplet_check(data_source, table_name, table_attr, path_dict, file_pattern, error_list) and \
                valid_file_date_format(data_source, table_name, table_attr, path_dict, file_pattern, error_list) and \
                record_count_match(data_source, table_name, table_attr, path_dict, file_pattern, error_list):

            if move_files_to_output(data_source, table_name, path_dict,
                                    table_path, file_pattern, error_list):
                logger.info("File Validation and copy to staging for table {} is successful..".format(table_name))
                #Audit Logging
                metrics=ingestion_audit_metrics_dict()
                metrics.update({"data_source":data_source,"success_msg":"File Validation Successful"})
                audit_source = str(inp_dir_path+"/"+file_pattern).replace("//","/")
                log_dict={"run_id": str(job_id),"key":"'"+next_seq_id_str+"|"+str(feed_id)+"'","event_type": "'Ingestion'",
                  "event": "'FILE_VALIDATION'","source": "'"+file_path+"'",
                  "target": "'"+table_path+"'","source_type": "'File'","target_type": "'Table'",
                  "status": "'Success'","metrics": metrics}
                log_audit_entry(log_dict,db_connection_prop)


    # Check if the errors list is empty or not. If it's not empty, there are error logs written for this run.
    # Fail the job
    if len(error_list) != 0:
        logger.error("please check the error log.. There are incorrect file validations.. JOB FAILED...")

        # For each element in error_list, extract the attributes required to insert entry in file details table
        for row in error_list:
            filename = "'" + row["filename"] + "'"
            stage = "'" + row["stage"] + "'"
            # Insert an entry into batch file details table with the error stage for the file pattern
            insert_record_into_batch_file_detail(batch_seq_id=batch_seq_id,
                                                 file_name=filename,
                                                 file_processed_status="'Failed'",
                                                 failed_stage=stage,
                                                 prop_file_path=db_connection_prop)

            #Audit Logging
            metrics=ingestion_audit_metrics_dict()
            metrics.update({"data_source":data_source,"error_msg":row["stage"],"error_type":"FILE VALIDATION ERROR"})
            if row["stage"]=="Record Count MisMatch":
                err_msg = row["error_desc"]
                if err_msg.find('>>AUDIT')!=-1:
                    err_cnt=err_msg[err_msg.find('>>AUDIT'):len(err_msg)]
                    err_cnt_lst = err_cnt.split(',')
                    metrics.update({"ctrl_file_count":err_cnt_lst[1],"dat_file_count":err_cnt_lst[2]})

            log_dict={"run_id": str(job_id),"key":"'"+next_seq_id_str+"|"+str(feed_id)+"'","event_type": "'Ingestion'",
                  "event": "'FILE_VALIDATION'","source": filename,
                  "target": "'NA'","source_type": "'File'","target_type": "'NA'",
                  "status": "'Failed'","metrics": metrics}
            log_audit_entry(log_dict,db_connection_prop)

        # Update batch seq detail with status as Failed for this run
        error_rollback(processed_file_list)
        sys.exit(1)
    time.sleep(300)
    end_dags()

    # Airflow BashOperator by default takes the last line written to stdout into the XCom variable
    # once the bash command completes. Hence printing the next sequence which will be used in the CDC script
    # as an argument via the xcom_pull method
    # This print should be the last statement of the code..


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s: [%(levelname)s]: %(name)s: %(message)s")
    logging.getLogger("py4j").setLevel(logging.ERROR)
    main()
