import os
import time
import pysftp
import argparse
import json
import distutils.util
import logging
import sys
import re

from file_sensor import get_file_patterns, get_next_seq_id_for_batch
from utils.config import config

# credentials section properties
CRED_SECTION_SFTP = 'sfxsftp'
SFTP_KEY = 'sftp_key'
SFTP_HOSTNAME = 'sftp_hostname'
SFTP_USERNAME = 'sftp_username'
SFTP_PASSWORD = 'sftp_password'
CRED_SECTION_PG = 'postgresql'

# sftp config section properties
INGESTION_CONFIG = 'ingestion_config'
SFTP_CONFIG = 'sftp_config'
REMOTE_DIR = 'remote_dir'
LOCAL_DIR = 'local_dir'
DELETE_PROCESSED = 'delete_processed'
PROCESSED_DIR = 'processed_dir'

# string constants
PROCESSED_DIR_LIT = 'processed/'
FALSE_STR_LIT = 'False'
EOT_EXTN = '.eot'

# creating logging object
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s: [%(levelname)s]: %(name)s: %(message)s")


# Python user-defined exceptions for no remote files found
class Error(Exception):
    """Base class for other exceptions"""
    pass


class NoFileFound(Error):
    """Raised when remote directory has no files after 1 hour"""
    pass


class NoEOTFileFound(Error):
    """Raised when remote directory has no files after 1 hour"""
    pass


# print Start Dags message
def start_dags():
    logger.debug("DAG STARTED RUNNING")


# print end dags message
def end_dags():
    logger.debug("DAG HAS COMPLETED SUCCESSFULLY..")


def sync_dir(jfile, connection_file):
    """
    Sync to all the files of remote_dir.
    :param jfile: Source specific config details
    :param connection_file: Connection details file path
    :return: The number of files synced.
    """
    sftp_config = jfile.get(INGESTION_CONFIG, {}).get(SFTP_CONFIG, {})
    remote_dir = sftp_config.get(REMOTE_DIR)
    local_dir = sftp_config.get(LOCAL_DIR)
    # files_synced = 0
    sftp = _get_connection(connection_file)

    if sftp is None:
        sys.exit(1)
    else:
        try:
            logger.info('sftp sync of {} starting...'.format(remote_dir))
            files_synced = _sync_r(sftp, remote_dir, local_dir, sftp_config, jfile, connection_file)
            logger.info('synced {} file(s) from {}'.format(files_synced, remote_dir))
        except Exception as e:
            logger.error('sync of {} could not complete successfully, [Exception] : {}'.format(remote_dir, e))
            files_synced = 0
        finally:
            logger.info('closing the SFTP session for : {}'.format(remote_dir))
            sftp.close()
    return files_synced


def _get_connection(connection_file):
    """
     Get connection object with the details provided in CREDENTIALS_FILE
    :return: Returns pysftp.Connection object
    """
    credentials_sftp = config(connection_file, CRED_SECTION_SFTP)
    if credentials_sftp is None:
        logger.error('_get_connection():: SFTP not provided in ingestion framework config file..')
        return None
    else:
        try:
            # checking if the required credentials items are present in the configuration
            if SFTP_HOSTNAME not in credentials_sftp:
                logger.error('_get_connection():: HOST NAME is missing for SFTP connection..')
                return None
            if SFTP_USERNAME not in credentials_sftp:
                logger.error('_get_connection():: USER NAME is missing for SFTP connection..')
                return None
            if SFTP_KEY not in credentials_sftp:
                logger.error('_get_connection():: KEY PATH is missing for SFTP connection..')
                return None
            cnopts = pysftp.CnOpts()
            cnopts.hostkeys = None
            return pysftp.Connection(credentials_sftp[SFTP_HOSTNAME], username=credentials_sftp[SFTP_USERNAME],
                                     private_key=credentials_sftp[SFTP_KEY], cnopts=cnopts)
        except Exception as ex:
            # any errors during connection setup result in a non-usable connection
            logger.error('sftp connection could not be established!, [Exception] : {}'.format(ex))
            sys.exit(1)


def _sync_r(sftp, remote_dir, local_dir, sftp_config, jfile, connection_file):
    """
    Recursively sync the sftp contents starting at remote dir to the local dir and return the number of files synced
    :param sftp: Connection to the sftp server
    :param remote_dir: Remote dir to start sync from
    :param local_dir: To sync to
    :param sftp_config: SFTP source specific config details
    :param jfile: Ingestion config file path
    :param connection_file: Connection file path
    :return:
    """
    files_synced = 0
    # get the next_batch_seq_id for decide which files needs to be pulled
    next_batch_seq_id = get_next_seq_id_for_batch(jfile, connection_file)
    # get the file patters to be pulled in this run based on next_batch_seq_id
    all_tables_file_patterns = get_file_patterns(jfile, next_batch_seq_id)
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)
    # checks if all the files received at remote directory path
    check_all_files_received(sftp, remote_dir, all_tables_file_patterns)
    for item in sftp.listdir(remote_dir):
        remote_dir_item = os.path.join(remote_dir, item)
        local_dir_item = os.path.join(local_dir, item)
        if sftp.isdir(remote_dir_item):
            # skip processed directory which is present inside remote directory
            continue
        if sftp.isfile(remote_dir_item) and is_file_pattern_matches(all_tables_file_patterns, remote_dir_item):
            if _should_sync_file(sftp, remote_dir_item, local_dir_item):
                logger.info('sync {} => {}'.format(remote_dir_item, local_dir_item))
                sftp.get(remote_dir_item, local_dir_item, preserve_mtime=True)
                file_cleanup_processed(sftp, remote_dir_item, local_dir_item, sftp_config, item)
                files_synced += 1
            else:
                files_synced += _sync_r(sftp, remote_dir_item, local_dir_item, sftp_config, jfile,
                                        connection_file)
    return files_synced


def _should_sync_file(sftp, remote_file_path, local_file_path):
    """
    If the remote_file should be synced - if it was not downloaded or it is out of sync with the remote version
    :param sftp: Connection to the sftp server
    :param remote_file_path: Remote file path
    :param local_file_path: Local file path.
    :return: Returns a boolean whether we the file should be synced
    """
    if not os.path.exists(local_file_path):
        return True
    else:
        remote_attr = sftp.lstat(remote_file_path)
        local_stat = os.stat(local_file_path)
        return _is_size_different(remote_attr, local_stat) or _is_mtime_different(remote_attr, local_stat)


def _is_size_different(remote_attr, local_stat):
    """
    Checks if a remote file is eligible for sync
    :param remote_attr: Remote file attributes
    :param local_stat: Local file attributes
    :return: If both files have different size
    """
    return remote_attr.st_size != local_stat.st_size


def _is_mtime_different(remote_attr, local_stat):
    """
    Checks if a remote file is eligible for sync
    :param remote_attr: Remote file attributes
    :param local_stat: Local file attributes
    :return: if both files have different modified timestamp
    """
    return remote_attr.st_mtime != local_stat.st_mtime


def _is_file_transfer_successful(sftp, remote_file_path, local_file_path):
    """
    Check if file transfer is successful from remote server to landing server
    :param sftp: Connection to the sftp server
    :param remote_file_path: Remote file path
    :param local_file_path: Local file path.
    :return: Returns True is file transfer is successful
    """
    remote_attr = sftp.lstat(remote_file_path)
    local_stat = os.stat(local_file_path)
    return remote_attr.st_size == local_stat.st_size and remote_attr.st_mtime == local_stat.st_mtime


def create_dir_if_not_exist(sftp, processed_path):
    """
    Creates directory in the SFTP server if it doesn't exist
    :param sftp: Connection to the sftp server
    :param processed_path: Directory where the file will be moved after successful transfer
    :return: Creates the directory in SFTP server if doesn't exist
    """
    try:
        sftp.stat(processed_path)
    except Exception:
        logger.warning('processed directory does not exist. so, creating directory : {}'.format(processed_path))
        sftp.mkdir(processed_path, mode=700)


def move_file_processed(sftp, remote_dir_item, sftp_config, item, processed=PROCESSED_DIR_LIT):
    """
    Move successfully transferred file to processed directory
    :param sftp: Connection to the sftp server
    :param remote_dir_item: Remote file path
    :param sftp_config: SFTP source specific config details
    :param item: File item
    :param processed: Directory path where the successfully transferred file will be moved
    :return:
    """
    remote_dir = sftp_config.get(REMOTE_DIR)
    processed_path = os.path.join(remote_dir, processed)
    processed_item = os.path.join(processed_path, item)
    try:
        create_dir_if_not_exist(sftp, processed_path)
        logger.info(
            'file transfer successful , moving file : {} to processed directory: {}'.format(remote_dir_item,
                                                                                            processed_path))
        sftp.rename(remote_dir_item, processed_item)
    except IOError:
        logger.warning(
            'problem in cleaning up the file : {},looks like file already exists in {}'.format(remote_dir_item,
                                                                                               processed_path))
        sftp.remove(processed_item)
        logger.info('removing the file {} and moving the file {}'.format(processed_item, remote_dir_item))
        sftp.rename(remote_dir_item, processed_item)


def parse_cmd_args():
    """
    Add the command line arguments that are to be parsed.
    :return: The parsed arguments
    """
    parser = argparse.ArgumentParser(description='To pull files from SFX directory')
    parser.add_argument('etl_config_file_path', help='path to the ETL configuration file for this job')
    parser.add_argument('ingestion_fw_path', help='path to the SFTP & PostgreSQL connection file')
    args = parser.parse_args()
    return args


def file_cleanup_processed(sftp, remote_dir_item, local_dir_item, sftp_config, item):
    """
    Cleans up the processed files from remote_dir
    :param sftp: Connection to the sftp server
    :param remote_dir_item: Remote file path
    :param local_dir_item: Local file path
    :param sftp_config: SFTP source specific config details
    :param item: File item
    :return:
    """
    delete_processed = sftp_config.get(DELETE_PROCESSED, FALSE_STR_LIT)
    processed_dir = sftp_config.get(PROCESSED_DIR, PROCESSED_DIR_LIT)
    try:
        if bool(distutils.util.strtobool(delete_processed)) is True:
            if _is_file_transfer_successful(sftp, remote_dir_item, local_dir_item):
                logger.info(
                    'file transfer successful , deleting file from source directory: {}'.format(remote_dir_item))
                sftp.remove(remote_dir_item)
        else:
            if _is_file_transfer_successful(sftp, remote_dir_item, local_dir_item):
                move_file_processed(sftp, remote_dir_item, sftp_config, item, processed=processed_dir)
    except Exception as e:
        logger.error('oops..something went wrong while cleaning up the processed files, [Exception] : {}'.format(e))


def is_file_pattern_matches(all_tables_file_patterns, remote_dir_item):
    """
    Check if file needs to be pulled using configured patterns & batch seq.id
    :param all_tables_file_patterns: File patterns to be pulled
    :param remote_dir_item: Remote file item
    :return: Returns True if file qualified for pull else False
    """
    match_flag = False
    for pattern in all_tables_file_patterns:
        if re.search(pattern, remote_dir_item):
            logger.info('file : {} qualified for pull'.format(remote_dir_item))
            match_flag = True
            break
    return match_flag


def is_eot_file_exists(sftp, remote_dir_item):
    """
    Check if EOT file is available
    :param sftp: connection to the sftp server
    :param remote_dir_item: Remote file  path
    :return:
    """
    run_counter = 0
    while True:
        file_prefix = remote_dir_item.split('.')[0]
        eot_file = file_prefix + EOT_EXTN
        try:
            if run_counter < 10:
                run_counter += 1
                sftp.stat(eot_file)
                break
            else:
                logger.warning(
                    'no EOT file found for {}* after {} attempts, terminating file pull process'.format(file_prefix,
                                                                                                        run_counter))
                exit(1)
        except IOError:
            logger.warning('no EOT find found for : {}*, waiting for 5 min.'.format(file_prefix))
            # time.sleep(300)
            time.sleep(2)
    return True


def check_all_files_received(sftp, remote_dir, all_tables_file_patterns):
    """
    Checks if all the  files has been received at remote dir
    :param sftp: Connection to the sftp server
    :param remote_dir: Remote dir
    :param all_tables_file_patterns: File pattern needs to be pulled from remote directory
    :return:
    """
    for item in sftp.listdir(remote_dir):
        remote_dir_item = os.path.join(remote_dir, item)
        if is_file_pattern_matches(all_tables_file_patterns, remote_dir_item):
            is_eot_file_exists(sftp, remote_dir_item)


def main():
    """
    Calls the sync_dir function to start pulling the files from SFX to Landing server
    :return:
    """
    start_dags()
    # counter to track no. of attempts
    run_counter = 0
    # Parse command line arguments..
    # Input to the script is the full path to the SFTP configuration file & SFX server SFTP connection file
    args = parse_cmd_args()
    # Get connection file path
    connection_file = args.ingestion_fw_path
    # Get  ETL config file path
    config_file = args.etl_config_file_path
    # Read the ETL JSON property file
    jfile = json.loads(open(config_file).read())
    # Read the SFTP JSON properties
    remote_dir = jfile.get(INGESTION_CONFIG, {}).get(SFTP_CONFIG, {}).get(REMOTE_DIR)
    # Sync remote directory files
    while True:
        try:
            if sync_dir(jfile, connection_file) == 0:
                run_counter += 1
                logger.warning('No files found in the remote directory!, attempt : {}'.format(run_counter))
                raise NoFileFound('No files found in the remote directory!, attempt : {}'.format(run_counter))
            break
        except NoFileFound:
            if run_counter < 5:
                logger.warning(
                    'remote directory {} has no files, waiting for 15 min..'.format(remote_dir))
                # time.sleep(900)
                time.sleep(2)
            else:
                logger.error(
                    'No files found in remote directory : {}, even after 1 hour with {} attempts'.format(remote_dir,
                                                                                                         run_counter))
                sys.exit(1)
    end_dags()


if __name__ == "__main__":
    main()
