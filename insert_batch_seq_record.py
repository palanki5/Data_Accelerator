import sys
import argparse
import json
import logging

from utils.utils import insert_record_batch_seq_detail
from metastore.sequence_pattern import get_next_seq_id


def parse_cmd_args():
    """
    add the command line arguments that are to be parsed.
    :return: the parsed arguments
    """
    cmd_parser = argparse.ArgumentParser(description="Ingestion Framework JDBC tables")
    cmd_parser.add_argument("etl_config_file_path", help="path to the ETL configuration file for this job")
    cmd_parser.add_argument("db_connection", help="Postgres database connection property file")
    cmd_parser.add_argument("job_id", help="the job id of the run that is currently being processed")
    cmd_args = cmd_parser.parse_args()
    return cmd_args


def main():
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s: [%(levelname)s]: %(name)s: %(message)s")

    # Parse command line arguments.. Input to the script is the full path to the ETL configuration file
    args = parse_cmd_args()

    # Read the ETL JSON property file
    jfile = json.loads(open(args.etl_config_file_path).read())

    # postgres database connection property file
    db_connection_prop = args.db_connection

    # Read the job id from the parameter
    job_id = "'" + args.job_id + "'"

    # Populate the properties from the base file
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
    #
    # The interval will be used to calculate how many days should be incremented to look for next sequence in file
    seq_interval = jfile["ingestion_config"].setdefault("seq_interval", "daily").lower() if seq_type == "date" else None

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
    logger.info("RDBMS Import:Sequence ID Value For This Execution Run..... {0}".format(next_seq_id_str))

    # Insert a record into batch_seq_detail table with status as "In-Progress" for this run
    insert_record_batch_seq_detail(feed_id,
                                   next_seq_id_str,
                                   job_id,
                                   status="'In-Progress'",
                                   prop_file_path=db_connection_prop)

    # Airflow BashOperator by default takes the last line written to stdout into the XCom variable
    # once the bash command completes. Hence printing the next sequence which will be used in the CDC script
    # as an argument via the xcom_pull method
    # This print should be the last statement of the code..
    print(next_seq_id_str)


if __name__ == "__main__":
    main()
