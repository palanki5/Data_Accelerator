import logging

from sequence_by_num import SequenceByNumber
from sequence_by_date import SequenceByDate
from sequence_by_series import SequenceBySeries
# from dbmanager import DbManager


def get_next_seq_id(seq_type, feed_id, initial_seq_id, seq_pattern, series, db_prop_file, seq_interval):
    """
    Check if the sequence is based on Integer or Date value..
    :param seq_type: Sequence Type passed from the json file..
    :param feed_id: Get the feed_id from the json configuration file
    :param initial_seq_id: Initial sequence id received in json.. This will be used if it's first time load
    :param seq_pattern: Sequence pattern provided in json if the sequence is of date format...
    :param db_prop_file: Database property file to establish connection
    :param seq_interval: Seq Interval to calculate delta days/weeks/months for date sequence
    :return: If sequence is based on integer, return SequenceByNumber Object and sequence type
             If sequence is based on date,    return SequenceByDate   Object and sequence type
    """
    logger = logging.getLogger(__name__)
    try:
        """ Now we will receive sequence type from json file and batch_detail table is deprecated..
            so the below part is commented
        db = DbManager(db_prop_file)
        # Get if the sequence is from a number or from date.. Accordingly Call the abstract class
        get_seq_type_query = "SELECT BATCH_SEQ_TYPE FROM " + table_name + " WHERE feed_id = " + str(feed_id)
        seq_type = db.execute_query(sql_query=get_seq_type_query)
        db.close_connection()
        for row in seq_type:
        """
        sequence_type = seq_type
        if sequence_type == "sequence":
            logger.debug("getting maximum sequence id (number type) for feed :: {0}".format(feed_id))
            next_seq_id = SequenceByNumber(db_prop_file).get_next_batch_seq_number(feed_id=feed_id,
                                                                                   initial_seq_id=initial_seq_id)
            logger.debug("Sequence ID to be used for this run is :: {0}".format(next_seq_id))
            return next_seq_id
        elif sequence_type == "date":
            logger.debug("getting maximum sequence id (date type) for feed :: {0}".format(feed_id))
            next_seq_id = SequenceByDate(db_prop_file).get_next_batch_seq_number(feed_id=feed_id,
                                                                                 initial_seq_id=initial_seq_id,
                                                                                 seq_pattern=seq_pattern,
                                                                                 seq_interval=seq_interval)
            logger.debug("Sequence ID to be used for this run is :: {0}".format(next_seq_id))
            return next_seq_id
        elif sequence_type == "series":
            next_seq_id = SequenceBySeries(db_prop_file).get_next_batch_seq_number(feed_id=feed_id,
                                                                                   initial_seq_id=initial_seq_id,
                                                                                   seq_pattern=seq_pattern,
                                                                                   series=series)
            logger.debug("Bill Cycle Sequence ID to be used for this run is :: {0}".format(next_seq_id))
            return next_seq_id
        elif sequence_type == "aria":
            # TODO : this has to be handled by application teams
            print("to do in future for aria")
        else:
            raise ValueError("Invalid seq type provided in batch detail table for feed id : "
                             "{0}".format(feed_id))
    except Exception as ex:
        logger.error("get_next_seq_id():: failed with error : {0}".format(ex))
