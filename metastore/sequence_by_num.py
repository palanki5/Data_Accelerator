import logging
from dbmanager import DbManager


class SequenceByNumber:
    """
    Class SequenceByNumber encapsulates the logic to extract maximum sequence number from the
    Control table "batch_seq_detail" for the feed id provided as input
    """
    def __init__(self, db_prop_file):
        self.table_name = "batch_seq_detail"
        self.db_conn_prop = db_prop_file
        self.logger = logging.getLogger(__name__)

    def get_next_batch_seq_number(self, feed_id, initial_seq_id):
        """
        Get the next batch seq number from the batch_seq_details table
        :param feed_id: feed_id is passed from the json configuration file
        :param initial_seq_id: Initial sequence id received in json.. This will be used if it's first time load
        :return: Get the result of max(seq_id), status from the batch_seq_detail table
                 1. if the previous run status is SUCCESS, then add 1 to the previous seq number and return
                 2. if the previous run status is FAILED,  then return the previous seq number to rerun the job
                 3. if the result is empty, then it is the 1st run.. return Initial sequence id received from json
        """
        table_name = self.table_name
        db = DbManager(self.db_conn_prop)
        logger = logging.getLogger(__name__)
        try:
            """
            Execute the below SQL in the batch_seq_detail table
            e.g.: SELECT CAST(SEQ_ID AS INTEGER) AS MAX_SEQ_ID, STATUS
                    FROM batch_seq_detail
                   WHERE (CAST(SEQ_ID AS INTEGER), FEED_ID) IN (SELECT MAX(CAST(SEQ_ID AS INTEGER)), FEED_ID
                                          FROM batch_seq_detail
                                        WHERE FEED_ID = 101
                                        GROUP BY FEED_ID)
                     AND FEED_ID = 101
                   GROUP BY MAX_SEQ_ID, STATUS
            """
            get_max_sequence_query = "SELECT CAST(SEQ_ID AS INTEGER) AS MAX_SEQ_ID, STATUS" + \
                                     " FROM " + table_name + \
                                     " WHERE (CAST(BATCH_SEQ_ID AS INTEGER), FEED_ID) IN" \
                                     " (SELECT  MAX(CAST(BATCH_SEQ_ID AS INTEGER)), FEED_ID" + \
                                     " FROM " + table_name + \
                                     " WHERE FEED_ID = " + str(feed_id) + \
                                     " GROUP BY FEED_ID) AND FEED_ID = " + str(feed_id) + \
                                     " GROUP BY MAX_SEQ_ID, STATUS"
            result = db.execute_query(get_max_sequence_query)
            db.close_connection()

            if len(result) == 0:
                return initial_seq_id
            else:
                for row in result:
                    max_seq_id = row[0]
                    status = row[1].lower()
                    if status == "success":
                        max_seq_id += 1
                    elif status == "failed":
                        max_seq_id = max_seq_id
                        logger.debug("Control table previous execution failed, considered previous sequence number :".format(max_seq_id))
                    elif status == "in-progress":
                        max_seq_id = None
            return max_seq_id
        except Exception as ex:
            self.logger.error("Get Next Batch Seq Number failed with error : {0}".format(ex))
