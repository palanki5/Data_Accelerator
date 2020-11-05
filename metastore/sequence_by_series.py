import calendar
import datetime
import logging
from dbmanager import DbManager

class SequenceBySeries:
    """
    Class SequenceByBillCycle encapsulates the logic to extract maximum sequence number from the
    Control table "batch_seq_detail" for the feed id provided as input
    """
    def __init__(self, db_prop_file):
        self.table_name = "batch_seq_detail"
        self.db_conn_prop = db_prop_file
        self.logger = logging.getLogger(__name__)

    def get_next_batch_seq_number(self, feed_id, initial_seq_id, seq_pattern, series):
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
            get_max_sequence_query = "SELECT BATCH_SEQ_ID AS MAX_BATCH_SEQ_ID,SEQ_ID AS MAX_SEQ_ID, STATUS" + \
                                     " FROM "+ table_name + \
                                     " WHERE (BATCH_SEQ_ID, FEED_ID) IN " + \
                                     "(SELECT MAX(BATCH_SEQ_ID), FEED_ID" + \
                                     " FROM " +table_name + \
                                     " WHERE FEED_ID = "+str(feed_id) + \
                                     " GROUP BY FEED_ID) AND FEED_ID = " + str(feed_id) + \
                                     " GROUP BY MAX_BATCH_SEQ_ID, MAX_SEQ_ID, STATUS"

            result = db.execute_query(get_max_sequence_query)
            db.close_connection()

            logger.info("BillCycle Get Sequence Query Executed, Contraol table query result for batch seq id is: {} successful..".format(result))

            #for row in result:
                 #logger.debug("Control Table Row Values: Each Row Value from control table..... Row0 values: "+str(row[0])+" ,Row1 values: "+str(row[1]) +" ,Row2 values: "+str(row[2]))
                 #print("Row0 values: "+str(row[0])+" ,Row1 values: "+str(row[1]) +" ,Row2 values "+str(row[2]))

            if len(result) == 0:
                logger.debug("Control table previous Sequence Number is null, Considered initial_seq_id for this run.")
                billcycle_seq_num = str(initial_seq_id)
                next_billcycle_seq_num = billcycle_seq_num
                logger.debug("next bill cycle sequece value is...:".format(next_billcycle_seq_num))

            else:
                for row in result:
                    logger.info("Control table having previous Sequence Number...")
                    #billcycle_seq_num from control table row[1] value
                    billcycle_seq_num = str(row[1])
                    bc_value = billcycle_seq_num
                    #print(bc_value)
                    status = row[2].lower()
                    if status == "success":
                        if bc_value == series[-1]:
                            element = series[0]
                            next_billcycle_seq_num = element
                            logger.debug("Control table next bill cycle sequece value is...:".format(next_billcycle_seq_num))
                        else:
                            n_idx_as_list = [idx + 1 for idx, item in enumerate(series) if bc_value == item]
                            n_idx_int = int(n_idx_as_list[0] if n_idx_as_list else -1)
                            if n_idx_int == -1:
                                raise Exception("Invalid Bill Cycle Value Provided, Re-validate Source Bill Cycle Number")
                            element = series[n_idx_int]
                            next_billcycle_seq_num = element
                            logger.debug("Control Table next bill cycle sequece value is.....:".format(next_billcycle_seq_num))
                    elif status == "failed":
                        next_billcycle_seq_num = bc_value
                        logger.debug("Control table previous execution failed, considered previous sequence number :".format(next_billcycle_seq_num))
            return next_billcycle_seq_num

        except Exception as ex:
            self.logger.error("Get Next Batch Seq Number failed with error : {0}".format(ex))
