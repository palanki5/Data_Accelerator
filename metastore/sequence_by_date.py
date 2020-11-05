import logging
import datetime
from dbmanager import DbManager
from dateutil.relativedelta import relativedelta
from dateutil import parser


class SequenceByDate:
    """
    Class SequenceByDate encapsulates the logic to extract maximum sequence number from the
    Control table "batch_seq_detail" for the feed id provided as input
    """
    def __init__(self, db_prop_file):
        self.table_name = "batch_seq_detail"
        self.db_conn_prop = db_prop_file
        self.logger = logging.getLogger(__name__)

    def __get_next_sequence_from_interval(self, max_seq_id, seq_interval):
        """
        Get the next sequence date from the sequence interval..
        If interval is Monthly, increment the sequence by 1 month
        If interval is Weekly,  increment the sequence by 1 week
        If interval is Daily,   increment the sequence by 1 day
        If interval is Monday-Friday, increment the sequence by 1 day for tuesday - friday
                                      increment the sequence by 3 days for monday
        Similarly other intervals like tuesday-thursday, saturday-sunday would work as well
        :param max_seq_id: last run success sequence number
        :param seq_interval: Sequence interval to calculate delta
        :return: Next sequence to be used
        """
        logger = logging.getLogger(__name__)
        try:
            if seq_interval == "monthly":
                next_seq_day = max_seq_id + relativedelta(months=1)
            elif seq_interval == "weekly":
                next_seq_day = max_seq_id + relativedelta(weeks=1)
            elif seq_interval == "daily":
                next_seq_day = max_seq_id + relativedelta(days=1)
            else:
                days_in_week = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
                first_run_day_idx = days_in_week.index(seq_interval.split("-")[0].strip())
                last_run_day_idx = days_in_week.index(seq_interval.split("-")[1].strip())

                if first_run_day_idx > last_run_day_idx:
                    # For reverse schedules like sun-fri, thu-tue etc.
                    run_schedule_days = days_in_week[first_run_day_idx:] + days_in_week[:last_run_day_idx + 1]
                else:
                    # For forward schedules like mon-fri, wed-sun etc.
                    run_schedule_days = days_in_week[first_run_day_idx:last_run_day_idx + 1]

                last_execution_day = parser.parse(str(max_seq_id)).strftime("%A").lower()

                if last_execution_day == run_schedule_days[-1]:
                    days_delta = 7 - (last_run_day_idx - first_run_day_idx)
                    next_seq_day = max_seq_id + relativedelta(days=days_delta)
                elif last_execution_day not in run_schedule_days:
                    logger.error("Seq Interval not provided properly according to run date")
                    raise ValueError("Seq Interval not provided properly according to run date")
                else:
                    next_seq_day = max_seq_id + relativedelta(days=1)
            return next_seq_day
        except Exception as ex:
            self.logger.error("Get Next Sequence for Date Interval failed with error : {0}".format(ex))

    def get_next_batch_seq_number(self, feed_id, initial_seq_id, seq_pattern, seq_interval):
        """
        Get the next batch seq number from the batch_seq_details table
        :param feed_id: feed_id is passed from the json configuration file
        :param initial_seq_id: Initial sequence id received in json.. This will be used if it's first time load
        :param seq_pattern: sequence pattern for the date as expected in file name
        :param seq_interval: Seq Interval to calculate delta days/weeks/months for date sequence
        :return: Get the result of max(seq_id), status from the batch_seq_detail table
                 1. if the previous run status is SUCCESS, then add 1 to the previous seq number and return
                 2. if the previous run status is FAILED,  then return the previous seq number to rerun the job
                 3. if the result is empty, then it is the 1st run.. return Initial sequence id received from json
                         is formatted with the seq pattern provided in json..
        """
        table_name = self.table_name
        db = DbManager(self.db_conn_prop)
        logger = logging.getLogger(__name__)
        try:
            """
            Execute the below SQL in the batch_seq_detail table
            e.g.: SELECT SEQ_ID AS MAX_SEQ_ID, STATUS
                    FROM batch_seq_detail
                   WHERE (CAST(BATCH_SEQ_ID AS INTEGER), FEED_ID) IN (SELECT MAX(BATCH_CAST(SEQ_ID AS INTEGER)), FEED_ID
                                          FROM batch_seq_detail
                                        WHERE FEED_ID = 101
                                        GROUP BY FEED_ID)
                     AND FEED_ID = 101
                   GROUP BY MAX_SEQ_ID, STATUS
            """

            get_max_sequence_query = "SELECT SEQ_ID AS MAX_SEQ_ID, STATUS" + \
                                     " FROM " + table_name + \
                                     " WHERE (CAST(BATCH_SEQ_ID AS INTEGER), FEED_ID) IN" \
                                     " (SELECT  MAX(CAST(BATCH_SEQ_ID AS INTEGER)), FEED_ID" + \
                                     " FROM " + table_name + \
                                     " WHERE FEED_ID = " + str(feed_id) + \
                                     " GROUP BY FEED_ID) AND FEED_ID = " + str(feed_id) + \
                                     " GROUP BY MAX_SEQ_ID, STATUS"
            result = db.execute_query(get_max_sequence_query)
            db.close_connection()
            next_seq_id = ""

            if len(result) == 0:
                initial_seq_id = initial_seq_id.zfill(8) if "%y" not in seq_pattern else initial_seq_id.zfill(6)
                next_seq_id = datetime.datetime.strptime(initial_seq_id, seq_pattern).strftime(seq_pattern)
            else:
                next_sequence = ""
                for row in result:
                    # Format the sequence to date format so that it can be incremented by +1 for next date
                    max_seq_str = str(row[0]).zfill(8) if "%y" not in seq_pattern else str(row[0]).zfill(6)
                    max_seq_id = datetime.datetime.strptime(max_seq_str, seq_pattern).date()
                    status = row[1].lower()
                    if status == "success":
                        # max_seq_id += datetime.timedelta(days=1)
                        next_sequence = self.__get_next_sequence_from_interval(max_seq_id, seq_interval)
                    elif status == "failed":
                        next_sequence = max_seq_id
                        logger.debug("Control table previous execution failed, considered previous sequence number :".
                                     format(max_seq_id))
                    elif status == "in-progress":
                        # return None
                        next_sequence = None
                    # Re-format the sequence to the format provided in json, so that it matches the pattern in filename
                    if next_sequence is not None:
                        next_seq_id = datetime.datetime.strptime(str(next_sequence), "%Y-%m-%d").strftime(seq_pattern)
                    else:
                        next_seq_id = None
            return next_seq_id
        except Exception as ex:
            self.logger.error("Get Next Batch Seq Number failed with error : {0}".format(ex))
