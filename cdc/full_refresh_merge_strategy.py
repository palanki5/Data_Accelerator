import logging
from ingestion_fw.cdc.default_merge_strategy import DefaultMergeStrategy


class FullRefreshMergeStrategy(DefaultMergeStrategy):
    """
    Class DefaultMergeStrategy encapsulates the logic for merging staged records with the Active Table. It also
    determines the records that are to be inserted into the Historical table.
    """
    def __init__(self, table_conf, hive_context):
        self.table_conf = table_conf
        self.hive_context = hive_context
        self.logger = logging.getLogger(__name__)

    def get_records_for_history(self, staged_df):
        """
        Return the records that need to be appended to the Historical table
        :param staged_df: the dataframe containing the staged records
        :return: dataframe of records that are to be appended to the Historical table
        """
        # In case of full refresh, all staged records should be inserted into the Historical table
        return staged_df

    def get_records_for_active(self, staged_df, cur_active_df, cdc_params):
        """
        Return all the records that should be in the Active table.
        In case of full refresh, all the staged records must be overwritten in the Active table
        :param staged_df: the dataframe containing the staged records
        :param cur_active_df: the dataframe containing the records that are in the existing Active table
        :return: the dataframe containing all the records that are to be part of the Active table
        """
        return staged_df

    def write_into_history_table(self, staged_df, history_table):
        """
        Write One time data to the hive history table
        :param staged_df: Staged dataframe which has data for that run
        :param history_table: historical table name (dbname.tablename)
        :return: None
        """
        if len(history_table) != 0:
            # staged_df.write.format("parquet").mode('append').saveAsTable(history_table)
            staged_df.write.option("compression", "snappy").format("parquet").mode('append').insertInto(history_table)
            self.logger.info("Writing into History Completed to the specified Table :  " + history_table)
        else:
            self.logger.info("Skipping write into History table as the Pattern specified is Full_Refresh and "
                             "History_table details are not specified")
