"""
    Class ActiveViewStrategy 
    Encapsulates the logic for the Active View on the History Table. 
    It also determines the records that are to be inserted into the Historical table.
    
    CDC_Pattern     : daily_delta_av
        Mandatory Input : active_view [Name of the view created on the History Table]
        Optional Input  : partition_col [columns on which the history table may be partitioned]

    Functions - 
        get_records_for_history   : Returns records to append to History
        get_records_for_active : Does nothing. active_df will be None for this strategy
        write_into_history_table: Appends to history table (considers partions, if any)
"""

import logging
import sys
from ingestion_fw.cdc.default_merge_strategy import DefaultMergeStrategy

class ActiveViewStrategy(DefaultMergeStrategy):
    """
    Class ActiveViewStrategy encapsulates the logic for the Active View on the History Table. It also
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
       # All staged records should be inserted into the Historical table
       return staged_df
    
    def get_records_for_active(self, staged_df, active_df, cdc_params):
       """
       Return all the records that should be in the Active table.
       The records returned: Latest Records to be present in the Active table from the Active_View (View on History Table)
       :return: the dataframe containing all the records that are to be part of the Active table
       """
    
       return active_df
    
    def write_into_history_table(self,staged_df, history_table):
        """
        Write staged data to history table
        Adding support for writing into partitioned history table - Niloy
        :param staged_df: Staged dataframe which has data for that run
        :param history_table: historical table name (dbname.tablename)
        :return: None
        """
        partition_keys = self.table_conf['cdc_config'].setdefault('partition_col', [])
        #self.logger.info("number of partition_columns = {0} with keys {1}".format(len(partition_keys), partition_keys)) 
        if len(partition_keys) == 0:
            staged_df.write.option("compression", "snappy").format("parquet").mode('append').insertInto(history_table)
        else:
            self.hive_context.sql("set hive.exec.dynamic.partition=true")
            self.hive_context.sql("set hive.exec.dynamic.partition.mode=nonstrict")
            staged_df.write.option("compression", "snappy").format("parquet").\
                partitionBy(partition_keys).mode('append').insertInto(history_table)
    
    
    def write_into_active_table(self,sqlcontext, new_active_df, active_table):
        """
        Write CDC snapshot data to Active table
        :param new_active_df: dataframe having the delta transformed records
        :param active_table: active table name provided in the conf file
        :return: None
        """
        self.logger.info("In write into Active : No Processing ") 

