"""
    Class ActivePersistentStrategy 
	Encapsulates the logic for the Active View on the History Table. 
	It also determines the records that are to be inserted into the Historical table.
    	
	CDC_Pattern     : daily_delta_ap, daily_delta_av
    	Mandatory Input : active_view [Name of the view created on the History Table]
    	Optional Input  : custom_cdc_query [active_view will NOT be considered in this case]

    	Functions - 
    		get_records_for_history   : Returns record to Append to History
    		get_records_for_active : Return records for Active Table Creation 1. Based on Active View or 2. Based on Custom CDC Query
    		_eliminate_delete_records : Deletes records from the Active DF based on the Delete Query in the cfg file.
"""

import logging
import sys

class ActivePersistentStrategy:
    """
    Class ActivePersistentStrategy encapsulates the logic for the Active View on the History Table. It also
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
    
       active_view = self.table_conf['cdc_config']['active_view']
        
       query_str = "Select * from "+active_view
       new_active_df = self.hive_context.sql(query_str)
    
       stagedTableInfo = sorted(staged_df.columns)
       activeViewInfo = sorted(new_active_df.columns)
    
       if activeViewInfo != stagedTableInfo:
           self.logger.info("Columns which are Different btw stg n view. See below for Active_View  ::  "+active_view)
           self.logger.info(list(set(stagedTableInfo) - set(activeViewInfo)))
           self.logger.error("Inside get_records_for_active_ap():: Active View Name Column Mismatch. Cross Check the View Used << Columns mismatch listed above >> ")
           sys.exit(1)
    
       delete_query = self.table_conf['cdc_config'].setdefault('delete_query', '')
    
       if delete_query:
           new_active_df = self._eliminate_delete_records(new_active_df)
    
       return new_active_df
    
    def _eliminate_delete_records(self, new_active_df):
       """
       This function returns only the records which should be available in active table
       after elimination of delete records.
       :param new_active_df:  The active dataframe which needs elimination of delete records
       :return: Dataframe after elimination of delete records.
       """
       # delete_filters = self.table_conf['cdc_config']['delete_filters']
       delete_query = self.table_conf['cdc_config']['delete_query']
       self.logger.debug("ActivePersistentStrategy.eliminate_delete_records():: delete_query specified")
       active_table_name = self.table_conf['cdc_config']['active_table']
       only_active_table_name = active_table_name.split(".")[1] if "." in active_table_name else active_table_name
       new_active_df.registerTempTable(only_active_table_name + "_active_temp")
       new_active_post_delete = self.hive_context.sql(delete_query)
       return new_active_post_delete
    
    def write_into_history_table(self,staged_df, history_table):
        """
        Write One time data to the hive history table
        :param staged_df: Staged dataframe which has data for that run
        :param history_table: historical table name (dbname.tablename)
        :return: None
        """
        staged_df.write.option("compression", "snappy").format("parquet").mode('append').insertInto(history_table)
    
    
    def write_into_active_table(self,sqlcontext, new_active_df, active_table):
        """
        Write CDC snapshot data to the Hive Active table
        :param new_active_df: dataframe having the delta transformed records
        :param active_table: active table name provided in the conf file
        :return: None
        """
        self.logger.info("Inside Active Persistent Logic")
        new_active_df.write.option("compression", "snappy").format("parquet").insertInto(active_table,overwrite=True)
