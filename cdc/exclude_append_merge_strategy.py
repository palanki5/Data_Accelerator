import logging
from ingestion_fw.cdc.default_merge_strategy import DefaultMergeStrategy


class ExcludeAndAppendMergeStrategy(DefaultMergeStrategy):
    """
    Class ExcludeAndAppendMergeStrategy encapsulates the logic for merging staged records with the Active Table. By
    excluding the previous bill cycle records.
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
        custom_staging_query = self.table_conf['cdc_config'].get("custom_staging_query", 'N')
        active_table_name = self.table_conf['cdc_config']['active_table']
        if custom_staging_query != 'N' :
            only_active_table_name = active_table_name.split(".")[1] if "." in active_table_name else active_table_name
            staged_df.registerTempTable(only_active_table_name + "_staged")
            modified_staged_df = self.hive_context.sql(custom_staging_query)
            staged_df = modified_staged_df
            return staged_df
        else:
            return staged_df


    def get_records_for_active(self, staged_df, cur_active_df, cdc_params):
        """
        Return all the records that should be in the Active table.
        In case of exclude and append, all the active records of the processing bill date
         must be overwritten by staged records in the Active table
        :param staged_df: the dataframe containing the staged records
        :param cur_active_df: the dataframe containing the records that are in the existing Active table
        :param cdc_params: Configuration values
        :return: the dataframe containing all the records that are to be part of the Active table
        """
        exclude_cdc_query = self.table_conf['cdc_config'].setdefault("custom_cdc_query", "")
        active_table_name = self.table_conf['cdc_config']['active_table']
        batch_id = cdc_params['batch_id']

        if exclude_cdc_query:
            only_active_table_name = active_table_name.split(".")[1] if "." in active_table_name else active_table_name
            cur_active_df = cur_active_df.alias('active')
            cur_active_df.registerTempTable(only_active_table_name + "_active")
            staged_df.registerTempTable(only_active_table_name + "_staged")

            exclude_cdc_query = exclude_cdc_query.replace('%batch_id', batch_id)
            filter_active_df = self.hive_context.sql(exclude_cdc_query)

            new_active_df = filter_active_df.unionAll(staged_df)
            return new_active_df
        else:
            raise Exception("No custom CDC query information provided ")
