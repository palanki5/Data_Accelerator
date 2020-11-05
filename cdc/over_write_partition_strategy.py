import logging
from ingestion_fw.cdc.default_merge_strategy import DefaultMergeStrategy


class OverWritePartitionStrategy(DefaultMergeStrategy):
    """
    Class AppendStrategy encapsulates the logic for appending staged records to the Partitioned Active Table. It also
    determines the records that are to be inserted into the Historical table based on the condition provided in JSON.
    """
    def __init__(self, table_conf, hive_context):
        self.table_conf = table_conf
        self.hive_context = hive_context
        self.logger = logging.getLogger(__name__)

    def get_records_for_history(self, staged_df):
        # Return Staged df for history data load
        return staged_df

    def get_records_for_active(self, staged_df, active_df, cdc_params):
        # Appending all the staged records to the active partitioned table
        return staged_df

    def write_into_active_table(self, sqlcontext, new_active_df, active_table):
        """
        Appending staged data into Hive Active Partitioned table
        :param new_active_df: dataframe having the staged records
        :param active_table: active table name provided in the conf file
        :return: None
        """
        partition_col = self.table_conf['cdc_config']['partition_col']

        sqlcontext.sql("set hive.exec.dynamic.partition=true")
        sqlcontext.sql("set hive.exec.dynamic.partition.mode=nonstrict")

        new_active_df.write.option("compression", "snappy").format("hive"). \
            partitionBy(partition_col).mode("overwrite").saveAsTable(active_table)

        msck_cmd = "MSCK REPAIR TABLE " + active_table
        sqlcontext.sql(msck_cmd)
