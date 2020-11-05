from ingestion_fw.cdc.default_merge_strategy import DefaultMergeStrategy
from ingestion_fw.cdc.full_refresh_merge_strategy import FullRefreshMergeStrategy
from ingestion_fw.cdc.exclude_append_merge_strategy import ExcludeAndAppendMergeStrategy
from ingestion_fw.cdc.append_strategy import AppendStrategy
from ingestion_fw.cdc.active_view_strategy import ActiveViewStrategy 
from ingestion_fw.cdc.active_persistent_strategy import ActivePersistentStrategy
from ingestion_fw.cdc.over_write_partition_strategy import OverWritePartitionStrategy

def get_merge_strategy(cdc_pattern, table_conf, hive_context):
    """
    Returns the appropriate merge strategy to be applied for the given cdc pattern
    :param cdc_pattern: the cdc pattern string
    :return: an instance of DefaultMergeStrategy or its subclasses
    """
    cdc_pattern = cdc_pattern.lower()
    if cdc_pattern == "one_time_load" or cdc_pattern == "delta_load":
        return DefaultMergeStrategy(table_conf, hive_context)
    elif cdc_pattern == "delta_load_av":
        return ActiveViewStrategy(table_conf, hive_context)
    elif cdc_pattern == "delta_load_ap":
        return ActivePersistentStrategy(table_conf, hive_context)
    elif cdc_pattern == "bill_cycle":
        return DefaultMergeStrategy(table_conf, hive_context)
    elif cdc_pattern == "exclude_append":
        return ExcludeAndAppendMergeStrategy(table_conf, hive_context)		
    elif cdc_pattern == "full_refresh":
        return FullRefreshMergeStrategy(table_conf, hive_context)
    elif cdc_pattern == "append":
        return AppendStrategy(table_conf, hive_context)
    elif cdc_pattern == "over_write_partition":
        return OverWritePartitionStrategy(table_conf, hive_context)
    else:  # No valid cdc pattern specified
        raise ValueError("No valid cdc_pattern specified", cdc_pattern)
