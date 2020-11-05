import logging


class DefaultMergeStrategy:
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
        # All staged records should be inserted into the Historical table
        return staged_df

    def get_records_for_active(self, staged_df, cur_active_df, cdc_params):
        """
        Return all the records that should be in the Active table.
        The records returned: New records + updated records + records to be retained as is in the Active table
        :param staged_df: the dataframe containing the staged records
        :param cur_active_df: the dataframe containing the records that are in the existing Active table
        :return: the dataframe containing all the records that are to be part of the Active table
        """
        key_cols = self.table_conf['cdc_config']['key_cols']
        update_ts_col = self.table_conf['cdc_config']['update_ts_col']
        custom_cdc_query = self.table_conf['cdc_config']['custom_cdc_query']

        # Obtain the most recent record for each key combination in the staged dataframe
        staged_recent_df = self._get_most_recent_records(staged_df, key_cols, update_ts_col)

        if custom_cdc_query:
            self.logger.debug("DefaultMergeStrategy.get_records_for_active():: custom_cdc_query specified")
            active_table_name = self.table_conf['cdc_config']['active_table']
            only_active_table_name = active_table_name.split(".")[1] if "." in active_table_name else active_table_name

            cur_active_df = cur_active_df.alias('active')
            staged_recent_df = staged_recent_df.alias('staged')

            staged_recent_df.registerTempTable(only_active_table_name + "_staged")
            cur_active_df.registerTempTable(only_active_table_name + "_active")

            # Get the list of non-key columns - useful for filtering records later
            # all_cols = staged_recent_df.columns
            # non_key_cols = [col for col in all_cols if col not in key_cols]

            # execute the custom cdc query
            self.logger.info("DefaultMergeStrategy.get_records_for_active():: custom_cdc_query : " + custom_cdc_query)
            outer_join_df = self.hive_context.sql(custom_cdc_query)

        else:  # custom_cdc_query is not specified
            # Get the list of non-key columns - useful for filtering records later
            self.logger.debug("DefaultMergeStrategy.get_records_for_active():: custom_cdc_query NOT specified")
            # all_cols = staged_recent_df.columns
            # non_key_cols = [col for col in all_cols if col not in key_cols]

            # Create aliases for the Dataframes to avoid ambiguity in column names.
            # The aliases would be used to select columns.
            active_df = cur_active_df.alias('active')
            staged_recent_df = staged_recent_df.alias('staged')

            # Create the join condition
            cond_list = [active_df[x] == staged_recent_df[x] for x in key_cols]

            # Outer join the active dataframe with the staged dataframe
            outer_join_df = active_df.join(staged_recent_df, cond_list, 'outer')

        from pyspark import StorageLevel
        outer_join_df.persist(StorageLevel.MEMORY_AND_DISK_SER)

        # Identify all records in the Active dataframe that are not present in the staged dataframe.
        # Select only the active dataframe columns.
        # These are the records that are retained as is in the Active table.
        key_col_is_null_list = [staged_recent_df[col].isNull() for col in key_cols]
        import functools
        key_cols_are_null_cond = functools.reduce(lambda cond1, cond2: cond1 & cond2, key_col_is_null_list)
        unmatched_rows_df = outer_join_df.filter(key_cols_are_null_cond).select('active.*')

        # Identify all records that are new or found a match in the staged dataframe.
        # Select only the staged dataframe columns.
        # These are the records that are "upserted" in the Active table.
        key_col_not_null_list = [staged_recent_df[col].isNotNull() for col in key_cols]
        # At least one key column of the staged dataframe has a non-null value
        key_cols_are_not_null_cond = functools.reduce(lambda cond1, cond2: cond1 | cond2, key_col_not_null_list)
        upsert_rows_df = outer_join_df.filter(key_cols_are_not_null_cond).select('staged.*')

        # Union the unmatched rows with the rows to be upserted
        new_active_df = unmatched_rows_df.unionAll(upsert_rows_df)
        # Eliminate records which are not active from new active data frame
        # cdc_pattern = cdc_params['cdc_pattern'].lower()
        delete_query = self.table_conf['cdc_config'].setdefault('delete_query', '')

        if delete_query:
            new_active_df = self._eliminate_delete_records(new_active_df)

        return new_active_df

    def _get_most_recent_records(self, df, key_cols, sort_col):
        """
        This function returns the most recent record for the given key combination. The sort_col is expected to contain
        Date or timestamp value - this column is sorted in the descending order and ranked to get the most recent
        record.
        :param df:  the input Spark dataframe
        :param key_cols:  the dataframe columns that form the key combination
        :param sort_col:  the datetime column in the dataframe on which the records can be sorted in descending order
        :return: Dataframe with only 1 record (most recent) per key combination.
        """
        from pyspark.sql import Window
        from pyspark.sql.functions import row_number, col
        self.logger.debug("DefaultMergeStrategy._get_most_recent_record():: " + "key_cols = [" + ",".join(key_cols) +
                          "] sort_col=" + sort_col + "")
        part_window = Window.partitionBy(key_cols).orderBy(col(sort_col).desc())
        df_with_rownum = df.withColumn("rownum", row_number().over(part_window))
        df_recent = df_with_rownum.filter(col("rownum") == 1).drop("rownum")
        return df_recent

    def _eliminate_delete_records(self, new_active_df):
        """
        This function returns only the records which should be available in active table
        after elimination of delete records.
        :param new_active_df:  The active dataframe which needs elimination of delete records
        :return: Dataframe after elimination of delete records.
        """
        # delete_filters = self.table_conf['cdc_config']['delete_filters']
        delete_query = self.table_conf['cdc_config']['delete_query']
        self.logger.debug("DefaultMergeStrategy.eliminate_delete_records():: delete_query specified")
        active_table_name = self.table_conf['cdc_config']['active_table']
        only_active_table_name = active_table_name.split(".")[1] if "." in active_table_name else active_table_name
        new_active_df.registerTempTable(only_active_table_name + "_active_temp")
        new_active_post_delete = self.hive_context.sql(delete_query)
        return new_active_post_delete

    def write_into_history_table(self, staged_df, history_table):
        """
        Write One time data to the hive history table
        :param staged_df: Staged dataframe which has data for that run
        :param history_table: historical table name (dbname.tablename)
        :return: None
        """
        # staged_df.write.format("parquet").mode('append').saveAsTable(history_table)
        staged_df.write.option("compression", "snappy").format("parquet").mode('append').insertInto(history_table)

    def write_into_active_table(self, sqlcontext, new_active_df, active_table):
        """
        Write CDC snapshot data to the Hive Active table
        :param new_active_df: dataframe having the delta transformed records
        :param active_table: active table name provided in the conf file
        :return: None
        """
        # Write into another temp table to avoid errors with self table over writing.
        active_db_name = active_table.split(".")[0] if "." in active_table else 'default'
        active_table_name = active_table.split(".")[1] if "." in active_table else active_table
        active_table_temp = active_db_name + "." + active_table_name + '_table_temp'
        sqlcontext.sql("DROP TABLE IF EXISTS " + active_table_temp)
        new_active_df.write.option("compression", "snappy").format("parquet").mode('overwrite').saveAsTable(
            active_table_temp)
        active_table_temp_df = sqlcontext.sql("select * from " + active_table_temp)
        # active_table_temp_df.write.format("parquet").mode('overwrite').saveAsTable(active_table)
        active_table_temp_df.write.option("compression", "snappy").format("parquet").insertInto(active_table,
                                                                                                overwrite=True)
        # Deleting temp table created
        # sqlcontext.dropTempTable(active_table_temp)
        sqlcontext.sql("DROP TABLE IF EXISTS " + active_table_temp)
