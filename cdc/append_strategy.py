import logging
from ingestion_fw.cdc.default_merge_strategy import DefaultMergeStrategy
from datetime import datetime
import sys
import os
import shutil
from dateutil.relativedelta import relativedelta
from ingestion_fw.utils.utils import delete_dir, create_dir
from ingestion_fw.metastore.dbmanager import DbManager

# Move every files/folder to trash folder which need to be deleted.
trash_dir = "/dbfs/mnt/ops/deleted-files/"
create_dir(trash_dir)

class AppendStrategy(DefaultMergeStrategy):
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
        partition_keys = self.table_conf['cdc_config'].setdefault('partition_col', '[]')
        sqlcontext.sql("set hive.exec.dynamic.partition=true")
        sqlcontext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
        if len(partition_keys) == 0:
            self.logger.error("Provide atleast one partition column")
            sys.exit(1)
        else:
            new_active_df.write.option("compression", "snappy").format("hive"). \
                partitionBy(partition_keys).mode('append').saveAsTable(active_table)

    def get_relative_date(self, archival_period):
        """
        This functions returns the relative past date based on the archival period input
        eg: if archival_period is 1y, this function will return the timestamp of the date which is exactly 1
            year ago from now
        :param archival_period: archival_period provided in json file (eg: 1y or 1m or 1d or 1h)
        :return: relative date
        """
        arch_d = None
        arch_val = int(archival_period[:-1])
        if archival_period[-1] in ["y", "Y"]:
            arch_d = datetime.now() - relativedelta(years=arch_val)
        elif archival_period[-1] in ["m", "M"]:
            arch_d = datetime.now() - relativedelta(months=arch_val)
        elif archival_period[-1] in ["d", "D"]:
            arch_d = datetime.now() - relativedelta(days=arch_val)
        elif archival_period[-1] in ["h", "H"]:
            arch_d = datetime.now() - relativedelta(hours=arch_val)
        else:
            self.logger.error("enter archival_period in 1y or 1m or 1d format")
        return arch_d

    def get_delta(self, last_run, frequency):
        delta_val = None
        import math
        a = int(frequency[:-1])

        if frequency[-1] == "y":
            next_run = last_run + relativedelta(years=a)
            delta_val = (datetime.today() - next_run).days
        elif frequency[-1] == "m":
            next_run = last_run + relativedelta(months=a)
            delta_val = (datetime.today() - next_run).days
        elif frequency[-1] == "d":
            next_run = last_run + relativedelta(days=a)
            delta_val = (datetime.today() - next_run).days
        elif frequency[-1] == "h":
            next_run = datetime.now() + relativedelta(hours=a)
            delta_val = math.floor((datetime.today() - next_run).days * 24 + (datetime.today() - next_run).seconds / 3600)
        else:
            self.logger.error("enter archival_period in 1y or 1m or 1d format")
        return delta_val

    def get_last_run_date(self, calc_last_run_date_dict):
        """
        Get the last run date for the append only pattern and then frequency will be added to calculate
        future/next run load date for the history table..
        :return: last run date to calculate the next load date for the history table
        """
        db_connection = calc_last_run_date_dict["db_connection"]
        start_date = calc_last_run_date_dict["start_date"]
        active_db_name = calc_last_run_date_dict["only_active_db_name"]
        history_db_name = calc_last_run_date_dict["only_history_db_name"]
        active_tbl_name = calc_last_run_date_dict["only_active_tbl_name"]
        history_tbl_name = calc_last_run_date_dict["only_history_tbl_name"]

        dbmanager = DbManager(db_connection)
        last_run_query = "SELECT max(date_trunc('second', dag_last_run_date::timestamp)) " \
                         "FROM append_dag_run_details" \
                         " WHERE active_db_name = '{0}' " \
                         " AND active_table = '{1}' " \
                         " AND history_db_name = '{2}' " \
                         " AND history_table = '{3}' " \
                         " GROUP BY active_db_name, active_table, history_db_name, history_table".\
            format(active_db_name, active_tbl_name, history_db_name, history_tbl_name)
        last_run = dbmanager.execute_query(last_run_query)
        last_run = last_run[0][0] if last_run else start_date
        dbmanager.close_connection()
        return last_run

    def insert_into_append_control_tbl(self, insert_audit_rec_dict):
        """
        Insert a record into the audit table with the required values
        :return: None
        """
        db_connection = insert_audit_rec_dict["db_connection"]
        dbmanager = DbManager(db_connection)
        dbmanager.insert_record(table_name=insert_audit_rec_dict["table_name"],
                                columns=insert_audit_rec_dict["columns"],
                                values=insert_audit_rec_dict["values"])
        dbmanager.close_connection()

    def write_into_history_table(self, staged_df, history_table):
        """
        This method moves the records from active table to history table based on one of the below conditions
        cond(1) Time based: It moves records from active to history based on the archival period provided in JSON file
                            Json should contain below value
                              archival_period: This should specify the months or years or days beyond which records
                              should be moved to history table. eg: 1y or 1m or 1d
                            For this to achieve active table should be partitioned on b2b_batch_id column
        cond(2) Condition based: It moves the records based on the condition provided in JSON file
                            Json should contain below value
                               archival_cond: Condition which decides what records should be moved to history table
                               eg: part_col > value.
                            For this to achieve active table should be partitioned on part_col
        cond(3) Ignore History table: It will completely ignore the history table. This can be achieved by simply not
                                        providing the history table name in JSON

        :param staged_df: contains staged records
        :param history_table: history table name provided in conf file
        :return: None
        """
        if len(history_table) != 0:
            append_config = self.table_conf['cdc_config'].setdefault('append_config', None)
            if append_config is None:
                self.logger.error("provide append_config dictionary which contains frequency, active_table_loc, "
                                  "start_date and either of archival_period or archive_condition")
                sys.exit(1)
            start_date = append_config.setdefault('start_date', None)
            frequency = append_config.setdefault('frequency', None).lower()
            act_table_loc = append_config.setdefault('active_table_loc', None)
            if None in (start_date, frequency, act_table_loc):
                self.logger.error("Provide start date, frequency, act_table_loc under append_config tag in JSON")
                sys.exit(1)
            archival_period = append_config.setdefault('archival_period', None)
            archive_condition = append_config.setdefault('archive_condition', None)

            # Start Date should be provided in %Y-%m-%d %H:%M:%S format
            try:
                start_date = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
            except Exception as ex:
                self.logger.error("Please provide start_date in append config in '%Y-%m-%d %H:%M:%S' format")
                self.logger.error(ex)
                sys.exit(1)

            # Generate the active and history database and table names
            act_tab = self.table_conf['cdc_config']['active_table']
            only_history_db_name = history_table.split(".")[0] if "." in history_table else history_table
            only_history_tbl_name = history_table.split(".")[1] if "." in history_table else history_table
            env_list = ["prd", "dev", "sit", "preprod", "ci"]
            env = [env_nm for env_nm in env_list if env_nm in only_history_db_name]
            active_table = act_tab.replace("<env>", env[0]) if "<env>" in act_tab else act_tab
            only_active_db_name = active_table.split(".")[0] if "." in active_table else active_table
            only_active_tbl_name = active_table.split(".")[1] if "." in active_table else active_table

            control_db_connection = append_config.setdefault("ingestion_fw_ini",
                                                             "/mnt/apps/b2b_data_ingestion_fw/config/ingestion_fw.ini")

            calc_last_run_date_dict = {"db_connection": control_db_connection,
                                       "start_date": start_date,
                                       "only_active_db_name": only_active_db_name,
                                       "only_history_db_name": only_history_db_name,
                                       "only_active_tbl_name": only_active_tbl_name,
                                       "only_history_tbl_name": only_history_tbl_name}

            last_run = self.get_last_run_date(calc_last_run_date_dict)
            run_delta = self.get_delta(last_run, frequency)

            if run_delta == 0:
                act_df = self.hive_context.table(active_table)

                if archival_period is not None:
                    relative_date = self.get_relative_date(archival_period)
                    history_df = act_df.where(act_df.b2b_insert_timestamp < relative_date)
                    records_count = history_df.count()

                    if records_count == 0:
                        self.logger.info("No records to write into history table")
                    else:
                        history_df.write.option("compression", "snappy").format("parquet").mode('append').\
                            insertInto(history_table)
                        df_part = history_df.select("b2b_batch_id").distinct()
                        list_of_part = [row.b2b_batch_id for row in df_part.collect()]
                        max_q = max(list_of_part)
                        alter_query = "ALTER TABLE " + active_table + " DROP PARTITION (b2b_batch_id <= " + max_q + ")"
                        self.hive_context.sql(alter_query)

                        for i in list_of_part:
                            hdfs_path = act_table_loc + "/b2b_batch_id=" + i
                            #delete_dir(hdfs_path)
                            trash_path = os.path.join(trash_dir,
                                                      datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S-%f'))
                            create_dir(trash_path)
                            shutil.move(hdfs_path, trash_path)

                        msck_cmd = "msck repair table " + active_table
                        self.hive_context.sql(msck_cmd)

                        insert_column = ["active_db_name",
                                         "active_table",
                                         "history_db_name",
                                         "history_table",
                                         "dag_start_date",
                                         "dag_last_run_date",
                                         "records_inserted_in_history_tbl"]
                        insert_values = ["'" + only_active_db_name + "'",
                                         "'" + only_active_tbl_name + "'",
                                         "'" + only_history_db_name + "'",
                                         "'" + only_history_tbl_name + "'",
                                         "'" + start_date.strftime("%Y-%m-%d %H:%M:%S") + "'",
                                         "'" + datetime.today().strftime("%Y-%m-%d %H:%M:%S") + "'",
                                         records_count]
                        insert_audit_rec_dict = {"db_connection": control_db_connection,
                                                 "table_name": "append_dag_run_details",
                                                 "columns": insert_column,
                                                 "values": insert_values}
                        self.insert_into_append_control_tbl(insert_audit_rec_dict)
                elif archive_condition is not None:
                    history_df = act_df.where(archive_condition)
                    records_count = history_df.count()

                    if records_count == 0:
                        self.logger.info("No records to write into history table")
                    else:
                        history_df.write.option("compression", "snappy").format("parquet").mode('append').\
                            insertInto(history_table)
                        active_db_name = active_table.split(".")[0] if "." in active_table else 'default'
                        only_active_table_name = active_table.split(".")[1] if "." in active_table else active_table
                        active_table_temp = active_db_name + "." + only_active_table_name + "_temp"
                        self.hive_context.sql("DROP TABLE IF EXISTS " + active_table_temp)
                        # history_df.registerTempTable(active_table_temp)
                        history_df.write.option("compression", "snappy").format("parquet").mode('overwrite').saveAsTable(
                            active_table_temp)
                        # custom_cdc_query should list the partition columns of the table
                        # eq: select distinct concat('part_col=',part_col) from dbname.active_tablename_temp;
                        partition_keys = self.table_conf['cdc_config']['partition_col']
                        tmp_list = []
                        for i in partition_keys:
                            tmp_list.append("concat('{0}=',".format(i) + i + ")")
                        part_list = ','.join(tmp_list)
                        list_of_part_q = "select distinct concat_ws('/',{0}) from {1}".format(part_list, active_table_temp)
                        df_part = self.hive_context.sql(list_of_part_q)
                        list_of_part = [row._c0 for row in df_part.collect()]
                        alter_query = "ALTER TABLE " + active_table + " DROP PARTITION (" + archive_condition + ")"
                        self.hive_context.sql(alter_query)

                        for i in list_of_part:
                            hdfs_path = act_table_loc + "/" + i
                            #delete_dir(hdfs_path)
                            trash_path = os.path.join(trash_dir,
                                                      datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S-%f'))
                            create_dir(trash_path)
                            shutil.move(hdfs_path, trash_path)

                        msck_cmd = "msck repair table " + active_table
                        self.hive_context.sql(msck_cmd)
                        self.hive_context.sql("DROP TABLE IF EXISTS " + active_table_temp)
                        # self.hive_context.sql(msck_cmd)

                        # Insert record into the postgres append_dag_run_details to keep track of the ingestion runs
                        insert_column = ["active_db_name",
                                         "active_table",
                                         "history_db_name",
                                         "history_table",
                                         "dag_start_date",
                                         "dag_last_run_date",
                                         "records_inserted_in_history_tbl"]
                        insert_values = ["'" + only_active_db_name + "'",
                                         "'" + only_active_tbl_name + "'",
                                         "'" + only_history_db_name + "'",
                                         "'" + only_history_tbl_name + "'",
                                         "'" + start_date.strftime("%Y-%m-%d %H:%M:%S") + "'",
                                         "'" + datetime.today().strftime("%Y-%m-%d %H:%M:%S") + "'",
                                         records_count]

                        insert_audit_rec_dict = {"db_connection": control_db_connection,
                                                 "table_name": "append_dag_run_details",
                                                 "columns": insert_column,
                                                 "values": insert_values}
                        self.insert_into_append_control_tbl(insert_audit_rec_dict)
            else:
                self.logger.info("Skipping history table for this run")
        else:
            self.logger.info("Ignoring the history table")
