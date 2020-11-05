import logging
import psycopg2 as postgres
from ingestion_fw.utils.config import config


class DbManager:
    """
    Class DbManager encapsulates the logic to establish a database connection with postgresql database host
    and will be able to run insert, update, create, delete, drop, select, truncate queries against tables.
    """
    def __init__(self, db_conn_file):
        """
        Initializes the connection details
        :param self: self object with params and properties
        :return: None
        """
        self.logger = logging.getLogger(__name__)
        try:
            self.params = config(filename=db_conn_file, section='postgresql')
            self.connection = postgres.connect(host=self.params["host"],
                                               database=self.params["database"],
                                               user=self.params["user"],
                                               password=self.params["password"])
            self.connection.autocommit = True
            self.cursor = self.connection.cursor()
            self.cursor.execute("SET search_path TO " + self.params["schema"])
        except (Exception, postgres.DatabaseError) as error:
            raise Exception("Cannot connect to database due to error : {0}".format(error))

    def execute_query(self, sql_query):
        """
        Execute sql query
        :param sql_query: SQL Query to be executed
        :return: set of tuple retured from the SQL execution
        """
        try:
            sql_command = sql_query
            self.cursor.execute(sql_command)
            self.logger.debug("Executing SQL : " + sql_command)
            result = self.cursor.fetchall()
            return result
        except Exception as ex:
            raise Exception("Query Execution failed with error : {0}".format(ex))

    def select_columns(self, table_name, col_list):
        """
        Run SQL Query to select some columns from the table
        :param table_name: Table name to be used in the SELECT clause
        :param col_list: list of columns to be selected in the SELECT clause
        :return: return the tuple with the result returned from SELECT clause
        """
        try:
            columns_in_query = ''
            for col in col_list:
                # Check if the item is last column in the list..
                # if it is last element, don't append the comma and space at last
                if col == col_list[-1]:
                    columns_in_query = columns_in_query + col
                else:
                    columns_in_query = columns_in_query + col + ", "

            select_command = "select " + columns_in_query + " from " + table_name
            self.cursor.execute(select_command)
            self.logger.debug("Executing SQL : " + select_command)
            result = self.cursor.fetchall()
            return result
        except Exception as ex:
            raise Exception("Column Selection Query Failed with error : {0}".format(ex))

    def create_table(self, table_name, column_dict):
        """
        Execute create table query on the database
        :param table_name: Table name to be created in the database
        :param column_dict: List of dictionaries with information on
                            Column Name and data type with additional information like PRIMARY KEY etc.
                             e.g. [{"feed_id": "INT PRIMARY KEY"}, {"batch_value": "VARCHAR(100)"}]
        :return: None.. Executes the create table SQL query on database
        """
        try:
            column_ddl = ''
            for item in column_dict:
                for key in item:
                    # Check if the item is last column in the dictionary list..
                    # if it is last element, don't append the commad and space at last
                    if item == column_dict[-1]:
                        column_ddl = column_ddl + key + ' ' + item[key]
                    else:
                        column_ddl = column_ddl + key + ' ' + item[key] + ', '

            create_table_command = "CREATE TABLE " + table_name + "(" + column_ddl + ")"
            self.logger.debug("Executing SQL : " + create_table_command)
            self.cursor.execute(create_table_command)
        except Exception as ex:
            raise Exception("Create table failed with error : {0}".format(ex))

    def insert_record(self, table_name, columns, values):
        """
        Executes Insert into table SQL Query on the table
        :param table_name: Table name where record needs to be inserted
        :param columns: List of columns where a value has to be entered
                         e.g. columns = ["feed_id", "batch_value"]
        :param values: List of values in the same seqeuence as columns
                         e.g. values = [1001, "'test 2 batch'"]
        :return: None.. Executes the Insert SQL query on the table
        """
        try:
            insert_col_str = ''
            insert_val_str = ''
            for col in columns:
                if col == columns[-1]:
                    insert_col_str = insert_col_str + col
                else:
                    insert_col_str = insert_col_str + col + ', '

            for val in values:
                if val == values[-1]:
                    insert_val_str = insert_val_str + str(val)
                else:
                    insert_val_str = insert_val_str + str(val) + ', '

            """
            Insert Query : e.g. INSERT INTO test(feed_id, batch_value) VALUES (1001, 'test 2 batch')
            """
            insert_command = "INSERT INTO " + table_name + " (" + insert_col_str + \
                             ") VALUES " + "(" + insert_val_str + ")"
            self.logger.debug("Executing SQL : " + insert_command)
            self.cursor.execute(insert_command)
        except Exception as ex:
            raise Exception("Insert Query Failed with error : {0}".format(ex))

    def update_record(self, table_name, update_column, where_clause):
        """
        Executes Update Record SQL Query on the table
        :param table_name: Table Name to run update query against
        :param update_column: Dictionary having list of columns and respective values to be updated
        :param where_clause: WHERE clause to identify the record which needs to be updated
        :return: None.. Executes the Update table SQL query on the table
        """
        try:
            """
             Update query : e.g. UPDATE test SET STATUS = 'failed',
                                                 JOB_ID = 'TEST_JOB_ID AXIS 2'
                                  WHERE FEED_ID = 1001
            """
            set_value_ddl = ' SET '
            for item in update_column:
                for key in item:
                    # Check if the item is last column in the dictionary..
                    # If it is the last element, don't append comma and space at last
                    if item == update_column[-1]:
                        set_value_ddl = set_value_ddl + key + ' = ' + item[key]
                    else:
                        set_value_ddl = set_value_ddl + key + ' = ' + item[key] + ', '

            update_query = "UPDATE " + table_name + set_value_ddl + " WHERE " + where_clause
            self.logger.debug("Executing SQL : " + update_query)
            self.cursor.execute(update_query)
        except Exception as ex:
            raise Exception("Update Query Failed with error : {0}".format(ex))

    def truncate_table(self, table_name):
        """
        Truncates the table passed
        :param table_name: Table Name which needs to be truncated
        :return: None.. Executes the Truncate table SQL query on the table
        """
        try:
            truncate_table_query = "TRUNCATE TABLE " + table_name
            self.logger.debug("Executing SQL : " + truncate_table_query)
            self.cursor.execute(truncate_table_query)
        except Exception as ex:
            raise Exception("Truncate Table failed with error : {0}".format(ex))

    def drop_table(self, table_name):
        """
        Drops the table passed
        :param table_name: Table Name which needs to be dropped
        :return: None.. Executes the Drop table SQL query on the table
        """
        try:
            drop_table_query = "DROP TABLE " + table_name
            self.logger.debug("Executing SQL : " + drop_table_query)
            self.cursor.execute(drop_table_query)
        except Exception as ex:
            raise Exception("Drop Table failed with error : {0}".format(ex))

    def close_connection(self):
        """
        Close the connection to db
        :return: None..
        """
        self.connection.close()
