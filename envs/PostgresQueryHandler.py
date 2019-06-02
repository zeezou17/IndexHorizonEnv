import psycopg2
import sys
from environment.Utils import Utils
from environment.Constants import Constants
from pglast import Node, parse_sql


class PostgresQueryHandler:
    # attribute which will hold the postgres connection
    connection = None
    hypo_indexes_dict = dict()

    @staticmethod
    def __get_connection():
        # create connection only if it is not done before
        if PostgresQueryHandler.connection is None:
            # connect to postgres
            try:
                # read database config from config file
                database_config = Utils.read_config_data(Constants.CONFIG_DATABASE)
                # connect to postgres
                PostgresQueryHandler.connection = psycopg2.connect(database=database_config["dbname"],
                                                                   user=database_config["user"],
                                                                   password=database_config["password"],
                                                                   host=database_config["host"],
                                                                   port=database_config["port"])
                PostgresQueryHandler.connection.autocommit = True
            # capture connection exception
            except psycopg2.OperationalError as exception:
                print('Unable to connect to postgres \n Reason: {0}').format(str(exception))
                # exit code
                sys.exit(1)
            else:
                cursor = PostgresQueryHandler.connection.cursor()
                # Print PostgreSQL version
                cursor.execute("SELECT version();")
                record = cursor.fetchone()
                print("***You are connected to below Postgres database*** \n ", record, "\n")
        return PostgresQueryHandler.connection

    @staticmethod
    def execute_select_query(query):
        cursor = PostgresQueryHandler.__get_connection().cursor()
        cursor.execute(query)
        returned_rows = cursor.fetchall()
        cursor.close()
        return returned_rows

    @staticmethod
    def execute_select_query_and_get_row_count(query):
        cursor = PostgresQueryHandler.__get_connection().cursor()
        # trim whitespaces and add wrapper query to get count of rows i.e select count(*) from (<QUERY>) table1
        query_to_execute = ' '.join(query.strip().replace('\n', ' ').lower().split())
        query_to_execute = "Select count(*) from (" + query_to_execute + " ) table1"
        cursor.execute(query_to_execute)
        returned_count = cursor.fetchone()
        cursor.close()
        return returned_count

    @staticmethod
    def create_hypo_index(table_name, col_name):
        key = table_name + Constants.MULTI_KEY_CONCATENATION_STRING + col_name
        # create hypo index if it is not already present
        if key not in PostgresQueryHandler.hypo_indexes_dict:
            cursor = PostgresQueryHandler.__get_connection().cursor()
            # replace placeholders in the create hypo index query with table name and column name
            cursor.execute(Constants.QUERY_CREATE_HYPO_INDEX.format(table_name, col_name))
            returned_index_id = cursor.fetchone()
            cursor.close()
            PostgresQueryHandler.hypo_indexes_dict[key] = returned_index_id[0]

    @staticmethod
    def remove_hypo_index(table_name, col_name):
        key = table_name + Constants.MULTI_KEY_CONCATENATION_STRING + col_name
        # check whether index is already present
        if key in PostgresQueryHandler.hypo_indexes_dict:
            cursor = PostgresQueryHandler.__get_connection().cursor()
            # retrieve index id from dict and replace the place holder with index id
            cursor.execute(Constants.QUERY_REMOVE_HYPO_INDEX.format(PostgresQueryHandler.hypo_indexes_dict.get(key, 0)))
            cursor.close()
            PostgresQueryHandler.hypo_indexes_dict.pop(key, None)

    @staticmethod
    def remove_all_hypo_indexes():
        cursor = PostgresQueryHandler.__get_connection().cursor()
        cursor.execute(Constants.QUERY_REMOVE_ALL_HYPO_INDEXES)
        cursor.close()

    @staticmethod
    def get_where_clause_list_for_query(query: str):
        query_tree = Node(parse_sql(query))
        for tre in query_tree:
            for node in tre.stmt.whereClause:
                print(str(node))
