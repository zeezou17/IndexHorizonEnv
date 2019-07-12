import psycopg2
import sys
from gym.envs.postgres_idx_advisor.envs.Utils import Utils
from gym.envs.postgres_idx_advisor.envs.Constants import Constants
from pglast import Node, parse_sql
import re
import numpy as np

class PostgresQueryHandler:
    # Attributes which will hold the postgres connection and HypoPG indexes dict
    connection = None
    connectionDefault = None
    connectionAgent = None
    hypo_indexes_dict = dict()

    @staticmethod
    def __get_connection():
        """
        :returns: Creates connection to postgres which is configured with postgres_idx_advisor.so
        """
        # Create connection only if it is not done before
        if PostgresQueryHandler.connection is None:
            # Connect to postgres
            try:
                # Read database config from config file
                database_config = Utils.read_config_data(Constants.CONFIG_DATABASE)
                # connect to postgres
                PostgresQueryHandler.connection = psycopg2.connect(database=database_config["dbname"],
                                                                   user=database_config["user"],
                                                                   password=database_config["password"],
                                                                   host=database_config["host"],
                                                                   port=database_config["port"])
                PostgresQueryHandler.connection.autocommit = True
            # Capture connection exception
            except psycopg2.OperationalError as exception:
                print('Unable to connect to postgres \n Reason: {0}').format(str(exception))
                # exit code
                sys.exit(1)
            else:
                cursor = PostgresQueryHandler.connection.cursor()
                # Print PostgreSQL version
                cursor.execute("SELECT version();")
                record = cursor.fetchone()
                #print("***You are connected to below Postgres database*** \n ", record, "\n")
        return PostgresQueryHandler.connection

    @staticmethod
    def __get_default_connection():
        """
        :returns: Gets the default connection to Db without postgres_idx_advisor.so
                  This connection is needed to work with HypoPG
        """
        if PostgresQueryHandler.connectionDefault is None:
            # Connect to postgres
            try:
                # Read database config from config file
                database_config = Utils.read_config_data(Constants.CONFIG_DATABASE)
                # Connect to postgres
                PostgresQueryHandler.connectionDefault = psycopg2.connect(database=database_config["dbname"],
                                                                   user=database_config["user"],
                                                                   password=database_config["password"],
                                                                   host=database_config["host"],
                                                                   port=database_config["port"])
                PostgresQueryHandler.connectionDefault.autocommit = True
            # Capture connection exception
            except psycopg2.OperationalError as exception:
                print('Unable to connect to postgres \n Reason: {0}').format(str(exception))
                # Exit code
                sys.exit(1)
            else:
                cursor = PostgresQueryHandler.connectionDefault.cursor()
                # Print PostgreSQL version
                cursor.execute("SELECT version();")
                record = cursor.fetchone()
                # print("***You are connected to below Postgres database*** \n ", record, "\n")
        return PostgresQueryHandler.connectionDefault

    @staticmethod
    def execute_select_query(query: str, load_index_advisor: bool = False, get_explain_plan: bool = False):
        """
        :param query: contains the query that needs to be executed
        :param load_index_advisor: if true loads the index advisor plugin
        :param get_explain_plan: if true retrieves the explain plan for the query
        :returns: the no of rows returned by the query
        """
        if load_index_advisor:
            cursor = PostgresQueryHandler.__get_connection().cursor()
            cursor.execute(Constants.CREATE_EXTENSION)
            cursor.execute(Constants.LOAD_PG_IDX_ADVISOR)
            cursor = PostgresQueryHandler.__get_connection().cursor()
        else:
            cursor = PostgresQueryHandler.__get_default_connection().cursor()
            cursor.execute(Constants.DROP_PG_IDX_ADVISOR)
        if get_explain_plan:
            query = Constants.QUERY_EXPLAIN_PLAN.format(query)
        cursor.execute(query)
        returned_rows = cursor.fetchall()

        # Uncomment if you want to see the indexes currently set in HypoPG
        #PostgresQueryHandler.check_hypo_indexes()

        cursor.close()
        return returned_rows

    @staticmethod
    def get_table_row_count(table_name):
        """

        :param table_name: name of the table
        :return: number of rows in the table
        """
        cursor = PostgresQueryHandler.__get_connection().cursor()
        cursor.execute(Constants.QUERY_FIND_NUMBER_OF_ROWS.format(table_name))
        returned_count = cursor.fetchone()
        cursor.close()
        return returned_count[0]

    @staticmethod
    def execute_count_query(query):
        """

        :param query: query string
        :return: number of rows returned by the query
        """
        cursor = PostgresQueryHandler.__get_connection().cursor()
        cursor.execute(query)
        returned_count = cursor.fetchone()
        cursor.close()
        return returned_count[0]

    @staticmethod
    def execute_select_query_and_get_row_count(query):
        """

        :param query: query string
        :return: number of rows returned by the query
        """
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
        """
            Creates Hypo Index using the input
        """
        key = table_name + Constants.MULTI_KEY_CONCATENATION_STRING + col_name
        # create hypo index if it is not already present
        """if key not in PostgresQueryHandler.hypo_indexes_dict:
            cursor = PostgresQueryHandler.__get_default_connection().cursor()
            #print('setting index', table_name, col_name)
            # replace placeholders in the create hypo index query with table name and column name
            cursor.execute(Constants.QUERY_CREATE_HYPO_INDEX.format(table_name, col_name))
            returned_index_id = cursor.fetchone()
            print('fetchall', cursor.fetchall(), returned_index_id )
            cursor.close()
            PostgresQueryHandler.hypo_indexes_dict[key] = returned_index_id[0]"""
        cursor = PostgresQueryHandler.__get_default_connection().cursor()
        # print('setting index', table_name, col_name)
        # replace placeholders in the create hypo index query with table name and column name
        cursor.execute(Constants.QUERY_CREATE_HYPO_INDEX.format(table_name, col_name))
        returned_index_id = cursor.fetchone()
        #print('fetchall', cursor.fetchall(), returned_index_id)
        cursor.close()

    @staticmethod
    def remove_hypo_index(table_name, col_name):
        """
            Removes the input Hypo Index
        """
        key = table_name + Constants.MULTI_KEY_CONCATENATION_STRING + col_name
        # Check whether index is already present
        if key in PostgresQueryHandler.hypo_indexes_dict:
            cursor = PostgresQueryHandler.__get_connection().cursor()
            # Retrieve index id from dict and replace the place holder with index id
            cursor.execute(Constants.QUERY_REMOVE_HYPO_INDEX.format(PostgresQueryHandler.hypo_indexes_dict.get(key, 0)))
            cursor.close()
            PostgresQueryHandler.hypo_indexes_dict.pop(key, None)

    @staticmethod
    def remove_all_hypo_indexes():
        """
            Removes all the Hypo Indexes
        """
        cursor = PostgresQueryHandler.__get_default_connection().cursor()
        cursor.execute(Constants.QUERY_REMOVE_ALL_HYPO_INDEXES)
        cursor.close()

    @staticmethod
    def get_where_clause_list_for_query(query: str):
        """
            Retreives the complete WHERE CLAUSE
        """
        query_tree = Node(parse_sql(query))
        for tre in query_tree:
            for node in tre.stmt.whereClause:
                print(str(node))

    @staticmethod
    def check_hypo_indexes():
        """
            Check the indexes present in HypoPG
        """
        cursor = PostgresQueryHandler.__get_default_connection().cursor()
        cursor.execute(Constants.QUERY_CHECK_HYPO_INDEXES)
        #print(cursor.fetchall())
        cursor.close()

    @staticmethod
    def add_query_cost_suggested_indexes(result):
        """

        :param result: explain plan
        :return: query cost from the explain plan
        """
        #PostgresQueryHandler.check_hypo_indexes()
        explain_plan = ' \n'.join(map(str, result))
        # extract cost
        #print(explain_plan)
        cost_pattern = "cost=(.*)row"
        cost_match = re.search(cost_pattern, explain_plan)
        if cost_match is not None:
            cost_query = cost_match.group(1).split('..')[-1]
            return float(cost_query)

    @staticmethod
    def get_observation_space(queries_list):
        """
        :param queries_list: list of queries
        :return: observation matrix containing the selectivity factor
            Calculates the initial observation space with the selectivity factors.
            Currently a static matrix as the observation space has only 7 queries and 61 coloumns.
            To work on dynamic method a whole new mechanism needs to be written on how to construct the action_space_json file
        """
        observation_space = np.array(np.ones((8, 61)))
        observation_space[0, :] = np.array((np.zeros((61,))))
        action_space = Utils.read_json_action_space()
        for query_number in range(len(queries_list)):
            for key, value in queries_list[query_number].where_clause_columns_query.items():
                table_name, col_name = key.split(Constants.MULTI_KEY_CONCATENATION_STRING)
                selectivity_index = action_space[(table_name + "." + col_name).upper()]
                observation_space[query_number+1, selectivity_index] = queries_list[query_number].selectivity_for_where_clause_columns[key]
        return observation_space
