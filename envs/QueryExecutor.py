# imports
from typing import Dict, List
import itertools

from gym.envs.postgres_idx_advisor.envs.Column import Column
from gym.envs.postgres_idx_advisor.envs.Constants import Constants
from gym.envs.postgres_idx_advisor.envs.PostgresQueryHandler import PostgresQueryHandler
from gym.envs.postgres_idx_advisor.envs.Table import Table
from gym.envs.postgres_idx_advisor.envs.Utils import Utils

# class
class QueryExecutor:
    # tables_map will hold table name and columne details
    tables_map: Dict[str, Table] = dict()
    column_map: Dict[str, List[str]] = dict()

    @staticmethod
    def initialize_table_information():
        # get list of tables
        tables = tuple(Utils.read_config_data(Constants.CONFIG_TABLES).keys())

        # call postgres to get table details from database
        returned_table_details = PostgresQueryHandler.execute_select_query(
            Constants.QUERY_GET_TABLE_DETAILS.format(tables))

        for table_column in returned_table_details:
            # table_column will have
            #       at position 0: table_name
            #       at position 1: column_name
            #       at position 2: data type and size
            #       at position 3: primary key (true , false)

            data_type = table_column[2]
            table_name = table_column[0]
            column_name = table_column[1]

            # find column size
            # fixed length data types are stored in map
            if data_type in Constants.POSTGRES_DATA_TYPE_SIZE_MAP:
                data_size = Constants.POSTGRES_DATA_TYPE_SIZE_MAP[data_type]

            # if data_type is not present in dict then it is variable length data type ,
            # data size needs to extracted from the text present data_type
            else:
                # size is present with in brackets
                # examples : "character varying(44)" , "numeric(15,2)" , "character(25)"

                # extract size information
                from_index = data_type.find("(")
                to_index = data_type.find(")")
                temp_text = str(data_type[from_index + 1:to_index])
                data_size = sum(int(val) for val in temp_text.split(','))

            # check whether map entry exists for table if not create one
            if table_name not in QueryExecutor.tables_map:
                # get number of rows add it to table object
                QueryExecutor.tables_map[table_name] = Table(table_name,
                                                             PostgresQueryHandler.get_table_row_count(table_name))

            # add column  to table object
            QueryExecutor.tables_map[table_name].add_column(Column(column_name, data_type, data_size))
            # check whether map entry exists for column name if not create one
            if column_name not in QueryExecutor.column_map:
                QueryExecutor.column_map[column_name] = list()
            # add column as key and table as value for easier find
            QueryExecutor.column_map[column_name].append(table_name)

    @staticmethod
    def create_query_matrix(queries: List[str]):
        for query in queries:
            PostgresQueryHandler.get_where_clause_list_for_query(query)
            break

    @staticmethod
    def find_single_query_cost(query: str):
        print(query)

    @staticmethod
    def create_observation_space(queries_list):
        """
        :param queries_list: list of queries
        :param length: if 2 2d dimensional else 3 dimensional
        :return: generates the pbservation matrix using the queries
        """
        return PostgresQueryHandler.get_observation_space(queries_list)

    @staticmethod
    def get_initial_cost(queries_list):
        """
        :param queries_list: list of queries
        :return: cost without any indexes on the queries
        """
        initial_cost = 0.0
        for query in queries_list:
            initial_cost += float(query.query_cost_without_index)
        #print('Cost -0 ', initial_cost)
        return initial_cost

    @staticmethod
    def init_variables(filename):
        """
        :param filename: contains queries
        :return: retrieved queries, predicates and suggested indexes
        """
        query_executor = QueryExecutor()
        query_executor.initialize_table_information()
        queries_list, all_predicates, idx_advisor_suggested_indexes = Utils.get_queries_from_sql_file(
            query_executor.column_map, query_executor.tables_map, filename)
        print('Suggested indexes',idx_advisor_suggested_indexes)
        return queries_list, all_predicates, idx_advisor_suggested_indexes


    @staticmethod
    def get_best_cost(queries_list, idx_advisor_suggested_indexes):
        """
        :param queries_list: list of queries
        :param idx_advisor_suggested_indexes: suggested indexes by the index advisor
        :return: cost of the queries using the suggested indexes
        """
        for suggested_indexes in idx_advisor_suggested_indexes:
            table_name, col_name = suggested_indexes.split(Constants.MULTI_KEY_CONCATENATION_STRING)
            PostgresQueryHandler.create_hypo_index(table_name, col_name)

        PostgresQueryHandler.check_hypo_indexes()

        query_cost_with_idx_advisor_suggestion = 0.0
        for query in queries_list:
            result = PostgresQueryHandler.execute_select_query(query.query_string, load_index_advisor=False,
                                                               get_explain_plan=True)
            query_cost_with_idx_advisor_suggestion += PostgresQueryHandler.add_query_cost_suggested_indexes(
                result)
        print('Best Total Cost')
        print(query_cost_with_idx_advisor_suggestion)
        PostgresQueryHandler.remove_all_hypo_indexes()
        return query_cost_with_idx_advisor_suggestion

    @staticmethod
    def generate_next_state(queries_list, action, observation_space):
        """
        :param queries_list: list of queries
        :param action: action given by the agent
        :param observation_space: existing observation
        :return: new observation and the cost of the action in the DB
            Generates the next observation state and the cost of performing an action given by the agent
        """
        observation_space[0, action] = 1
        action_space = Utils.read_json_action_space()
        #table_name, col_name = None
        for key, value in action_space.items():
            if value == action:
                table_name, col_name = key.split(".")
                break;
        PostgresQueryHandler.create_hypo_index(table_name, col_name)
        PostgresQueryHandler.check_hypo_indexes()
        query_cost_with_idx_advisor_suggestion = 0.0
        for query in queries_list:
            result = PostgresQueryHandler.execute_select_query(query.query_string, load_index_advisor=False,
                                                               get_explain_plan=True)
            query_cost_with_idx_advisor_suggestion += PostgresQueryHandler.add_query_cost_suggested_indexes(
                result)
        return observation_space, query_cost_with_idx_advisor_suggestion

    @staticmethod
    def remove_all_hypo_indexes():
        PostgresQueryHandler.remove_all_hypo_indexes()

    @staticmethod
    def get_gin_properties():
        """
        :return: returns the offset, train file and test file locations
        """
        gin_config = Utils.read_config_data(Constants.CONFIG_GINPROPERTIES)
        k_offset = gin_config["k_offset"]
        train_file = gin_config["train_file"]
        test_file = gin_config["test_file"]
        agent = gin_config["agent"]
        return int(k_offset), train_file, test_file, agent

    @staticmethod
    def check_step_variables(observation, cost_agent_idx, switch_correct, k, k_idx, value, value_prev, done, reward, counter, action):
        """
            Prints the parameters passed
        """
        print('observation',observation)
        print('cost_agent_idx', cost_agent_idx)
        print('switch_correct', switch_correct)
        print('k', k)
        print('k_idx', k_idx)
        print('value', value)
        print('value_prev', value_prev)
        print('done', done)
        print('reward', reward)
        print('counter', counter)
        print('action', action)

    @staticmethod
    def get_best_index_combination(queries_list, idx_advisor_suggested_indexes, k):
        """
        :param queries_list: list of queries on which the cost should be calculated
        :param idx_advisor_suggested_indexes: indexes suggested by the advisor
        :param k: number of indexes that should be considered while calculating the best cost
        :return: returns the best combination cost
            Gets the combination of the specified k value and calculates the best cost
        """
        index_combinations = [list(x) for x in itertools.combinations(idx_advisor_suggested_indexes, k)]
        max_cost = float("inf")
        best_index_combination = []
        for val in index_combinations:
            current_cost = 0.0
            for suggested_indexes in val:
                table_name, col_name = suggested_indexes.split(Constants.MULTI_KEY_CONCATENATION_STRING)
                PostgresQueryHandler.create_hypo_index(table_name, col_name)

            for query in queries_list:
                result = PostgresQueryHandler.execute_select_query(query.query_string, load_index_advisor=False,
                                                                   get_explain_plan=True)
                current_cost += PostgresQueryHandler.add_query_cost_suggested_indexes(
                    result)

            if current_cost < max_cost:
                max_cost = current_cost
                best_index_combination = val
            PostgresQueryHandler.remove_all_hypo_indexes()
        print('best_index_combination', best_index_combination)
        print('max cost', max_cost)
        return max_cost
