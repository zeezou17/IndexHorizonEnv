# imports
from typing import Dict, List

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
        #print('Observation Space', PostgresQueryHandler.get_observation_space(queries_list))
        return PostgresQueryHandler.get_observation_space(queries_list)

    @staticmethod
    def get_initial_cost(queries_list):
        initial_cost = 0.0
        for query in queries_list:
            #print(queries_list)
            initial_cost += float(query.query_cost_without_index)
        print('Cost -0 ', initial_cost)
        return initial_cost

    @staticmethod
    def init_variables(eval_mode):
        print('eval_mode', eval_mode)
        query_executor = QueryExecutor()
        query_executor.initialize_table_information()
        queries_list, all_predicates, idx_advisor_suggested_indexes = Utils.get_queries_from_sql_file(
            query_executor.column_map, query_executor.tables_map)
        print('Suggested indexes',idx_advisor_suggested_indexes)
        return queries_list, all_predicates, idx_advisor_suggested_indexes


    @staticmethod
    def get_best_cost(queries_list, idx_advisor_suggested_indexes):
        #print('*********************************')
        #print('Set Hypo PG Index')
        for suggested_indexes in idx_advisor_suggested_indexes:
            table_name, col_name = suggested_indexes.split(Constants.MULTI_KEY_CONCATENATION_STRING)
            #print(table_name, col_name)
            PostgresQueryHandler.create_hypo_index(table_name, col_name)

        #print('Verify if indexes are setting')
        PostgresQueryHandler.check_hypo_indexes()

        #print('Get total cost of the all queries')
        query_cost_with_idx_advisor_suggestion = 0.0
        for query in queries_list:
            #print('************************************')
            #print('QUERY :' + query.query_string)
            #print('******************')
            result = PostgresQueryHandler.execute_select_query(query.query_string, load_index_advisor=False,
                                                               get_explain_plan=True)
            query_cost_with_idx_advisor_suggestion += PostgresQueryHandler.add_query_cost_suggested_indexes(
                result)
            #print(query_cost_with_idx_advisor_suggestion)
        print('Best Total Cost')
        print(query_cost_with_idx_advisor_suggestion)

        #print('******************************')
        #print('Remove hypo indexes')
        PostgresQueryHandler.remove_all_hypo_indexes()
        return query_cost_with_idx_advisor_suggestion

    @staticmethod
    def generate_next_state(queries_list, action, observation_space):
        observation_space[0, action] = 1
        #observation_space[:, action] = 1
        #print('action', action)
        #print(observation_space)
        action_space = PostgresQueryHandler.read_json_action_space()
        #table_name, col_name = None
        for key, value in action_space.items():
            if value == action:
                table_name, col_name = key.split(".")
                #print('action',action,table_name,col_name)
                break;
        PostgresQueryHandler.create_hypo_index(table_name, col_name)
        PostgresQueryHandler.check_hypo_indexes()
        query_cost_with_idx_advisor_suggestion = 0.0
        for query in queries_list:
            #print('************************************')
            #print('QUERY :' + query.query_string)
            #print('******************')
            result = PostgresQueryHandler.execute_select_query(query.query_string, load_index_advisor=False,
                                                               get_explain_plan=True)
            query_cost_with_idx_advisor_suggestion += PostgresQueryHandler.add_query_cost_suggested_indexes(
                result)
            # print(query_cost_with_idx_advisor_suggestion)
        print('Agent Cost')
        print(query_cost_with_idx_advisor_suggestion)
        return observation_space, query_cost_with_idx_advisor_suggestion

    @staticmethod
    def remove_all_hypo_indexes():
        PostgresQueryHandler.remove_all_hypo_indexes()

    @staticmethod
    def check_step_variables(observation,cost_agent_idx,switch_correct,k,k_idx,value,value_prev,done,reward,counter,action):
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
#query_executor = QueryExecutor()
#query_executor.initialize_table_information()
#queries_list, all_predicates,idx_advisor_suggested_indexes = Utils.get_queries_from_sql_file(query_executor.column_map,query_executor.tables_map)
#queries_list, all_predicates,idx_advisor_suggested_indexes = QueryExecutor.init_variables()
"""for query in queries_list:
    print('************************************')
    print('QUERY :' + query.query_string)
    print('******************')
    print('**EXTRACTED PREDICATES**')
    print('******************')
    for key, value in query.where_clause_columns_query.items():
        print(key + '   :  ' + value+' : '+str(query.selectivity_for_where_clause_columns[key])+'  : '+str(query.query_cost_without_index) , query.query_string)
        print()
    print('******************')

print(' all predicates ')
print(all_predicates)
print('all suggested indexes')
print(idx_advisor_suggested_indexes)
# query_executor.create_query_matrix(Utils.get_queries_from_sql_file())
# for key, value in query_executor.tables_map.items():
#     print(value)


print('***************************************')
print('Functions for implementing in env')
print('*******************1***************')
print('Initialize DB')"""

#queries_list, all_predicates,idx_advisor_suggested_indexes = QueryExecutor.init_variables()
"""print('Functions for implementing in env')
print('*******************1***************')
print('Get Observation Space')"""

#print(QueryExecutor.create_observation_space(queries_list))
"""
observation_space = QueryExecutor.create_observation_space(queries_list)
print('************************************')
print('*******************2***************')
print('Get Initial Cost')
print(QueryExecutor.get_initial_cost(queries_list))

print('************************************')
print('*******************3***************')
print('Get Best Cost')
print(QueryExecutor.get_best_cost(queries_list, idx_advisor_suggested_indexes))

print('************************************')
print('*******************3***************')
print('Agent Action Response')
action = 2
new_obs, new_cost = QueryExecutor.generate_next_state(queries_list, action, observation_space)
print(new_obs, new_cost)

print('**********************************')
print('Get matrix')
print(PostgresQueryHandler.get_observation_space(queries_list))

print('*********************************')
print('Set Hypo PG Index')
for suggested_indexes in idx_advisor_suggested_indexes:
    table_name, col_name = suggested_indexes.split(Constants.MULTI_KEY_CONCATENATION_STRING)
    #print(table_name, col_name)
    PostgresQueryHandler.create_hypo_index(table_name, col_name)

print('Verify if indexes are setting')
PostgresQueryHandler.check_hypo_indexes()


print('Get total cost of the all queries')
query_cost_with_idx_advisor_suggestion = 0.0
for query in queries_list:
    print('************************************')
    print('QUERY :' + query.query_string)
    print('******************')
    result = PostgresQueryHandler.execute_select_query(query.query_string, load_index_advisor=False, get_explain_plan=True)
    query_cost_with_idx_advisor_suggestion = query_cost_with_idx_advisor_suggestion + PostgresQueryHandler.add_query_cost_suggested_indexes(result)
    print(query_cost_with_idx_advisor_suggestion)
print('Total Cost')
print(query_cost_with_idx_advisor_suggestion)

print('******************************')
print('Remove hypo indexes')
PostgresQueryHandler.remove_all_hypo_indexes()
"""