# imports
from typing import Dict, List
from environment.PostgresQueryHandler import PostgresQueryHandler
from environment.Utils import Utils
from environment.Constants import Constants
from environment.Table import Table
from environment.Column import Column


# class
class QueryExecutor:
    # tables_map will hold table name and columne details
    tables_map: Dict[str, Table] = dict()

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
                QueryExecutor.tables_map[table_name] = Table(table_name)

            # add column  to table object
            QueryExecutor.tables_map[table_name].add_column(Column(column_name, data_type, data_size))

    @staticmethod
    def create_query_matrix(queries: List[str]):
        for query in queries:
            PostgresQueryHandler.get_where_clause_list_for_query(query)
            break

    @staticmethod
    def find_single_query_cost(query: str):
        print(query)


query_executor = QueryExecutor()
query_executor.initialize_table_information()
Utils.get_queries_from_sql_file(query_executor.tables_map)
#query_executor.create_query_matrix(Utils.get_queries_from_sql_file())
# for key, value in query_executor.tables_map.items():
#     print(value)
