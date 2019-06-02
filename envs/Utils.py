import configparser
from typing import List, Dict
from environment.Query import Query
from environment.Table import Table


class Utils:
    @staticmethod
    def read_config_data(section: str):
        # read database config from config file
        config = configparser.ConfigParser()
        config.read('../config.ini')
        return dict(config.items(section))

    @staticmethod
    def get_queries_from_sql_file(tables_map: Dict[str, Table]):
        sql_file = open('../temp.sql', 'r')
        file_content = sql_file.read()
        sql_file.close()
        queries_list: List[Query] = list()
        for query_text in file_content.split(';'):
            # remove comments
            if query_text.strip() != '':
                queries_list.append(Query(query_text, tables_map))
        return queries_list

