import configparser
from typing import List, Dict
from gym.envs.postgres_idx_advisor.envs.Query import Query
from gym.envs.postgres_idx_advisor.envs.Table import Table
from gym.envs.postgres_idx_advisor.envs.Constants import Constants
import json


class Utils:
    @staticmethod
    def read_config_data(section: str):
        # read database config from config file
        config = configparser.ConfigParser()
        config.read('/home/bhuvanesh/Documents/indexHorizonEnv/config.ini')
        return dict(config.items(section))

    @staticmethod
    def get_queries_from_sql_file(columns_map: Dict[str, List[str]], tables_map: Dict[str, Table], filename):
        Query.reset()
        sql_file = open(filename, 'r')
        file_content = sql_file.read()
        sql_file.close()
        queries_list: List[Query] = list()
        for query_text in file_content.split(';'):
            # remove comments
            if query_text.strip() != '':
                queries_list.append(Query(query_text, columns_map, tables_map))
        return queries_list, Query.all_predicates, Query.idx_advisor_suggested_indexes

    @staticmethod
    def read_json_action_space():
        gin_config = Utils.read_config_data(Constants.CONFIG_GINPROPERTIES)
        action_space_json = gin_config["action_space_json"]

        with open(action_space_json) as json_file:
            action_space = json.load(json_file)
        return action_space