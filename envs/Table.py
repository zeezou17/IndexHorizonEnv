# imports
from gym.envs.postgres_idx_advisor.envs.Column import Column
from typing import Dict


# Class
class Table:

    def __init__(self, table_name, number_of_rows):
        self._name = table_name
        self._number_of_rows = number_of_rows
        # column map contains  column details
        self._column_map: Dict[str, Column] = dict()

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, table_name: str):
        self._name = table_name

    @property
    def number_of_rows(self):
        return self._number_of_rows

    @name.setter
    def name(self, number_of_rows: int):
        self._number_of_rows = number_of_rows

    def add_column(self, column: Column):
        self._column_map[column.name] = column

    def get_column(self, column_name):
        return self._column_map.get(column_name)

    def __str__(self):
        return_string = "table name : " + self._name + " \n Columns "
        for key, value in self._column_map.items():
            return_string = return_string + "\n      " + str(value)
        return return_string
