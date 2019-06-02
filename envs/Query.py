from typing import Set, Dict
import sqlparse
from pglast import Node, parse_sql
from pglast.node import List
from environment import Table


class Query:
    query_string: str
    table_alias_dict: Dict[str, str]
    where_clause_columns_query: Dict[str, str]

    def __init__(self, query_string, tables_map: dict):
        self.query_string = sqlparse.format(query_string, strip_comments=True, reindent=True).strip()
        self.table_alias_dict = dict()
        # parse query to get where clause columns
        res = sqlparse.parse(self.query_string)
        print(res)
        root = Node(parse_sql(self.query_string))
        if root[0].stmt.__class__.__name__ == 'Node':
            self._parse_node(root[0].stmt, tables_map)
        print('\n\n')

    def _parse_node(self, node: Node, tables_map: dict):
        print('group start')
        print(' ')
        if 'fromClause' in node.attribute_names:
            # each list item will hold table details
            for idx, listitem in enumerate(node.parse_tree['fromClause']):
                if 'RangeSubselect' in listitem:
                    for child_node in node.fromClause[idx].traverse():
                        print(child_node.__class__.__name__)
                        if child_node.node_tag == 'RangeSubselect':
                            self._parse_node(child_node.subquery, tables_map)
                            break
                elif 'RangeVar' in listitem:
                    table_info = listitem['RangeVar']
                    # get table name
                    table_name = table_info['relname']
                    # check whether table has alias
                    if 'alias' in table_info:
                        # alias content is a dict
                        table_alias = table_info['alias']['Alias']['aliasname']
                    else:
                        table_alias = table_name
                    self.table_alias_dict[table_alias] = table_name
        if 'whereClause' in node.attribute_names:
            if 'args' in node.whereClause.attribute_names:
                self._parse_where_clause(node.whereClause.args, tables_map)
            else:
                self._parse_where_clause(node.parse_tree['whereClause'], tables_map)
        print('tables map' + str(self.table_alias_dict))
        print('group end', end='\n\n\n')

    def _parse_where_clause(self, where_data, tables_map):
        print(' data type :' + type(where_data).__name__)
        if isinstance(where_data, List):
            for item in where_data:
                if isinstance(item, Node):
                    if 'args' in item.attribute_names:
                        self._parse_where_clause(item.args, tables_map)
                    else:
                        self._parse_where_clause(item.parse_tree, tables_map)
                else:
                    self._parse_where_clause(item, tables_map)
        if isinstance(where_data, dict):
            print(where_data)
            # name will hold conditional operator value
            # lexpr will hold left side expression
            # rexpr will hold right side expression

