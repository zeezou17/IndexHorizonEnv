import sqlparse
import time
import re
from gym.envs.postgres_idx_advisor.envs.PostgresQueryHandler import PostgresQueryHandler


class AdvisorPerformanceMeasurer:

    @staticmethod
    def measure_average_time_taken(filename: str, iterations: int, is_hypo_pg_time_included: bool = False):
        """

        :param filename: contains the queries for each iteration on which the time needs to be measured
        :param iterations: the number of times the time needs to be noted
        :param is_hypo_pg_time_included: if set true it considers the time it needs to set an index in hypopg
        :return: time taken for the Postgres Index Advisor to get indexes.

            Measures the amount of time taken by the postgres Index Advisor
        """
        sql_file = open(filename, 'r')
        file_content = sql_file.read()
        sql_file.close()
        index_pattern = "(create index.+?(?='))"
        start_time = time.time()
        for i in range(iterations):
            query_list = list()
            for query_text in file_content.split(';'):
                if query_text.strip() != '':
                    # remove comments
                    query = sqlparse.format(query_text, strip_comments=True, reindent=True, wrap_after=100).strip()
                    # get index advisor suggestion
                    explain_plan = PostgresQueryHandler.execute_select_query(query, load_index_advisor=True,
                                                                             get_explain_plan=True)
                    if is_hypo_pg_time_included:
                        query_list.append(query)
                        explain_plan = ' \n'.join(map(str, explain_plan))
                        # extract cost
                        for match in re.finditer(index_pattern, explain_plan):
                            # this statment will filter create index statements and
                            # then will extract table name and column name in a array at pos 0 and 1 respectively
                            table_col = explain_plan[match.start():match.end()].split(' on ')[1].replace('(',
                                                                                                         ' ').replace(
                                ')', '').strip().split(' ')
                            PostgresQueryHandler.create_hypo_index(table_col[0], table_col[1])
            if is_hypo_pg_time_included:
                for query_1 in query_list:
                    PostgresQueryHandler.execute_select_query(query_1, load_index_advisor=False, get_explain_plan=True)
                PostgresQueryHandler.remove_all_hypo_indexes()
        end_time = time.time()
        return (end_time - start_time) / iterations