# This class holds
#  all constants in the environment package
#  postgres sql queries


class Constants(object):
    QUERY_CHECK_HYPO_INDEXES = "SELECT * FROM hypopg_list_indexes()"
    QUERY_CREATE_HYPO_INDEX = "SELECT indexrelid FROM hypopg_create_index('CREATE INDEX ON  {0}({1})')"
    QUERY_REMOVE_HYPO_INDEX = "select * from hypopg_drop_index( {0} )"
    QUERY_FIND_SELECTIVITY = "select count(1) from {0} where {1}"
    QUERY_FIND_NUMBER_OF_ROWS = "select count(1) from {0} "
    QUERY_REMOVE_ALL_HYPO_INDEXES = "select * from  hypopg_reset()"
    LOAD_PG_IDX_ADVISOR = "load 'pg_idx_advisor.so'"
    DROP_PG_IDX_ADVISOR = "DROP EXTENSION IF EXISTS pg_idx_advisor"
    CREATE_EXTENSION = "CREATE EXTENSION IF NOT EXISTS pg_idx_advisor"
    QUERY_EXPLAIN_PLAN = "EXPLAIN (format text) ({0})"
    QUERY_GET_TABLE_DETAILS = "SELECT DISTINCT " \
                              "             pgc.relname as table_name , " \
                              "             a.attname as column_name, " \
                              "             format_type(a.atttypid, a.atttypmod) as data_type, " \
                              "             coalesce(i.indisprimary,false) as primary_key " \
                              "     FROM pg_attribute a  " \
                              "     JOIN pg_class pgc ON pgc.oid = a.attrelid " \
                              "     LEFT JOIN pg_index i ON  " \
                              "                (pgc.oid = i.indrelid AND i.indkey[0] = a.attnum) " \
                              "     LEFT JOIN pg_description com on  " \
                              "                (pgc.oid = com.objoid AND a.attnum = com.objsubid) " \
                              "     LEFT JOIN pg_attrdef def ON  " \
                              "                 (a.attrelid = def.adrelid AND a.attnum = def.adnum) " \
                              "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = pgc.relnamespace " \
                              "WHERE 1=1  " \
                              "   AND pgc.relkind IN ('r','') " \
                              "   AND n.nspname <> 'pg_catalog' " \
                              "   AND n.nspname <> 'information_schema' " \
                              "   AND n.nspname <> 'information_schema' " \
                              "   AND n.nspname <> 'information_schema' " \
                              "   AND n.nspname <> 'information_schema' " \
                              "   AND n.nspname <> 'information_schema' " \
                              "   AND n.nspname !~ '^pg_toast' " \
                              "   AND a.attnum > 0 AND pgc.oid = a.attrelid " \
                              "	  AND pg_table_is_visible(pgc.oid) " \
                              "	  AND NOT a.attisdropped " \
                              "	  AND pgc.relname in {0} " \
                              "	ORDER BY table_name,column_name"
    MULTI_KEY_CONCATENATION_STRING = '::'
    CONFIG_TABLES = 'Tables'
    CONFIG_DATABASE = 'Database'
    CONFIG_GINPROPERTIES = 'GINProperties'
    POSTGRES_DATA_TYPE_SIZE_MAP = {'bigint': 8, 'boolean': 1, 'date': 4, 'integer': 4, 'smallint': 2, 'double': 8}
    POSTGRES_WHERE_CLAUSE_OPERATORS_LIST = ['<', '>', '<=', '>=', '=', '<>', '!=', ' IS ', ' is ', ' isnull', ' ISNULL',
                                            ' NOTNULL', ' notnull', ' between ', ' in ']
    POSTGRES_WHERE_CLAUSE_BOOLEAN_OPERATIONS_LIST = ['AND ', 'and ', 'or ', 'OR ']
    POSTGRES_BOOLEAN_OPERATIONS_LIST_AS_STRING = "|".join(POSTGRES_WHERE_CLAUSE_BOOLEAN_OPERATIONS_LIST)
    POSTGRES_PREDICATE_OPERATIONS_LIST_AS_STRING = "|".join(POSTGRES_WHERE_CLAUSE_OPERATORS_LIST)
    POSTGRES_ORDER_BY_GROUP_BY_KEYWORDS = "GROUP BY|group by|ORDER by|order by|limit |LIMIT "
    POSTGRES_KEYWORD_FROM = "from |FROM "
    POSTGRES_KEYWORD_WHERE = "where | WHERE"

    # this is added to restrict modification of constant attributes/ fields
    def __setattr__(self, *_):
        pass


Constants = Constants()
