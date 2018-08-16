from befh.clients.sql import SqlClient
from befh.clients.database import DatabaseClient
from befh.util import Logger
import psycopg2
import psycopg2.extras

class PostgresqlClient(SqlClient):
    """
    Postgresql client
    """
    @classmethod
    def replace_keyword(cls):
        return 'insert into'

    def __init__(self):
        """
        Constructor
        """
        SqlClient.__init__(self)

    def connect(self, **kwargs):
        """
        Connect
        :param path: sqlite file to connect
        """
        host = kwargs['host']
        port = kwargs['port']
        user = kwargs['user']
        pwd = kwargs['pwd']
        schema = kwargs['schema']
        #self.conn = pymysql.connect(host=host,
        #                            port=port,
        #                            user=user,
        #                            password=pwd,
        #                            db=schema,
        #                            charset='utf8mb4',
        #                            cursorclass=pymysql.cursors.DictCursor)
        self.conn = psycopg2.connect(host=host,
                                     port=port,
                                     dbname=schema,
                                     user=user,
                                     password=pwd)
        self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        return self.conn is not None and self.cursor is not None

    def execute(self, sql):
        """
        Execute the sql command
        :param sql: SQL command
        """
        try:
            return_val = self.cursor.execute(sql)
            return True
        except Exception as err:

            raise

    def commit(self):
        """
        Commit
        """
        self.conn.commit()

    def fetchone(self):
        """
        Fetch one record
        :return Record
        """
        return self.cursor.fetchone()

    def fetchall(self):
        """
        Fetch all records
        :return Record
        """
        return self.cursor.fetchall()

    def select(self, table, columns=['*'], condition='', orderby='', limit=0, isFetchAll=True):
        """
        Select rows from the table
        :param table: Table name
        :param columns: Selected columns
        :param condition: Where condition
        :param orderby: Order by condition
        :param limit: Rows limit
        :param isFetchAll: Indicator of fetching all
        :return Result rows
        """
        select = SqlClient.select(self, table, columns, condition, orderby, limit, isFetchAll)
        if len(select) > 0:
            if columns[0] != '*':
                ret = []
                for ele in select:
                        row = []
                        for column in columns:
                            row.append(ele[column])

                        ret.append(row)
            else:
                ret = [list(e.values()) for e in select]

            return ret
        else:
            return select

    def insert(self, table, columns, types, values, primary_key_index=(), is_orreplace=False, is_commit=True):
        """
        Insert into the table
        :param table: Table name
        :param columns: Column array
        :param types: Type array
        :param values: Value array
        :param primary_key_index: An array of indices of primary keys in columns,
                          e.g. [0] means the first column is the primary key
        :param is_orreplace: Indicate if the query is "INSERT OR REPLACE"
        """
        if len(columns) != len(values):
            return False

        column_names = ','.join(columns)
        value_string = ','.join([SqlClient.convert_str(e) for e in values])
        if is_orreplace:
            conflict_update_string = []
            column_vals = dict(zip(columns, [SqlClient.convert_str(e) for e in values]))
            for column, val in column_vals.items():
                if column == "id":
                    continue

                conflict_update_string += [('%s  = %s' % (column, val))]

            conflict_update_string = ', '.join(conflict_update_string)

            if table == "exchanges_snapshot":
                conflict_columns = "exchange, instmt"
            else:
                conflict_columns = "id"

            sql = "insert into %s (%s) values (%s) on conflict (%s) do update set %s" % (table, column_names, value_string, conflict_columns, conflict_update_string)
        else:
            sql = "insert into %s (%s) values (%s)" % (table, column_names, value_string)

        self.lock.acquire()
        try:
            self.execute(sql)
            if is_commit:
                self.commit()
        except Exception as e:
            Logger.info(self.__class__.__name__, "SQL error: %s\nSQL: %s" % (e, sql))
        self.lock.release()
        return True