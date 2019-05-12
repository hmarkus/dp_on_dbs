import select
import re
import psycopg2 as pg
from psycopg2 import sql

import logging

DEBUG_SQL = logging.DEBUG - 5

def setup_debug_sql():
    logging.addLevelName(DEBUG_SQL, "SQL")

    def debug_sql(self, message, *args, **kws):
        if self.isEnabledFor(DEBUG_SQL):
            self._log(DEBUG_SQL, message, args, **kws)
    logging.Logger.debug_sql = debug_sql

logger = logging.getLogger(__name__)

class DB(object):
    _pool = None
    _conn = None
    _auto_commit = False
    _praefix = None
    _ignore_next_praefix = 0

    @classmethod
    def from_cfg(cls, params):
        instance = cls()
        instance.connect(params)
        return instance

    @classmethod
    def from_pool(cls, pool):
        instance = cls()
        instance._pool = pool
        instance._conn = pool.getconn()
        return instance 

    # TODO: improve logger.debug_sql so this is not needed?
    def __debug_query__ (self, query, params = []):
        logger.debug_sql(query.as_string(self._conn),*params)
        """
        if not self._debug:
            return

        print("[DB] Executing: "),
        print(query.as_string(self._conn)),
        if params:
            print(" (params = %s)" % str(params))
        else:
            print()
        """

    def __table_name__(self, table):
        if self._praefix and self._ignore_next_praefix == 0:
            return sql.Identifier(self._praefix+table)
        else:
            if self._ignore_next_praefix > 0:
                self._ignore_next_praefix -= 1
            return sql.Identifier(table)

    def connect(self, params):
        self._conn = pg.connect(**params)

    def close(self):
        if self._pool:
            self._pool.putconn(self._conn)
        else:
            self._conn.close()
            self._conn = None

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    """
    use this maybe if we want to support distributed workers

    def listen(self, name):
        self.execute_ddl("LISTEN %s" % name)

    def poll(self):
        while True:
            if select.select([self._conn],[],[],5) == ([],[],[]):
                print "Timeout"
            else:
                self._conn.poll()
                while self._conn.notifies:
                    notify = self._conn.notifies.pop(0)
                    print "Got NOTIFY:", notify.pid, notify.channel, notify.payload
                    return True
    """

    def execute(self,q,p = []):
        self.__debug_query__(q,p)
        with self._conn.cursor() as cur:
            cur.execute(q,p)
            self.last_rowcount = cur.rowcount

    def exec_and_fetch(self,q,p = []):
        self.__debug_query__(q,p)
        with self._conn.cursor() as cur:
            cur.execute(q,p)
            self.last_rowcount = cur.rowcount
            return cur.fetchone()
        
    def execute_ddl(self,q):
        self.__debug_query__(q)
        with self._conn.cursor() as cur:
            cur.execute(q)
        # DDL always auto-commits as its default for many DBMS 
        # should make transition to e.g. Oracle easier
        self.commit()

    def drop_table(self, name, if_exists = True):
        q = sql.SQL("DROP TABLE %s {}" % "IF EXISTS" if if_exists else "").format(
                    self.__table_name__(name)
                    )
        self.execute_ddl(q)

    def create_table(self, name, columns, if_not_exists = True):
        q = sql.SQL("CREATE TABLE %s {} ({})" % "IF NOT EXISTS" if if_not_exists else "").format(
                    self.__table_name__(name),
                    sql.SQL(', ').join(sql.Identifier(c[0]) + sql.SQL(" "+c[1]) for c in columns)
                    )
        self.execute_ddl(q)

    def create_view(self, name, text):
        q = sql.SQL("CREATE VIEW {} AS ").format(self.__table_name__(name))
        q = sql.Composed([q,sql.SQL(text)])
        self.execute_ddl(q)

    # TODO: get this working without full-fletched query-builder?
    def select(self,columns,where = None):
        tab_alias = {}
        cnt = 1
        for c in columns:
            tab = c[0]
            if ' ' in tab:
                tab_alias[tab] = tab[tab.find(' ')+1:]
            elif tab not in tab_alias:
                tab_alias[tab] = "t{}".format(cnt)
                cnt += 1
        q = sql.SQL("SELECT {} FROM {}").format(
                    sql.SQL(', ').join(map(lambda c: sql.Identifier(tab_alias[c[0]],str(c[1])), columns)),
                    sql.SQL(', ').join(map(lambda t: sql.Composed([sql.Identifier(t[0]),
                                                                   sql.SQL(' '),
                                                                   sql.Identifier(t[1])
                                                                  ])
                                        ,tab_alias.iteritems()))
                    )
        """
        if where:
            for w in where:

            q = sql.Composed([q,sql.SQL(" WHERE "), where_str])
        """
        print(q.as_string(self._conn))

    def replace_dynamic_tabs(self,query,tables):
        for t in tables:
            query = re.sub('(\W){}((\W|$))'.format(t),
                "\g<1>{}\g<2>".format(self.__table_name__(t).as_string(self._conn)),
                query)
        return query;

    def insert(self, table, columns, values, returning = None):
        sql_str = "INSERT INTO {} ({}) VALUES ({})"
        q = sql.SQL(sql_str).format(
                    self.__table_name__(table),
                    sql.SQL(', ').join(map(sql.Identifier, columns)),
                    sql.SQL(', ').join(sql.Placeholder() * len(columns))
                    )
        if returning:
            q = sql.Composed([q,sql.SQL(" RETURNING {}").format(sql.Identifier(returning))])
            return self.exec_and_fetch(q,values)
        else:
            self.execute(q,values)

    def insert_select(self, table, select, columns = None, returning = None):
        sql_str = "INSERT INTO {} {}"
        q = sql.SQL(sql_str).format(self.__table_name__(table), sql.SQL(select))
        if columns:
            q = sql.Composed([q,sql.SQL(', ').join(map(sql.Identifier, columns))])
        if returning:
            q = sql.Composed([q,sql.SQL(" RETURNING {}").format(sql.Identifier(returning))])
            return self.exec_and_fetch(q)
        else:
            self.execute(q)

    def update(self, table, columns, values, where = None, returning = None):
        sql_str = "UPDATE {} SET {}"
        q = sql.SQL(sql_str).format(
                    self.__table_name__(table),
                    sql.SQL(', ').join([sql.SQL("{} = {}").format(
                                sql.Identifier(s[0]),sql.SQL(s[1])
                            ) for s in zip(columns, values)])
                    )
        if where:
            q = sql.Composed([q,sql.SQL(" WHERE {}").format(sql.SQL(' AND ').join(map(sql.SQL,where)))])

        if returning:
            q = sql.Composed([q,sql.SQL(" RETURNING {}").format(sql.Identifier(returning))])
            return self.exec_and_fetch(q)
        else:
            self.execute(q)

    def call(self, procedure, params = []):
        q = sql.SQL("CALL {} ({})").format(
                    sql.Identifier(procedure),
                    sql.SQL(', ').join(sql.Placeholder() * len(params))
                    )
        self.execute(q,params)

    def set_praefix(self, praefix):
        self._praefix = praefix

    def ignore_next_praefix(self, count = 1):
        self._ignore_next_praefix = count

from psycopg2.pool import ThreadedConnectionPool
from threading import Semaphore

class BlockingThreadedConnectionPool(ThreadedConnectionPool):
    def __init__(self, minconn, maxconn, *args, **kwargs):
        self._semaphore = Semaphore(maxconn)
        super(BlockingThreadedConnectionPool,self).__init__(minconn, maxconn, *args, **kwargs)

    def getconn(self, *args, **kwargs):
        self._semaphore.acquire()
        return super(BlockingThreadedConnectionPool,self).getconn(*args, **kwargs)

    def putconn(self, *args, **kwargs):
        super(BlockingThreadedConnectionPool,self).putconn(*args, **kwargs)
        self._semaphore.release()
