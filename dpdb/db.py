# -*- coding: future_fstrings -*-
import logging
import select
import re
import psycopg2 as pg
from psycopg2 import sql
from psycopg2.pool import ThreadedConnectionPool
from threading import Semaphore
import numpy as np

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

    # we need this wrapper because conn object is required
    def __debug_query__ (self, query, params = []):
        logger.debug_sql(query.as_string(self._conn),*params)

    def __table_name__(self, table):
        if self._praefix and self._ignore_next_praefix == 0:
            return sql.Identifier(self._praefix+table)
        else:
            if self._ignore_next_praefix > 0:
                self._ignore_next_praefix -= 1
            return sql.Identifier(table)

    def connect(self, params):
        self._db_name = params["database"]
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

    def execute(self,q,p = []):
        try:
            self.__debug_query__(q,p)
            with self._conn.cursor() as cur:
                cur.execute(q,p)
                self.last_rowcount = cur.rowcount
        except pg.errors.AdminShutdown:
            logger.warning("Connection closed by admin")

    def exec_and_fetch(self,q,p = []):
        try:
            self.__debug_query__(q,p)
            with self._conn.cursor() as cur:
                cur.execute(q,p)
                self.last_rowcount = cur.rowcount
                return cur.fetchone()
        except pg.errors.AdminShutdown:
            logger.warning("Connection closed by admin")
    
    # returns all rows not only the first one
    def exec_and_fetch_all(self,q,p = []):
        try:
            self.__debug_query__(q,p)
            with self._conn.cursor() as cur:
                cur.execute(q,p)
                self.last_rowcount = cur.rowcount
                return cur.fetchall()
        except pg.errors.AdminShutdown:
            logger.warning("Connection closed by admin")
    
    def execute_ddl(self,q):
        try:
            self.__debug_query__(q)
            with self._conn.cursor() as cur:
                cur.execute(q)
            # DDL always auto-commits as its default for many DBMS
            # should make transition to e.g. Oracle easier
            self.commit()
        except pg.errors.AdminShutdown:
            logger.warning("Connection closed by admin")

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
        #print(q)
        self.execute_ddl(q)

    def create_table_node(self, name, columns, if_not_exists = True):
        q = sql.SQL("CREATE TABLE %s {} ({})" % "IF NOT EXISTS" if if_not_exists else "").format(
                    self.__table_name__(name),
                    sql.Identifier('insert_time') + sql.SQL(' TIMESTAMP,') + sql.SQL(', ').join(sql.Identifier(c[0]) + sql.SQL(" "+c[1]) for c in columns)
                    )
        #print(q)
        self.execute_ddl(q)

    def create_view(self, name, text):
        q = sql.SQL("CREATE VIEW {} AS ").format(self.__table_name__(name))
        q = sql.Composed([q,sql.SQL(text)])
        self.execute_ddl(q)
    
    def create_view_mat(self, name, text):
        q = sql.SQL("CREATE MATERIALIZED VIEW {} AS ").format(self.__table_name__(name))
        q = sql.Composed([q,sql.SQL(text)])
        self.execute_ddl(q)

    def refresh_mat_view(self, name):
        q = sql.SQL("REFRESH MATERIALIZED VIEW {}").format(self.__table_name__(name))
        self.execute_ddl(q)
    
    # create unique index for all the variable columns
    # necessary for the iterative approximation because the tables only have a sequence primary key
    # coalesce is used to make it work with NULL values in the columns (only works if always the same columns are NULL)
    def add_unique_index(self, table, columns):
        q = sql.SQL("CREATE UNIQUE INDEX {} ON {} ((ARRAY[{}])) ").format(
            sql.SQL("unique_" + table),
            self.__table_name__(table),
            #sql.SQL(', ').join(sql.SQL("coalesce(") + sql.Identifier(c) + sql.SQL(",False)") for c in columns)
            sql.SQL(', ').join(sql.Identifier(c) for c in columns)
            )
        #print(q)
        self.execute_ddl(q)


    def drop_view(self, name, if_exists = True): 
        q = sql.SQL("DROP VIEW %s {}" % "IF EXISTS" if if_exists else "").format(
                    self.__table_name__(name)
                    )
        self.execute_ddl(q)

    def replace_dynamic_tabs(self,query):
        def repl(m):
            tab = m.group(2)
            dyn_tab = self.__table_name__(tab).as_string(self._conn)
            return m.group(1) + dyn_tab + m.group(3)

        query = re.sub("(\W)(td_node_\w+)((\W|$))",
            repl,
            query)

        return query


    def select_random(self, rows, columns, problem, node, sel_list, where_filter, group_by):
        distinct_values = ""
        inner_select = ""
        inner_from = ""
        
        first = True
        ids = set()
        for v in node.vertices:
            distinct_values += f"v{v},"
            if node.needs_introduce(v):
                inner_select += "round(random()) :: int :: bool as "
            else:
                nodeid = node.vertex_children(v)[0].id
                inner_select += f"t{nodeid}."
                
                if first:
                    first = False
                    inner_from  += " FROM"
                if nodeid not in ids:
                    inner_from += " {} {},".format(f"p1_td_node_{nodeid}", f"t{nodeid}")
                ids.add(nodeid)
            inner_select += f"v{v},"
            
        distinct_values = distinct_values[:-1]

        extra_cols = problem.candidate_extra_cols(node)
        
        if extra_cols:
            inner_select += "{}".format(",".join(extra_cols))
        inner_select += ", generate_series(1,"+str(rows) + ")"
        

        for n in node.children:
            if first:
                first = False
                inner_from  += " FROM"
            nodeid = n.id
            if nodeid not in ids:
                inner_from += " {} {},".format(f"p1_td_node_{nodeid}", f"t{nodeid}")
            ids.add(nodeid)
         
        if not first:
            inner_from = inner_from[:-1]
            
        q = f"SELECT DISTINCT ON ({distinct_values}) {inner_select} {inner_from}"
        
        if group_by:
            q = f"SELECT {sel_list} FROM ({q}) AS candidate {where_filter} GROUP BY {group_by}"
        else:
            q = f"SELECT {sel_list} FROM ({q}) AS candidate {where_filter}"
        #print(q)
        q = sql.SQL(q)
        return self.exec_and_fetch_all(q)

    def select_random_python(self, rows, columns, problem, node, sel_list, where_filter, group_by):
        distinct_values = ""
        inner_select = ""
        inner_from = ""
        
        random_numbers = 0
        
        first = True
        ids = set()
        for v in node.vertices:
            distinct_values += f"v{v},"
            if node.needs_introduce(v):
                inner_select += "%s :: bool as " 
                random_numbers = random_numbers + 1
            else:
                nodeid = node.vertex_children(v)[0].id
                inner_select += f"t{nodeid}."
                
                if first:
                    first = False
                    inner_from  += " FROM"
                if nodeid not in ids:
                    inner_from += " {} {},".format(f"p1_td_node_{nodeid}", f"t{nodeid}")
                ids.add(nodeid)
            inner_select += f"v{v},"
            
        distinct_values = distinct_values[:-1]

        extra_cols = problem.candidate_extra_cols(node)
        
        if extra_cols:
            inner_select += "{}".format(",".join(extra_cols))
        #inner_select += ", generate_series(1,"+str(rows) + ")"
        

        for n in node.children:
            if first:
                first = False
                inner_from  += " FROM"
            nodeid = n.id
            if nodeid not in ids:
                inner_from += " {} {},".format(f"p1_td_node_{nodeid}", f"t{nodeid}")
            ids.add(nodeid)
         
        if not first:
            inner_from = inner_from[:-1]
            
        q = f"SELECT DISTINCT {inner_select} {inner_from}"
        
        if group_by:
            q = f"SELECT {sel_list} FROM ({q}) AS candidate {where_filter} GROUP BY {group_by}"
        else:
            q = f"SELECT {sel_list} FROM ({q}) AS candidate {where_filter}"
        q = sql.SQL(q)
        print(q)
         
        random_values = np.random.randint(2, size=(rows, random_numbers))
        print(random_values)
        random_values = random_values.tolist()
        
        retVal = set()
        for i in range(rows):
            result = self.exec_and_fetch_all(q, random_values[i])
            #print(result)
            if result:
                #print(type(result))
                retVal.update(tuple(result))
            #print(retVal)
        return retVal
    
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

    def insert_list(self, table, listToInsert, columnCount, columns):
        with self._conn.cursor() as cur:
            q = "INSERT INTO p1_" + str(table) + " VALUES("
            for i in range(columnCount):
                q += "%s, "
            q += "%s) ON CONFLICT ((ARRAY[{}])) DO UPDATE SET model_count = greatest({}.model_count, EXCLUDED.model_count)".format(
                    ",".join(c for c in columns),
                    "p1_" + str(table))
            #print(q)
            cur.executemany(q, listToInsert)

    def insert_select(self, table, select, checkConflict = False, columns = None, returning = None):
        sql_str = "INSERT INTO {} {}"
        q = sql.SQL(sql_str).format(self.__table_name__(table), sql.SQL(select))
        if columns and not checkConflict:
            q = sql.Composed([q,sql.SQL(', ').join(map(sql.Identifier, columns))])
        if checkConflict:
            # if conflicts should be handeld (iterative approximation) set the model count, if a conflict appears
            # to the greater one of the newly inserted or the current one in the table
            q = sql.Composed([q, sql.SQL(' ON CONFLICT ((ARRAY[{}])) DO UPDATE SET model_count = greatest({}.model_count, EXCLUDED.model_count)').format(
                #sql.SQL(', ').join(sql.SQL("coalesce(") + sql.Identifier(c) + sql.SQL(",False)") for c in columns),
                sql.SQL(', ').join(sql.Identifier(c) for c in columns),
                self.__table_name__(table))])
            #q = sql.Composed([q, sql.SQL(' ON CONFLICT (row_number) DO UPDATE SET model_count = greatest({}.model_count, EXCLUDED.model_count)').format(self.__table_name__(table))]) 
            #print(q)
        if returning:
            q = sql.Composed([q,sql.SQL(" RETURNING {}").format(sql.Identifier(returning))])
            return self.exec_and_fetch(q)
        else:
            self.execute(q)

    def create_table_as(self, table, newTableName):
        q = sql.SQL("CREATE TABLE {} AS SELECT * FROM {}").format(
                self.__table_name__(newTableName),
                self.__table_name__(table)
                )
        self.execute(q)
    
    def alter_table_name(self, oldName, newName):
        q = sql.SQL("ALTER TABLE {} RENAME TO {}").format(
                self.__table_name__(oldName),
                self.__table_name__(newName)
                )
        self.execute(q)

    def delete_all_rows(self, table):
        q = sql.SQL("DELETE FROM {}").format(
                self.__table_name__(table)
                )
        self.execute(q)

    def delete_n_rows(self, table, rows):
        #print(self.select_query("SELECT * FROM {} ORDER BY insert_time LIMIT {}".format(table, str(rows))))
        q = sql.SQL("DELETE FROM {} WHERE ctid IN (SELECT ctid FROM {} ORDER BY insert_time LIMIT {})").format(
                self.__table_name__(table),
                self.__table_name__(table),
                sql.SQL(str(rows))
                )
        self.execute(q)

    def persist_view(self, table, view=None):
        if not view:
            view = table + "_v"
        select = f"SELECT * FROM {view}"
        select = self.replace_dynamic_tabs(select)
        self.insert_select(table, select)
    
    # parse whole query and returns all result rows
    def select_query(self, query):
        q = sql.SQL(self.replace_dynamic_tabs(query))
        #print(q)
        return self.exec_and_fetch_all(q)
    
    # when all is set then all result rows (not only the first one) get returned
    def select(self, table, columns, where = None, all = False):
        q = sql.SQL("SELECT {} FROM {}").format(
                    sql.SQL(', ').join(sql.SQL(c) for c in columns),
                    self.__table_name__(table)
                    )
        if where:
            q = sql.Composed([q,sql.SQL(" WHERE {}").format(sql.SQL(' AND ').join(map(sql.SQL,where)))])
        
        if all:
            return self.exec_and_fetch_all(q)
        else:
            return self.exec_and_fetch(q)

    def create_select(self,table,ass_sql):
        q = sql.SQL("CREATE TABLE {} AS {}").format(
                    self.__table_name__(table),
                    sql.SQL(ass_sql)
                    )
        self.execute_ddl(q)

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
    
    # the model count is updated if the row already exists in the table - the rows get returned from the select
    # also every column has to be checked against NULL because NULL is not a distinct value
    def update_select_model_count(self, table, select, columns):
        q = sql.SQL("UPDATE {} SET model_count = subquery.model_count FROM ({}) AS subquery WHERE {}").format(
                    self.__table_name__(table),
                    sql.SQL(select),
                    sql.SQL(" AND ").join([sql.SQL("({}.{} IS NULL OR {}.{} = subquery.{})").format(
                        self.__table_name__(table),
                        sql.Identifier(c),
                        self.__table_name__(table),
                        sql.Identifier(c), sql.Identifier(c)
                        ) for c in columns])
                    )
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

    def execute_select(self, q):
        query = sql.SQL(q)
        return self.exec_and_fetch(q)

class DBAdmin(DB):
    def killall(self, app_name):
        q = "select pg_kill_all_sessions(%s,%s)"
        self.execute(sql.SQL(q),[self._db_name,app_name])
        """
        q = "select pg_terminate_backend(pid) from pg_stat_activity where pid <> pg_backend_pid() and datname = %s"
        if app_name:
            q += " and application_name=%s"
            self.execute(sql.SQL(q),[self._db_name,app_name])
        else:
            self.execute(sql.SQL(q),[self._db_name])
        """

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
