# -*- coding: future_fstrings -*-
import logging
import os
import signal
import threading
import math
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

from dpdb.reader import TwReader
from dpdb.db import DB

import numpy as np
from random import randint

import argparse
logger = logging.getLogger(__name__)

args = SimpleNamespace()
args.general = {
    "--limit-result-rows": dict(
        type=int,
        dest="limit_result_rows",
        help="Limit number of result rows per table. Can be a list (useful with --iterations) then every item of the list is used roughly the same amount of times.",
        nargs="*"
    ),
    "--randomize": dict(
        #action="store_true",
        dest="randomize_rows",
        choices=["order", "offset", "noview"],
        help="Switch between randomize methods"
    ),
    "--candidate-store": dict(
        dest="candidate_store",
        help="How to store/use candidate results",
        choices=["cte","subquery","table"],
        default="subquery"
    ),
    "--limit-introduce": dict(
        type=int,
        dest="limit_introduce",
        help="Limit number of result rows when introducing a new node"
    ),
    "--lower-cap": dict(
        type=int,
        dest="lower_cap",
        default=0,
        help="Lower Cap for activating the limit in solve. If lowerCap == 0 it will be ignored."
    ),
    "--upper-cap": dict(
        type=int,
        dest="upper_cap",
        default=0,
        help="Upper Cap for a maximum of rows per step. If upperCap == 0 it will be ignored."
    ),
    "--table-row-limit": dict(
        type=int,
        dest="table_row_limit",
        default=0,
        help="Max Amount of Rows in table - after this limit is reached the model_count still gets updated but no new rows are inserted. If limit = 0 the limit will be ignored."
    )
    #"--no-view": dict(
        #action="store_true",
        #dest="no_view",
        #help="If set the rows are not generated via a view but only with random numbers directly in the select."
    #)
}

args.specific = {}

def node2tab(node):
    return f"td_node_{node.id}"

def node2tab_alias(node):
    return f"t{node.id}"

def var2tab(node, var):
    if node.needs_introduce(var):
        return "introduce"
    else:
        return node2tab(node.vertex_children(var)[0])

def var2tab_alias(node, var):
    if node.needs_introduce(var):
        return f"i{var}"
    else:
        return node2tab_alias(node.vertex_children(var)[0])

def var2col(var):
    return f"v{var}"

def var2tab_col(node, var, alias=True):
    if node.needs_introduce(var):
        if alias:
            return "{}.val {}".format(var2tab_alias(node, var),var2col(var))
        else:
            return "{}.val".format(var2tab_alias(node, var))
    else:
        return "{}.{}".format(var2tab_alias(node, var),var2col(var))

def bitmap_index_values(node, v):
    if node.needs_introduce(v):
        return "coalesce({}.val, False)::int::bit".format(var2tab_alias(node, v))
    return "coalesce({}.{}, False)::int::bit".format(var2tab_alias(node,v), var2col(v))

class Problem(object):
    id = None
    td = None
    
    # minimum amount of results that is needed to activate the limit
    LIMIT_RESULT_ROWS_LOWER_CAP = None
    # maximum amount of results that are selected from the view
    LIMIT_RESULT_ROWS_UPPER_CAP = None
    # max amount of results allowed in a table - after this limit is hit the model_count only gets updated
    # but no new rows are inserted in this table
    TABLE_ROW_LIMIT = None

    def __init__(self, name, pool, lower_cap, upper_cap, table_row_limit, max_worker_threads=12,
            candidate_store="cte", limit_result_rows=None,
            limit_introduce=None,  **kwargs):
        self.name = name
        self.pool = pool
        self.candidate_store = candidate_store
        if limit_result_rows is not None:
            self.limit_result_rows = limit_result_rows[0]
        else:
            self.limit_result_rows = limit_result_rows

        # check if --randomize-rows parameter is set if yes then in solve
        # the rows shouldn't be randomized 
        if "randomize_rows" in kwargs and kwargs["randomize_rows"]:
            self.randomize_rows = kwargs["randomize_rows"]
        else:
            self.randomize_rows = None

        logger.info("Running: " + str(self.randomize_rows))
        logger.info("Limit Result Rows: " + str(limit_result_rows))
        logger.info("Faster: " + str(kwargs["faster"]))
        #print(self.randomize_rows)
        #echo "user Ok"
        #createdb -p $PORT dpdb_pg 
        #psql -p $PORT dpdb_pg <<'EOF'
        #print(self.randomize_rows)
        #else:
            #self.randomize_rows = True
        #if "no_view" in kwargs and kwargs["no_view"]:
            #self.no_view = True
        #else:
            #self.no_view = False
        self.limit_introduce = limit_introduce
        self.max_worker_threads = max_worker_threads
        self.kwargs = kwargs
        self.type = type(self).__name__
        self.db = DB.from_pool(pool)
        self.interrupted = False
        self.TABLE_ROW_LIMIT = table_row_limit
        self.LIMIT_RESULT_ROWS_LOWER_CAP = lower_cap
        self.LIMIT_RESULT_ROWS_UPPER_CAP = upper_cap
        if self.LIMIT_RESULT_ROWS_UPPER_CAP != 0 and self.LIMIT_RESULT_ROWS_LOWER_CAP > self.LIMIT_RESULT_ROWS_UPPER_CAP:
            raise ValueError("Upper Limit must be higher than lower limit")

    # overwrite the following methods (if required)
    def td_node_column_def(self, var):
        pass

    def td_node_extra_columns(self):
        return []

    def candidate_extra_cols(self,node):
        return []

    def assignment_extra_cols(self,node):
        return []

    def group_extra_cols(self,node):
        return []

    # if you overwrite this, make sure to alias the introduced value as "val"
    def introduce(self,node):
        return "SELECT true val UNION ALL SELECT false"

    def join(self,node):
        joins = []
        for v in node.vertices:
            vertex_join = []
            vc = node.vertex_children(v)
            if not vc:
                continue
            fst = vc[0].id
            for j in range(1,len(vc)):
                snd = vc[j].id
                vertex_join.append("t{1}.{0} = t{2}.{0}".format(var2col(v),fst,snd))
                fst = snd
            if vertex_join:
                joins.append(" AND ".join(vertex_join))

        if joins:
            return "WHERE {}".format(" AND ".join(joins))
        else:
            return ""

    def filter(self,node):
        return "WHERE FALSE"

    def prepare_input(self, fname):
        pass

    def setup_extra(self):
        pass

    def before_solve(self):
        pass

    def after_solve(self):
        pass

    def before_solve_node(self, node, db):
        pass

    def after_solve_node(self, node, db):
        pass
    
    # the following methods can be overwritten at your own risk
    def candidates_select(self,node):
        introduce = False
        q = ""

        if any(node.needs_introduce(v) for v in node.vertices):
            # introudce is used to check if new variables are introduced in this call
            introduce = True
            q += "WITH introduce AS ({}) ".format(self.introduce(node))
        
        # attempts to assign a PK to each row - failed because every select of the view gives a different result
        #q += "SELECT ROW_NUMBER() OVER (ORDER BY {}) as row_number, {}".format(
                #",".join([var2tab_alias(node,v) for v in node.vertices]),
                #",".join([var2tab_col(node, v) for v in node.vertices]),
                #)

        
        #q += "SELECT {} as row_number, {}".format(
                #"||".join([bitmap_index_values(node, v) for v in node.vertices]),
                #",".join([var2tab_col(node, v) for v in node.vertices])
                #)

        q += "SELECT {}".format(
                ",".join([var2tab_col(node, v) for v in node.vertices]))

        extra_cols = self.candidate_extra_cols(node)
        if extra_cols:
            q += "{}{}".format(", " if node.vertices else "", ",".join(extra_cols))

        if node.vertices or node.children:
            q += " FROM {}".format(
                    ",".join(set(["{} {}".format(var2tab(node, v), var2tab_alias(node, v)) for v in node.vertices] +
                                 ["{} {}".format(node2tab(n), node2tab_alias(n)) for n in node.children]))
                    )

        if len(node.children) > 1:
            q += " {} ".format(self.join(node))
        
        # the rows are always randomly ordered to avoid skipping a variable - not necessary to achieve randomness therefore left out to achieve better performance
        #q += " ORDER BY RANDOM()"
        
        # limit_introduce is used to be able to set a limit separately for introduce and solve 
        if introduce and self.limit_introduce:
            # in the limit the newly introduced variables are used
            limit = self.limit_introduce / 100
            q += f" LIMIT (SELECT least(Count(*), {self.LIMIT_RESULT_ROWS_UPPER_CAP}) FROM "
            q += "{}".format(",".join(set(["{} {}".format(var2tab(node, v), "limit" + var2tab_alias(node,v)) for v in node.vertices] + ["{} {}".format(node2tab(n), "limit" + node2tab_alias(n)) for n in node.children]))) 
            q += f") * {limit}" 
        
        return q

    def assignment_select(self,node):
        sel_list = ",".join([var2col(v) if v in node.stored_vertices
            else "null::{} {}".format(self.td_node_column_def(v)[1],var2col(v)) for v in node.vertices])
        extra_cols = self.assignment_extra_cols(node)
        if extra_cols:
            sel_list += "{}{}".format(", " if sel_list else "", ",".join(extra_cols))

        candidates_sel = self.candidates_select(node)

        if self.candidate_store == "cte":
            q = f"WITH candidate AS ({candidates_sel}) SELECT {sel_list} FROM candidate"
        elif self.candidate_store == "subquery":
            q = f"SELECT {sel_list} FROM ({candidates_sel}) AS candidate"
            #q = f"SELECT min(row_number) as row_number, {sel_list} FROM ({candidates_sel}) AS candidate"
            #q = f"SELECT row_number as row_number, {sel_list} FROM ({candidates_sel}) AS candidate"
        elif self.candidate_store == "table":
            q = f"SELECT {sel_list} FROM td_node_{node.id}_candidate"
        
        return q

    def assignment_view(self,node,checkLimit=False):
        q = "{} {}".format(self.assignment_select(node),self.filter(node))
        if node.stored_vertices:
            q += " GROUP BY {}".format(",".join([var2col(v) for v in node.stored_vertices]))
            # attempt of assigning a PK
            #q += ", row_number"
        #else:
            #q += " GROUP BY row_number"

        extra_group = self.group_extra_cols(node)
        if extra_group:
            if not node.stored_vertices:
                q += " GROUP BY ";
            else:
                q += ", "
            q += "{}".format(",".join(extra_group))

        if not node.stored_vertices and not extra_group:
            q += " LIMIT 1"
        else:
            if self.randomize_rows == "order":
                q += " ORDER BY RANDOM()"
            if checkLimit == True:
                # to use the limit the amount of rows has to be counted in the limit query
                # therefore the same FROM and WHERE clause has to be used in the subselect
                # in the limit but without the GROUP BY
                fromIndex = q.find("FROM")
                groupByIndex = q.find("GROUP BY")
                if groupByIndex != -1:
                    substr = q[fromIndex:groupByIndex]
                else:
                    substr = q[fromIndex:]
                limit = (list({self.limit_result_rows})[0])/100
                # if upper cap is set select the smaller one from count and cap if not just use count
                if self.LIMIT_RESULT_ROWS_UPPER_CAP != 0:
                    q += f" LIMIT ((SELECT least(Count(*), {self.LIMIT_RESULT_ROWS_UPPER_CAP}) {substr})*{limit})"
                else:
                    q += f" LIMIT ((SELECT Count(*) {substr})*{limit})"
                #print(self.db.select_query(f"SELECT least(Count(*), {self.LIMIT_RESULT_ROWS_UPPER_CAP}) {substr}"))
        return q

    # the following methods should be considered final
    def set_td(self, td):
        self.td = td

    def set_id(self,id):
        self.id = id
        self.db.set_praefix(f"p{self.id}_")

    def setup(self):
        def create_base_tables():
            self.db.create_table("problem", [
                ("id", "SERIAL NOT NULL PRIMARY KEY"),
                ("name", "VARCHAR(255) NOT NULL"),
                ("type", "VARCHAR(32) NOT NULL"),
                ("num_bags", "INTEGER"),
                ("tree_width", "INTEGER"),
                ("num_vertices", "INTEGER"),
                ("setup_start_time", "TIMESTAMP"),
                ("calc_start_time", "TIMESTAMP"),
                ("end_time", "TIMESTAMP")
            ])
            self.db.create_table("problem_option", [
                ("id", "INTEGER NOT NULL REFERENCES PROBLEM(id)"),
                ("type", "VARCHAR(8) NOT NULL DEFAULT 'argument'"),
                ("name", "VARCHAR(255) NOT NULL"),
                ("value", "VARCHAR(255)")
            ])

        def init_problem():
            problem_id = self.db.insert("problem",
                ["name","type","num_bags","tree_width","num_vertices"],
                [self.name,self.type,self.td.num_bags,self.td.tree_width,self.td.num_orig_vertices],"id")[0]
            self.set_id(problem_id)
            logger.info("Created problem with ID %d", self.id)
            
        def drop_tables():
            logger.debug("Dropping tables")
            self.db.drop_table("td_bag")
            self.db.drop_table("td_edge")
            for n in self.td.nodes:
                self.db.drop_table(f"td_node_{n.id}")

        def create_tables():
            logger.debug("Creating tables")
            self.db.create_table("td_node_status", [
                ("node", "INTEGER NOT NULL PRIMARY KEY"),
                ("start_time", "TIMESTAMP"),
                ("end_time", "TIMESTAMP"),
                ("rows", "INTEGER")
            ])
            self.db.create_table("td_edge", [("node", "INTEGER NOT NULL"), ("parent", "INTEGER NOT NULL")])
            self.db.create_table("td_bag", [("bag", "INTEGER NOT NULL"),("node", "INTEGER")])
            
            #for node in self.td.nodes:
                #print(f"{node.id}: {node.vertices}")
                #if node.parent:
                    #print(f"{node.parent.id}: {node.parent.vertices}")
                    #items = set(node.parent.vertices)
                    #constraint_relevant = [i for i in node.vertices if i in items]
                    #print(f"{node.id}: {constraint_relevant} - constraint")
                #print()

            if "parallel_setup" in self.kwargs and self.kwargs["parallel_setup"]:
                workers = {}
                with ThreadPoolExecutor(self.max_worker_threads) as executor:
                    for n in self.td.nodes:
                        e = executor.submit(create_tables_for_node,n,workers)
                        workers[n.id] = e
            else:
                for n in self.td.nodes:
                    create_tables_for_node(n)


        def create_tables_for_node(n, workers = {}):
            if "parallel_setup" in self.kwargs and self.kwargs["parallel_setup"]:
                for c in n.children:
                    if not self.interrupted:
                        workers[c.id].result()
                if self.interrupted:
                    return
                db = DB.from_pool(self.pool)
                db.set_praefix(f"p{self.id}_")
            else:
                db = self.db

            # create all columns and insert null if values are not used in parent
            # this only works in the current version of manual inserts without procedure calls in worker
            # attempt to add timestamp to each row and delete the oldest rows after some time
            #db.create_table_node(f"td_node_{n.id}", [self.td_node_column_def(c) for c in n.vertices] + self.td_node_extra_columns())
            db.create_table(f"td_node_{n.id}", [self.td_node_column_def(c) for c in n.vertices] + self.td_node_extra_columns())
            #db.create_table(f"td_node_{n.id}_temp", [self.td_node_column_def(c) for c in n.vertices] + self.td_node_extra_columns())
            
            # select only the columns that are not null in the table to reduce the amount of columns in the index  
            if n.parent:
                items = set(n.parent.vertices)
                n.constraint_relevant = [i for i in n.vertices if i in items]

                if n.constraint_relevant == []:
                    n.constraint_relevant = n.vertices
            else:
                n.constraint_relevant = n.vertices
            
            # add unique index for the iterative approximation 
            db.add_unique_index(f"td_node_{n.id}", [self.td_node_column_def(c)[0] for c in n.constraint_relevant]) 
            if self.candidate_store == "table":
                db.create_table(f"td_node_{n.id}_candidate", [self.td_node_column_def(c) for c in n.vertices] + self.td_node_extra_columns())
                candidate_view = self.candidates_select(n)
                candidate_view = db.replace_dynamic_tabs(candidate_view)
                db.create_view(f"td_node_{n.id}_candidate_v", candidate_view)
            
            # create view if the values should not be generated randomly in the select
            if self.randomize_rows != "noview":
                ass_view = self.assignment_view(n)
                ass_view = db.replace_dynamic_tabs(ass_view)
                #print(ass_view)
                db.create_view(f"td_node_{n.id}_v", ass_view)
            if "parallel_setup" in self.kwargs and self.kwargs["parallel_setup"]:
                db.close()
            
        def insert_data():
            logger.debug("Inserting problem data")
            self.db.ignore_next_praefix(3)
            self.db.insert("problem_option",("id", "name", "value"),(self.id,"candidate_store",self.candidate_store))
            self.db.insert("problem_option",("id", "name", "value"),(self.id,"limit_result_rows",self.limit_result_rows))
            self.db.insert("problem_option",("id", "name", "value"),(self.id,"randomize_rows",self.randomize_rows))
            for k, v in self.kwargs.items():
                if v:
                    self.db.ignore_next_praefix()
                    self.db.insert("problem_option",("id", "name", "value"),(self.id,k,v))

            for n in self.td.nodes:
                self.db.insert("td_node_status", ["node"],[n.id])
                for v in n.vertices:
                    self.db.insert("td_bag",("bag","node"), (n.id,v))
            for edge in self.td.edges:
                self.db.insert("td_edge",("node","parent"),(edge[1],edge[0]))

        create_base_tables()
        init_problem()
        self.db.ignore_next_praefix()
        self.db.update("problem",["setup_start_time"],["statement_timestamp()"],[f"ID = {self.id}"])
        if "faster" not in self.kwargs or not self.kwargs["faster"]:
            drop_tables()
            create_tables()
            insert_data()

        self.setup_extra()

        self.db.commit()

    def store_cfg(self,cfg):
        for k, v in cfg.items():
            if v:
                self.db.ignore_next_praefix()
                self.db.insert("problem_option",("id", "type", "name", "value"),(self.id,"cfg",k,v))

    
    #first = True
    def solve(self, delete = False):
        self.db.ignore_next_praefix()
        self.db.update("problem",["calc_start_time"],["statement_timestamp()"],[f"ID = {self.id}"])
        self.db.commit()
        
        self.before_solve()
        
        workers = {}
        with ThreadPoolExecutor(self.max_worker_threads) as executor:
            for n in self.td.nodes:
                e = executor.submit(self.node_worker,n,workers, delete)
                workers[n.id] = e

        self.after_solve()
                 
        # create the views new after every iteration to apply the limit again and get new rows for the next iteration
        #if "faster" not in self.kwargs or not self.kwargs["faster"]:
            #res = 0
            #for n in self.td.nodes:
                #self.db.drop_view(f"td_node_{n.id}_v")
                #ass_view = self.assignment_view(n)
                #ass_view = self.db.replace_dynamic_tabs(ass_view)
                #self.db.create_view(f"td_node_{n.id}_v", ass_view)
        
        self.db.ignore_next_praefix()
        self.db.update("problem",["end_time"],["statement_timestamp()"],[f"ID = {self.id}"])
        self.db.commit()
        # check how many rows where generated each iteration
        #print(self.summe)
        #self.first = False
        if "faster" not in self.kwargs or not self.kwargs["faster"]:
            self.db.ignore_next_praefix()
            elapsed = self.db.select("problem",["end_time-calc_start_time","calc_start_time-setup_start_time"],[f"ID = {self.id}"])
            logger.info("Setup time: %s; Calc time: %s", elapsed[1], elapsed[0])

    def interrupt(self):
        self.interrupted = True

    def node_worker(self, node, workers, delete):
        try:
            for c in node.children:
                if not self.interrupted:
                    logger.debug("Node %d waiting for %d", node.id,c.id)
                    workers[c.id].result()

            if self.interrupted:
                logger.info("Node %d interrupted", node.id)
                return node

            db = DB.from_pool(self.pool)
            db.set_praefix(f"p{self.id}_")
            logger.debug("Creating records for node %d", node.id)
            self.solve_node(node,db, delete)
            db.close()
            if not self.interrupted:
                logger.debug("Node %d finished", node.id)
            return node
        except Exception:
            logger.exception("Error in worker thread")
            os.kill(os.getpid(), signal.SIGUSR1)

    summe = 0
    def solve_node(self, node, db, delete):
        if "faster" not in self.kwargs or not self.kwargs["faster"]:
            db.update("td_node_status",["start_time"],["statement_timestamp()"],[f"node = {node.id}"])
            db.commit()

        self.before_solve_node(node, db)
        if self.candidate_store == "table":
            db.persist_view(f"td_node_{node.id}_candidate")
        if "faster" in self.kwargs and self.kwargs["faster"]:
            #print("faster")
            # if limit should be used or not
            if self.limit_result_rows:
                # True tells the assignment_view function that the limit part of the query should be added
                ass_view = self.assignment_view(node, True)
            else:
                ass_view = self.assignment_view(node)
            #print(ass_view)
            ass_view = self.db.replace_dynamic_tabs(ass_view)
            #db.drop_table(f"td_node_{node.id}")
            #print(self.first)
            #if self.first:
            db.create_select(f"td_node_{node.id}", ass_view)
            #else:
                #insertRows = db.select_query(ass_view)
                #print(insertRows)
        else:
            if self.randomize_rows != "noview":
                select = f"SELECT * from td_node_{node.id}_v"
                # also get timestamp to delete oldest rows
                #select = f"SELECT statement_timestamp(), * from td_node_{node.id}_v"
                # this randomize should not be necessary anymore
                #if self.randomize_rows:
                    #select += " ORDER BY RANDOM()"
                if self.limit_result_rows and (node.stored_vertices or self.group_extra_cols(node)):
                    # get number of result rows in table and check if the limit should be applied or not
                    count = db.select(f"td_node_{node.id}_v", ["Count(*)"])
                    count = count[0] 
                    if self.LIMIT_RESULT_ROWS_LOWER_CAP < count:
                        #if self.randomize_rows:
                            #select += " ORDER BY RANDOM()"
                        # if amount of rows is higher than the Cap use the Cap as Limit
                        # to avoid having to much rows to work with
                        if self.LIMIT_RESULT_ROWS_UPPER_CAP != 0 and self.LIMIT_RESULT_ROWS_UPPER_CAP < count:
                            self.summe += self.LIMIT_RESULT_ROWS_UPPER_CAP
                            select += f" LIMIT {self.LIMIT_RESULT_ROWS_UPPER_CAP}"
                        else:
                            limit = (list({self.limit_result_rows})[0])/100
                            if self.randomize_rows == "order":
                                select += f" LIMIT ({count}*{limit})"
                            #self.summe += (count*limit)
                            if self.randomize_rows == "offset":
                                limit = count * limit
                                offset = randint(0, round(count*((100-list({self.limit_result_rows})[0])/100)))
                                #print("Count: " + str(count))
                                #print("Offset: " + str(offset))
                                select += f" OFFSET {offset} FETCH NEXT {limit} ROWS ONLY"
                    else:
                        self.summe += count
                #count the rows in the table
                #print(select)
                countTable = db.select(f"td_node_{node.id}", ["Count(*)"])
                countTable = countTable[0]
                #print(countTable)
                #print(select)
                # if count is too high then the model_count for the existing rows gets updated but no new rows are inserted
                if self.TABLE_ROW_LIMIT == 0 or countTable < self.TABLE_ROW_LIMIT:
                    db.insert_select(f"td_node_{node.id}", db.replace_dynamic_tabs(select), True, [self.td_node_column_def(c)[0] for c in node.constraint_relevant])
                else:
                    #print("update")
                    db.update_select_model_count(f"td_node_{node.id}", db.replace_dynamic_tabs(select), [self.td_node_column_def(c)[0] for c in node.constraint_relevant]) 
                # refresh for materialized view 
                #db.refresh_mat_view(f"td_node_{node.id}_v")
            # if the values should be generated randomly in the select
            else:
                # build up the select according to the view
                sel_list = ",".join([var2col(v) if v in node.stored_vertices else "null::{} {}".format(self.td_node_column_def(v)[1],var2col(v)) for v in node.vertices])
                sel_list += ", sum(model_count) as model_count"
                where_filter = self.filter(node)
                group_by = ""
                if node.stored_vertices:
                    group_by = "{}".format(",".join([var2col(v) for v in node.stored_vertices]))
                #col = len(node.vertices)
                col = len(node.vertices)
                limit = (list({self.limit_result_rows})[0])/100
                #print(self.LIMIT_RESULT_ROWS_LOWER_CAP)
                # figure out how many rows should be generated
                #if self.LIMIT_RESULT_ROWS_LOWER_CAP < (2**col):
                    #if self.LIMIT_RESULT_ROWS_UPPER_CAP != 0 and self.LIMIT_RESULT_ROWS_UPPER_CAP < ((2**col)):
                        #rows = self.LIMIT_RESULT_ROWS_UPPER_CAP
                    #else:
                        #rows = ((2**(col/2))*limit)
                        #rows = ((2**col)*limit)
                #else:
                    #rows = (2**col)
                    #rows = (2**self.LIMIT_RESULT_ROWS_LOWER_CAP)
                #print(rows)
                #self.summe += rows
                #print("noview -----------------------------------------------------------------")
                select = db.select_random(math.floor((2**col)), col, self, node, sel_list, where_filter, group_by)
                db.insert_list(f"td_node_{node.id}", select, col, [self.td_node_column_def(c)[0] for c in node.constraint_relevant])
                #print(self.summe)
        if self.interrupted:
            return
        self.after_solve_node(node, db)
        if "faster" not in self.kwargs or not self.kwargs["faster"]:
            row_cnt = db.last_rowcount
            db.update("td_node_status",["end_time","rows"],["statement_timestamp()",str(row_cnt)],[f"node = {node.id}"])
        db.commit()

