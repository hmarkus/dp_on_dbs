# -*- coding: future_fstrings -*-
import logging
import os
import signal
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

from dpdb.reader import TwReader
from dpdb.db import DB

logger = logging.getLogger(__name__)

args = SimpleNamespace()
args.general = {
    "--limit-result-rows": dict(
        type=int,
        dest="limit_result_rows",
        help="Limit number of result rows per table"
    ),
    "--randomize-rows": dict(
        action="store_true",
        dest="randomize_rows",
        help="Randomize rows (useful with --limit-result-rows)"
    ),
    "--candidate-store": dict(
        dest="candidate_store",
        help="How to store/use candidate results",
        choices=["cte","subquery","table"],
        default="subquery"
    )
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

class Problem(object):
    id = None
    td = None
    sample = None
    values = None

    def __init__(self, name, pool, max_worker_threads=12,
            candidate_store="cte", limit_result_rows=None,
            randomize_rows=False, **kwargs):
        self.name = name
        self.pool = pool
        self.candidate_store = candidate_store
        self.limit_result_rows = limit_result_rows
        self.randomize_rows = randomize_rows
        self.max_worker_threads = max_worker_threads
        self.kwargs = kwargs
        self.type = type(self).__name__
        self.db = DB.from_pool(pool)
        self.interrupted = False

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
        q = ""

        if any(node.needs_introduce(v) for v in node.vertices):
            q += "WITH introduce AS ({}) ".format(self.introduce(node))

        q += "SELECT {}".format(
                ",".join([var2tab_col(node, v) for v in node.vertices]),
                )

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
        elif self.candidate_store == "table":
            q = f"SELECT {sel_list} FROM td_node_{node.id}_candidate"

        return q

    def assignment_view(self,node):
        filter = self.filter(node)
        q = "{} {}".format(self.assignment_select(node),filter)

        if self.sample:
            first = True
            for s, v in zip(self.sample, self.values):
                if set(s).issubset(node.vertices):
                    if not filter and first:
                        q += " WHERE NOT "
                        first = False
                    else:
                        q += " AND NOT "
                    q += " AND NOT ".join([" v{} = {}".format(vs, vv) for vs, vv in zip(s,v)])
                    #print(q)

        if node.stored_vertices:
            q += " GROUP BY {}".format(",".join([var2col(v) for v in node.stored_vertices]))

        extra_group = self.group_extra_cols(node)
        if extra_group:
            if not node.stored_vertices:
                q += " GROUP BY ";
            else:
                q += ", "
            q += "{}".format(",".join(extra_group))

        if not node.stored_vertices and not extra_group:
            q += " LIMIT 1"
        #print(q)
        return q

    # the following methods should be considered final
    def set_td(self, td):
        self.td = td

    def set_id(self,id):
        self.id = id
        self.db.set_praefix(f"p{self.id}_")

    def set_sample(self, sample, values):
        self.sample = sample
        self.values = values

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
            #vals = "{}".format(",".join(["i" + str(c) + ".val" for c in range(1, self.td.num_orig_vertices+1)]))
            #from_statement = "{}".format(", ".join(["introduce i" + str(c) for c in range(1, self.td.num_orig_vertices+1)]))
            #print(from_statement)
            #self.db.create_table('globalfilter', [self.td_node_column_def(c) for c in range(1,self.td.num_orig_vertices+1)])
            #select = "SELECT * FROM (WITH introduce AS ({}) SELECT {} FROM {}) AS truthtable".format(self.introduce(None), vals, from_statement)
            #self.db.insert_select('globalfilter', select)

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
             
            #for c in range(1,self.td.num_orig_vertices+1):
                #print(c)
                #print(self.td_node_column_def(c))
            #self.db.create_table('globalfilter', [self.td_node_column_def(c) for c in range(1,self.td.num_orig_vertices+1)])
            #select = "SELECT * FROM (WITH introduce AS ({}) SELECT i1.val, i2.val, i3.val, i4.val FROM introduce i1, introduce i2, introduce i3, introduce i4) AS truthtable".format(self.introduce(None))
            #self.db.insert_select('globalfilter', select)

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
            db.create_table(f"td_node_{n.id}", [self.td_node_column_def(c) for c in n.vertices] + self.td_node_extra_columns())
            if self.candidate_store == "table":
                db.create_table(f"td_node_{n.id}_candidate", [self.td_node_column_def(c) for c in n.vertices] + self.td_node_extra_columns())
                candidate_view = self.candidates_select(n)
                candidate_view = db.replace_dynamic_tabs(candidate_view)
                db.create_view(f"td_node_{n.id}_candidate_v", candidate_view)
            ass_view = self.assignment_view(n)
            ass_view = db.replace_dynamic_tabs(ass_view)
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

    def solve(self):
        self.db.ignore_next_praefix()
        self.db.update("problem",["calc_start_time"],["statement_timestamp()"],[f"ID = {self.id}"])
        self.db.commit()

        self.before_solve()

        workers = {}

        with ThreadPoolExecutor(self.max_worker_threads) as executor:
            for n in self.td.nodes:
                e = executor.submit(self.node_worker,n,workers)
                workers[n.id] = e

        self.after_solve()

        self.db.ignore_next_praefix()
        self.db.update("problem",["end_time"],["statement_timestamp()"],[f"ID = {self.id}"])
        self.db.commit()
        if "faster" not in self.kwargs or not self.kwargs["faster"]:
            self.db.ignore_next_praefix()
            elapsed = self.db.select("problem",["end_time-calc_start_time","calc_start_time-setup_start_time"],[f"ID = {self.id}"])
            logger.info("Setup time: %s; Calc time: %s", elapsed[1], elapsed[0])
        self.db.close()

    def interrupt(self):
        self.interrupted = True

    def node_worker(self, node, workers):
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
            self.solve_node(node,db)
            db.close()
            if not self.interrupted:
                logger.debug("Node %d finished", node.id)
            return node
        except Exception:
            logger.exception("Error in worker thread")
            os.kill(os.getpid(), signal.SIGUSR1)

    def solve_node(self, node, db):
        #print(db.select_query("SELECT * FROM globalfilter"))
        #print(db.select_query("SELECT v1 FROM globalfilter"))
        if "faster" not in self.kwargs or not self.kwargs["faster"]:
            db.update("td_node_status",["start_time"],["statement_timestamp()"],[f"node = {node.id}"])
            db.commit()
        #logger.info(node.vertices)
        self.before_solve_node(node, db)
        if self.candidate_store == "table":
            db.persist_view(f"td_node_{node.id}_candidate")
        if "faster" in self.kwargs and self.kwargs["faster"]:
            ass_view = self.assignment_view(node)
            ass_view = self.db.replace_dynamic_tabs(ass_view)
            db.create_select(f"td_node_{node.id}", ass_view)
        else:
            select = f"SELECT * from td_node_{node.id}_v"
            if self.randomize_rows:
                select += " ORDER BY RANDOM()"
            if self.limit_result_rows and (node.stored_vertices or self.group_extra_cols(node)):
                select += f" LIMIT {self.limit_result_rows}"
            db.insert_select(f"td_node_{node.id}", db.replace_dynamic_tabs(select))
            #for s in self.sample:
                #if set(s).issubset(set(node.vertices)):
                	#print(node.vertices)
                	#print(db.select_query(select))
            #print(db.select_query(f"SELECT * from td_node_{node.id}"))
        if self.interrupted:
            return
        self.after_solve_node(node, db)
        if "faster" not in self.kwargs or not self.kwargs["faster"]:
            row_cnt = db.last_rowcount
            db.update("td_node_status",["end_time","rows"],["statement_timestamp()",str(row_cnt)],[f"node = {node.id}"])
        db.commit()

