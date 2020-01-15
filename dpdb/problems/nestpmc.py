# -*- coding: future_fstrings -*-
import logging
import subprocess
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

#from nesthdb.solve import nesthdb
from dpdb.abstraction import Abstraction
from dpdb.problem import *
from dpdb.reader import CnfReader
from dpdb.writer import StreamWriter, FileWriter, normalize_cnf
from .sat_util import *

logger = logging.getLogger(__name__)

def var2col2(node,var):
    if node.is_minor(var):
        return "{}.val".format(var2tab_alias(node, var))
    else:
        return f"v{var}"

def lit2var2 (node,lit):
    return var2col2(node,abs(lit))

def lit2expr2 (node,lit):
    if lit > 0:
        return lit2var2(node,lit)
    else:
        return "NOT {}".format(lit2var2(node,lit))

class NestPmc(Problem):
    @classmethod
    def keep_cfg(cls):
        return ["asp_encodings","sat_solver"]

    def __init__(self, name, pool, max_solver_threads=12, inner_vars_threshold=0, store_formula=False, **kwargs):
        super().__init__(name, pool, **kwargs)
        self.store_formula = store_formula
        #self.abstr = Abstraction(self.sub_procs, **kwargs)
        #self.interrupt_handler.append(self.abstr.interrupt)
        self.max_solver_threads = max_solver_threads
        self.store_all_vertices = True
        self.inner_vars_threshold = inner_vars_threshold

    def td_node_column_def(self,var):
        return td_node_column_def(var)
        
    def td_node_extra_columns(self):
        return [("model_count","NUMERIC")]

    def candidate_extra_cols(self,node):
        return ["{}::numeric AS model_count".format(
                " * ".join(set([var2cnt(node,v) for v in node.vertices] +
                               [node2cnt(n) for n in node.children])) if node.vertices or node.children else "1"
                )]

    def assignment_extra_cols(self,node):
        return ["sum(model_count)::numeric AS model_count"]

    def filter(self,node):
        f = filter(self.var_clause_dict, node)
        if len(node.minor_vertices) > 0 and len(node.all_vertices) - len(node.vertices) <= self.inner_vars_threshold:
            if f == "":
                f = "WHERE "
            else:
                f += " AND "
            candidate_tabs = ",".join(["{} {}".format(var2tab(node, v), var2tab_alias(node, v)) for v in node.minor_vertices])
            f += f"EXISTS (WITH introduce AS ({self.introduce(node)}) SELECT 1 FROM {candidate_tabs} WHERE "
            cur_cl = covered_clauses(self.var_clause_dict,node.all_vertices)
            f += "({0})".format(") AND (".join(
                [" OR ".join([lit2expr2(node,c) for c in clause]) for clause in cur_cl]
            ))
            f += ")"
        return f
        #return filter(self.var_clause_dict, node)

    def setup_extra(self):
        def create_tables():
            self.db.ignore_next_praefix()
            self.db.create_table("problem_pmc", [
                ("id", "INTEGER NOT NULL PRIMARY KEY REFERENCES PROBLEM(id)"),
                ("num_vars", "INTEGER NOT NULL"),
                ("num_clauses", "INTEGER NOT NULL"),
                ("model_count", "NUMERIC")
            ])
            if "faster" not in self.kwargs or not self.kwargs["faster"]:
                self.db.create_table("projected_vars", [
                    ("id", "INTEGER NOT NULL REFERENCES PROBLEM(id)"),
                    ("var", "INTEGER NOT NULL")
                ])
                self.db.create_pk("projected_vars",["id","var"])

        def insert_data():
            self.db.ignore_next_praefix()
            self.db.insert("problem_pmc",("id","num_vars","num_clauses"),
                (self.id, self.num_vars, self.num_clauses))
            if "faster" not in self.kwargs or not self.kwargs["faster"]:
                for p in self.projected:
                    self.db.insert("projected_vars",("id", "var"),(self.id, p))
                self.db.ignore_next_praefix()
                self.db.insert("problem_option",("id", "name", "value"),(self.id,"store_formula",self.store_formula))
                if self.store_formula:
                    store_clause_table(self.db, self.clauses)

        create_tables()
        insert_data()

    def prepare_input(self, fname):
        input = CnfReader.from_file(fname)
        self.num_vars = input.num_vars
        self.num_clauses = input.num_clauses
        self.clauses = input.clauses
        self.projected = list(input.projected)
        self.var_clause_dict = defaultdict(set)
        #logger.debug("{} vars, {}={} clauses", input.num_vars, input.num_clauses, len(input.clauses))
        num_vars, edges, adj = cnf2primal(input.num_vars, input.clauses, self.var_clause_dict, True)
        return self.abstr.abstract(num_vars,edges,adj,self.projected)

    def set_recursive(self,func, depth):
        self.rec_func = func
        self.depth = depth

    def set_input(self,num_vars,num_clauses,projected,non_nested,var_clause_dict):
        self.num_vars = num_vars
        self.num_clauses = num_clauses
        #self.clauses = clauses
        self.projected = projected
        self.non_nested = non_nested
        self.var_clause_dict = var_clause_dict

    def after_solve_node(self, node, db):
        cols = [var2col(c) for c in node.vertices]
        executor = ThreadPoolExecutor(self.max_solver_threads)
        futures = []
        clauses = covered_clauses(self.var_clause_dict, node.all_vertices)
        for r in db.select_all(f"td_node_{node.id}",cols):
            if not self.interrupted:
                if len(node.all_vertices) - len(node.vertices) > self.inner_vars_threshold: # only if there is an inner problem to solve
                    futures.append(executor.submit(self.solve_sat, node, db, cols, r, clauses))
        for future in as_completed(futures):
            if future.exception():
                raise future.exception()
        executor.shutdown(wait=True)

    def solve_sat(self, node, db, cols, vals, covered_clauses):
        if self.interrupted:
            return
        try:
            where = []
            num_vars = len(node.all_vertices)
            extra_clauses = []
            clauses = list(covered_clauses)
            for i,v in enumerate(vals):
                if v != None:
                    where.append("{} = {}".format(cols[i],v))
                    n = node.vertices[i]
                    if v:
                        clauses.append([n])
                        extra_clauses.append(n)
                    else:
                        clauses.append([n*(-1)])
                        extra_clauses.append(n*(-1))
            #actually, it is probably better to leave it like that such that one could use maybe #sat instead of pmc?
            projected = self.projected.intersection(node.all_vertices) - set(node.vertices)
            non_nested = self.non_nested.intersection(node.all_vertices) - set(node.vertices)
            logger.info(f"Problem {self.id}: Calling recursive for bag {node.id}: {num_vars} {len(clauses)} {len(projected)}")
            sat = self.rec_func(node.all_vertices,clauses,non_nested,projected,self.depth+1,**self.kwargs)
            if not self.interrupted:
                db.update(f"td_node_{node.id}",["model_count"],["model_count * {}::numeric".format(sat)],where)
                db.commit()
        except Exception as e:
            raise e

    def after_solve(self):
        if self.interrupted:
            return
        root_tab = f"td_node_{self.td.root.id}"
        sum_count = self.db.replace_dynamic_tabs(f"(select coalesce(sum(model_count),0) from {root_tab})")
        self.db.ignore_next_praefix()
        self.model_count = self.db.update("problem_pmc",["model_count"],[sum_count],[f"ID = {self.id}"],"model_count")[0]
        logger.info("Problem has %d models", self.model_count)

def var2cnt(node,var):
    if node.needs_introduce(var):
        return "1"
    else:
        return "{}.model_count".format(var2tab_alias(node,var))

def node2cnt(node):
    return "{}.model_count".format(node2tab_alias(node))

args.specific[NestPmc] = dict(
    help="Solve nested PMC instances",
    options={
        "--store-formula": dict(
            dest="store_formula",
            help="Store formula in database",
            action="store_true",
        ),
        "--projected-size": dict(
            dest="projected_size",
            help="Size of projection to be generated for abstraction",
            type=int,
            default=8
        ),
        "--asp-timeout": dict(
            dest="asp_timeout",
            help="Timeout in seconds to find abstraction",
            type=int,
            default=30
        )
    }
)
