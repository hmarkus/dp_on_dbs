# -*- coding: future_fstrings -*-
import logging
import subprocess
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from dpdb.abstraction import Abstraction
from dpdb.problem import *
from dpdb.reader import CnfReader
from dpdb.writer import StreamWriter, FileWriter
from .sat_util import *

logger = logging.getLogger(__name__)

class PmcExt(Problem):
    @classmethod
    def keep_cfg(cls):
        return ["asp_encodings","sat_solver"]

    def __init__(self, name, pool, max_solver_threads=12, store_formula=False, **kwargs):
        super().__init__(name, pool, **kwargs)
        self.store_formula = store_formula
        self.abstr = Abstraction(**kwargs)
        self.max_solver_threads = max_solver_threads
        self.store_all_vertices = True

    def td_node_column_def(self,var):
        return td_node_column_def(var)
        
    def td_node_extra_columns(self):
        return [("model_count","NUMERIC")]

    def candidate_extra_cols(self,node):
        return ["{} AS model_count".format(
                " * ".join(set([var2cnt(node,v) for v in node.vertices] +
                               [node2cnt(n) for n in node.children])) if node.vertices or node.children else "1"
                )]

    def assignment_extra_cols(self,node):
        return ["sum(model_count) AS model_count"]

    def filter(self,node):
        return ""

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

        num_vars, edges, adj = cnf2primal(input.num_vars, input.clauses, self.var_clause_dict, True)
        return self.abstr.abstract(num_vars,edges,adj,self.projected)

    def after_solve_node(self, node, db):
        cols = [var2col(c) for c in node.vertices]
        executor = ThreadPoolExecutor(self.max_solver_threads)
        futures = []
        for r in db.select_all(f"td_node_{node.id}",cols):
            futures.append(executor.submit(self.solve_sat, node, db, cols, r))
        for future in as_completed(futures):
            if future.exception():
                raise future.exception()
        executor.shutdown(wait=True)

    def solve_sat(self, node, db, cols, vals):
        try:
            where = []
            orig_vars = self.abstr.orig_vertices(node.vertices)
            covered_vars = self.abstr.abstracted_vertices(orig_vars) + orig_vars
            num_vars = len(covered_vars)
            clauses = covered_clauses(self.var_clause_dict, covered_vars)
            extra_clauses = []
            for i,v in enumerate(vals):
                if v != None:
                    where.append("{} = {}".format(cols[i],v))
                    n = self.abstr.orig_vertex(node.vertices[i])
                    if v:
                        clauses.append([n])
                        extra_clauses.append(n)
                    else:
                        clauses.append([n*(-1)])
                        extra_clauses.append(n*(-1))
            sat = self.abstr.solve_external(self.num_vars,clauses,extra_clauses,self.projected)
            db.update(f"td_node_{node.id}",["model_count"],["model_count * {}".format(sat)],where)
        except Exception as e:
            raise e

    def after_solve(self):
        root_tab = f"td_node_{self.td.root.id}"
        sum_count = self.db.replace_dynamic_tabs(f"(select coalesce(sum(model_count),0) from {root_tab})")
        self.db.ignore_next_praefix()
        model_count = self.db.update("problem_pmc",["model_count"],[sum_count],[f"ID = {self.id}"],"model_count")[0]
        logger.info("Problem has %d models", model_count)

def var2cnt(node,var):
    if node.needs_introduce(var):
        return "1"
    else:
        return "{}.model_count".format(var2tab_alias(node,var))

def node2cnt(node):
    return "{}.model_count".format(node2tab_alias(node))

args.specific[PmcExt] = dict(
    help="Solve PMC instances using external SAT solver",
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
