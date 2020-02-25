# -*- coding: future_fstrings -*-
import logging
from collections import defaultdict

from dpdb.reader import CnfReader
from dpdb.problem import *
from .sat_util import *

logger = logging.getLogger(__name__)

class SharpSat_Analysis(Problem):

    def __init__(self, name, pool, store_formula=False, **kwargs):
        super().__init__(name, pool, **kwargs)
        self.store_formula = store_formula

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
        return filter(self.var_clause_dict, node)

    def before_solve_statistics(self,node):
        clauses = set()
        for v in node.vertices:
            clauses.update(self.var_clause_dict[v])
        logger.info("Node %d has %d upper clauses", node.id, len(clauses))

    def prepare_input(self, fname):
        input = CnfReader.from_file(fname)
        self.num_vars = input.num_vars
        self.num_clauses = input.num_clauses
        self.clauses = input.clauses
        self.var_clause_dict = defaultdict(set)

        return cnf2primal(input.num_vars, input.clauses, self.var_clause_dict)

    def setup_extra(self):
        def create_tables():
            self.db.ignore_next_praefix()
            self.db.create_table("problem_sharpsat", [
                ("id", "INTEGER NOT NULL PRIMARY KEY REFERENCES PROBLEM(id)"),
                ("num_vars", "INTEGER NOT NULL"),
                ("num_clauses", "INTEGER NOT NULL"),
                ("model_count", "NUMERIC")
            ])

        def insert_data():
            self.db.ignore_next_praefix()
            self.db.insert("problem_sharpsat",("id","num_vars","num_clauses"),
                (self.id, self.num_vars, self.num_clauses))
            if "faster" not in self.kwargs or not self.kwargs["faster"]:
                self.db.ignore_next_praefix()
                self.db.insert("problem_option",("id", "name", "value"),(self.id,"store_formula",self.store_formula))
                if self.store_formula:
                    store_clause_table(self.db, self.clauses)

        create_tables()
        insert_data()

    def after_solve(self):
        root_tab = f"td_node_{self.td.root.id}"
        sum_count = self.db.replace_dynamic_tabs(f"(select coalesce(sum(model_count),0) from {root_tab})")
        self.db.ignore_next_praefix()
        model_count = self.db.update("problem_sharpsat",["model_count"],[sum_count],[f"ID = {self.id}"],"model_count")[0]
        logger.info("Problem has %d models", model_count)

def var2cnt(node,var):
    if node.needs_introduce(var):
        return "1"
    else:
        return "{}.model_count".format(var2tab_alias(node,var))

def node2cnt(node):
    return "{}.model_count".format(node2tab_alias(node))

args.specific[SharpSat_Analysis] = dict(
    help="Solve #SAT instances (count number of models)",
    aliases=["#sat_analysis"],
    options={
        "--store-formula": dict(
            dest="store_formula",
            help="Store formula in database",
            action="store_true",
        )
    }
)

