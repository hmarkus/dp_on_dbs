# -*- coding: future_fstrings -*-
import logging
from collections import defaultdict

from dpdb.problem import *
from dpdb.reader import CnfReader
from .sat_util import *

logger = logging.getLogger(__name__)

class Pmc(Problem):

    def __init__(self, name, pool, store_formula=False, **kwargs):
        super().__init__(name, pool, **kwargs)
        self.store_formula = store_formula

    def td_node_column_def(self,var):
        return td_node_column_def(var)
        
    def filter(self,node):
        return filter(self.var_clause_dict, node)

    def setup_extra(self):
        def create_tables():
            self.db.ignore_next_praefix()
            self.db.create_table("problem_pmc", [
                ("id", "INTEGER NOT NULL PRIMARY KEY REFERENCES PROBLEM(id)"),
                ("num_vars", "INTEGER NOT NULL"),
                ("num_clauses", "INTEGER NOT NULL"),
                ("model_count", "NUMERIC")
            ])

        def insert_data():
            self.db.ignore_next_praefix()
            self.db.insert("problem_pmc",("id","num_vars","num_clauses"),
                (self.id, self.num_vars, self.num_clauses))
            if "faster" not in self.kwargs or not self.kwargs["faster"]:
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
        self.projected = input.projected
        self.var_clause_dict = defaultdict(set)

        num_vars, edges = cnf2primal(input.num_vars, input.clauses, self.var_clause_dict)
        # Create clique over projected variables
        for a in self.projected:
            for b in self.projected:
                if a < b:
                    edges.add((a,b))
        return (num_vars, edges)

    def after_solve(self):
        root_tab = f"td_node_{self.td.root.id}"
        projected_cols = ", ".join([f"v{p}" for p in self.projected])
        sum_count = self.db.replace_dynamic_tabs(f"(select count(*) from (select distinct {projected_cols} from {root_tab}) as projected)")
        self.db.ignore_next_praefix()
        model_count = self.db.update("problem_pmc",["model_count"],[sum_count],[f"ID = {self.id}"],"model_count")[0]
        logger.info("Problem has %d models", model_count)

    def get_root(self, bags, adj, htd_root):
        def is_valid(bag):
            for p in self.projected:
                if p not in bag:
                    return False
            return True

        wl = [htd_root]
        visited = set([htd_root])
        for n in wl:
            if is_valid(bags[n]):
                return n
            else:
                for c in adj[n]:
                    if not c in visited:
                        visited.add(c)
                        wl.append(c)

        return htd_root

def var2cnt(node,var):
    if node.needs_introduce(var):
        return "1"
    else:
        return "{}.model_count".format(var2tab_alias(node,var))

def node2cnt(node):
    return "{}.model_count".format(node2tab_alias(node))

args.specific[Pmc] = dict(
    help="Solve PMC instances",
    options={
        "--store-formula": dict(
            dest="store_formula",
            help="Store formula in database",
            action="store_true",
        )
    }
)
