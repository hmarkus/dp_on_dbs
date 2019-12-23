# -*- coding: future_fstrings -*-
import logging
import subprocess
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from dpdb.problem import *
from dpdb.reader import CnfReader
from dpdb.writer import StreamWriter, FileWriter
from .sat_util import *

logger = logging.getLogger(__name__)

class PmcExt(Problem):
    def __init__(self, name, pool, sat_solver_path, sat_solver_seed_arg=None, preprocessor_path=None, preprocessor_args=None, max_solver_threads=12, store_formula=False, **kwargs):
        super().__init__(name, pool, **kwargs)
        self.store_formula = store_formula
        self.sat_solver = [sat_solver_path]
        if sat_solver_seed_arg:
            self.sat_solver.append(sat_solver_seed_arg)
            self.sat_solver.append(str(kwargs["runid"]))
        if preprocessor_path:
            self.preprocessor = [preprocessor_path]
            if preprocessor_args:
                self.preprocessor.extend(preprocessor_args.split(' '))
        else:
            self.preprocessor = None

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
        self.projected = input.projected
        self.var_clause_dict = defaultdict(set)

        num_vars, edges, adj = cnf2primal(input.num_vars, input.clauses, self.var_clause_dict, True)
        proj_out = set(range(1,self.num_vars+1)) - set(self.projected)
        self.mg = MinorGraph(adj, proj_out)
        self.mg.abstract()
        self.mg.add_cliques()
        num_vars = len(self.projected)
        return num_vars, self.mg.edges

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
            clauses = list(self.clauses)
            extra_clauses = []
            for i,v in enumerate(vals):
                if v != None:
                    where.append("{} = {}".format(cols[i],v))
                    n = self.mg.orig_node(node.vertices[i])
                    if v:
                        clauses.append([n])
                        extra_clauses.append(n)
                    else:
                        clauses.append([n*(-1)])
                        extra_clauses.append(n*(-1))
            logger.debug("Calling SAT solver with {}".format(extra_clauses))
            maybe_sat = True
            num_vars = self.num_vars
            if self.preprocessor:
                ppmc = subprocess.Popen(self.preprocessor,stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
                StreamWriter(ppmc.stdin).write_cnf(self.num_vars,clauses)
                ppmc.stdin.close()
                input = CnfReader.from_stream(ppmc.stdout,silent=True)
                ppmc.wait()
                ppmc.stdout.close()
                maybe_sat = input.maybe_sat
                num_vars = input.num_vars
                clauses = input.clauses
            if maybe_sat:
                psat = subprocess.Popen(self.sat_solver,stdin=subprocess.PIPE, stdout=subprocess.PIPE)
                StreamWriter(psat.stdin).write_cnf(num_vars,clauses)
                psat.stdin.close()
                sat = 1 if psat.stdout.readline().decode().rstrip() == "s SATISFIABLE" else 0
                psat.wait()
                psat.stdout.close()
            else:
                sat = 0
            db.update(f"td_node_{node.id}",["model_count"],["model_count * {}".format(sat)],where)
        except Exception as e:
            raise e

    def after_solve(self):
        root_tab = f"td_node_{self.td.root.id}"
        sum_count = self.db.replace_dynamic_tabs(f"(select coalesce(sum(model_count),0) from {root_tab})")
        self.db.ignore_next_praefix()
        model_count = self.db.update("problem_pmc",["model_count"],[sum_count],[f"ID = {self.id}"],"model_count")[0]
        logger.info("Problem has %d models", model_count)

class MinorGraph:
    def __init__(self, adj_list, projected):
        self.adj_list = adj_list
        self._project = projected
        self._quantified = projected

        self._locked = None         #if we do not immediately remove the first self._project variable, it will be locked, actually only contains therefore atm at most one self._projected
        self._todo_clique = None    #variables that belong to a clique (only connected via self._projected paths)
        self._clique_uses_project = None    #maps cliques to corresponding self._projected atoms that will be removed
        self._clauses = []
        self._edges = []
        self._node_map = {}
        self._node_rev_map = {}

    def quantified(self):
        return self._quantified

    @property
    def project(self):
        return self._project

    @project.setter
    def project(self, p):
        self._project = p

    @property
    def edges(self):
        if len(self._edges) > 0:
            return self._edges
        last = 0
        for u in self.adj_list:
            last += 1
            self._node_map[u] = last
            self._node_rev_map[last] = u

        for u in self.adj_list:
            for v in self.adj_list[u]:
                if u < v:
                    self._edges.append((self._node_map[u],self._node_map[v]))
        return self._edges

    def orig_node(self,node):
        return self._node_rev_map[node]

    def _nonProjectNgbs(self, v, todo, ngbs, rem=True):
        for i in self.neighbors(v):
            assert(i != v)
            if i not in self._locked:
                if i not in self._project:  #todo: improve?
                    ngbs.add(i)
                elif i not in todo:
                    todo.append(i)
        if rem:
            self.remove_node(v)
        else:
            self._locked.add(v)
        return True

    def add_edge(self,a,b):
        self.adj_list[a].add(b)
        self.adj_list[b].add(a)

    def remove_node(self,v):
        if v in self.adj_list:
            for n in self.adj_list[v]:
                if v in self.adj_list[n]:
                    self.adj_list[n].remove(v)
        self.adj_list.pop(v,None)

    def neighbors(self,v):
        if v in self.adj_list:
            return self.adj_list[v]
        return []

    def contract(self, vx, rem=True):
        result = None
        initial_rem = rem
        ngbs = set()
        todo = [vx]
        pos = 0
        while pos < len(todo):
            v = todo[pos]
            res = self._nonProjectNgbs(v, todo, ngbs, rem=rem)
            if v == vx:
                result = res
            rem = True
            pos += 1

        if result:
            if tuple(ngbs) in self._clique_uses_project:
                self._clique_uses_project[tuple(ngbs)] += tuple(todo)
            else:
                self._clique_uses_project[tuple(ngbs)] = tuple(todo)

        if not initial_rem:
            for i in ngbs:
                self.add_edge(vx, i)
        else: #make cliques, not used anymore if initial_rem is False
            for i in ngbs:
                for j in ngbs:
                    if i < j:
                        self.add_edge(i, j)
            result = False
        return result

    def projectionVariablesOf(self, nodes):
        result = []
        nodes = set(nodes)
        for k, v in self._clique_uses_project.items():
            if nodes.issuperset(k):
                result += v
        return result

    def abstract(self, initial_rem=False):
        self._locked = set()
        self._clique_uses_project = {}
        self._todo_clique = []
        while len(self._project) > 0:
            j = self._project.pop()
            if self.contract(j, rem=initial_rem):
                self._todo_clique.append(j)

    def add_cliques(self):
        for k in self._todo_clique:
            for i in self.neighbors(k):
                assert(i not in self._todo_clique)
                for j in self.neighbors(k):
                    if i > j:
                        self.add_edge(i, j)
            self.remove_node(k)
        self._todo_clique = None
        self._locked = None

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
        )
    }
)
