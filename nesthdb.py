#!/usr/bin/python3
# -*- coding: future_fstrings -*-
import logging
import sys

from collections import defaultdict

from common import *
from dpdb.abstraction import MinorGraph, ClingoControl
from dpdb.db import BlockingThreadedConnectionPool, DEBUG_SQL, setup_debug_sql
from dpdb.problems.nestpmc import NestPmc
from dpdb.problems.sat_util import *
from dpdb.reader import CnfReader
from dpdb.writer import FileWriter, StreamWriter, normalize_cnf

logger = logging.getLogger("nestHDB")
#setup_logging("DEBUG")
setup_logging()
setup_debug_sql()

class Formula:
    def __init__(self, vars, clauses, projected=None):
        self.vars = vars
        self.num_vars = len(vars)
        self.clauses = clauses
        self.num_clauses = len(clauses)
        self.projected = projected
        self.var_clause_dict = defaultdict(set)

    @classmethod
    def from_file(cls, fname):
        input = CnfReader.from_file(fname)
        return cls(input.vars, input.clauses, input.projected)

class Graph:
    def __init__(self, nodes, edges, adj_list):
        self.nodes = nodes
        self.edges = edges
        self.adj_list = adj_list
        self.tree_decomp = None

    @property
    def num_nodes(self):
        return len(self.nodes)

    @property
    def num_edges(self):
        return len(self.edges)

    def abstract(self, non_nested):
        proj_out = self.nodes - non_nested
        #print("nodes:",self.nodes)
        #print("non-nested:",non_nested)
        mg = MinorGraph(self.nodes, self.adj_list, proj_out)
        mg.abstract()
        #print("edges:",mg.edges)
        mg.add_cliques()
        self.nodes = mg.nodes
        self.edges = mg.edges
        self.adj_list = mg.adj_list
        self.mg = mg

    def normalize(self):
        self.nodes_normalized = set()
        self.edges_normalized = set()
        self.adj_list_normalized = {}
        self._node_map = {}
        self._node_rev_map = {}

        #print("nodes in norm:",self.nodes)
        #print("edges in norm:",self.edges)
        last = 0
        for n in self.nodes:
            last += 1
            self._node_map[n] = last
            self._node_rev_map[last] = n
            self.nodes_normalized.add(last)

        for e in self.edges:
            u = self._node_map[e[0]]
            v = self._node_map[e[1]]
            if u < v:
                self.edges_normalized.add((u,v))
            else:
                self.edges_normalized.add((v,u))

    def decompose(self, seed=42):
        global cfg
        self.normalize()
        self.tree_decomp = decompose(self.num_nodes,self.edges_normalized,cfg["htd"],gr_file="test.gr",td_file="test.td",node_map=self._node_rev_map)

class Problem:
    def __init__(self, formula, non_nested, depth=0):
        self.formula = formula
        self.projected = formula.projected
        self.projected_orig = formula.projected
        self.non_nested = non_nested
        self.non_nested_orig = non_nested
        self.maybe_sat = True
        self.models = None
        self.depth = depth

    def preprocess(self):
        global cfg
        if "preprocessor" not in cfg["nesthdb"]:
            return # True, num_vars, vars, len(clauses), clauses, None
        cfg_prep = cfg["nesthdb"]["preprocessor"]
        preprocessor = [cfg_prep["path"]]
        if "args" in cfg_prep:
            preprocessor.extend(cfg_prep["args"].split(' '))
        ppmc = subprocess.Popen(preprocessor,stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        StreamWriter(ppmc.stdin).write_cnf(self.formula.num_vars,self.formula.clauses)
        ppmc.stdin.close()
        input = CnfReader.from_stream(ppmc.stdout,silent=True)
        ppmc.wait()
        ppmc.stdout.close()
        self.formula = Formula(input.vars,input.clauses)
        self.maybe_sat = input.maybe_sat
        self.models = input.models	#TODO: 1) sometimes preprocessor returns "s inf", 2) for pmc this is, sadly, wrong most of the time :(
        self.projected = self.projected.intersection(input.vars)

    def decompose_nested_primal(self):
        num_vars, edges, adj = cnf2primal(self.formula.num_vars, self.formula.clauses, self.formula.var_clause_dict, True)
        #print("vars:",self.formula.vars)
        self.graph = Graph(set(self.formula.vars), edges, adj)
        logger.info(f"Primal graph #vertices: {num_vars}, #edges: {len(edges)}")
        #nodes, normalized_adj, normalized_edges, mg = abstract(vars, adj, self.projected)
        self.graph.abstract(self.non_nested)
        logger.info(f"Nested primal graph #vertices: {self.graph.num_nodes}, #edges: {self.graph.num_edges}")
        self.graph.decompose()

    def choose_subset(self):
        global cfg
        cfg_asp = cfg["nesthdb"]["asp"]
        for enc in cfg_asp["encodings"]:
            size = enc["size"]
            timeout = 30 if "timeout" not in enc else enc["timeout"]
            logger.debug("Running clingo %s for size %d and timeout %d", enc["file"],size,timeout)
            c = ClingoControl(self.graph.edges,self.non_nested)
            res = c.choose_subset(min(size,len(self.non_nested)),enc["file"],timeout)[2]
            print(res)
            if len(res) == 0:
                logger.warning("Clingo did not produce an answer set, fallback to previous result {}".format(projected))
            else:
                self.non_nested = set(res[0])
            logger.debug("Clingo done%s", " (timeout)" if c.timeout else "")

    def call_solver(self,type):
        assert(type == "sat")
        logger.info(f"Call solver: {type} with #vars {self.formula.num_vars}, #clauses {len(self.formula.clauses)}, #projected {len(self.projected)}")
        sat_solver = ['/home/hecher/decodyn/src/benchmark-tool/programs/picosat-965']
        import tempfile
        tmp = tempfile.NamedTemporaryFile().name
        with FileWriter(tmp) as fw:
            fw.write_cnf(self.formula.num_vars,self.formula.clauses,normalize=True, proj_vars=self.projected)
            for i in range(0,128,1):
                psat = subprocess.Popen(sat_solver + [tmp], stdout=subprocess.PIPE)
                output = CnfReader.from_stream(psat.stdout,silent=True)
                psat.wait()
                psat.stdout.close()
                result = output.models
                if psat.returncode == 245 or psat.returncode == 250:
                    logger.debug("Retrying call to external solver, returncode {}, index {}".format(psat.returncode, i))
                else:
                    logger.debug("No Retry, returncode {}, result {}, index {}".format(psat.returncode, result, i))
                    break

        return result
    
    def nestedpmc(self):
        global cfg

        #nestedpmc_sim(cfg,mg,td,projected,var_clause_dict,depth)
        pool = BlockingThreadedConnectionPool(1,cfg["db"]["max_connections"],**cfg["db"]["dsn"])
        #problem_cfg = {}
        #if "problem_specific" in cfg and cls.__name__.lower() in cfg["problem_specific"]:
        #    problem_cfg = cfg["problem_specific"][cls.__name__.lower()]
        #problem = NestPmc(file,pool, **cfg["dpdb"], **flatten_cfg(problem_cfg, [], '_',cls.keep_cfg()), **kwargs)
        problem = NestPmc("test",pool, **cfg["dpdb"])
        problem.set_td(self.graph.tree_decomp)
        problem.set_recursive(self.solve_rec,self.depth)
        problem.set_input(self.graph.num_nodes,-1,self.projected,self.non_nested_orig,self.formula.var_clause_dict,self.graph.mg)
        problem.setup()
        problem.solve()
        """
        print("result: ",problem.model_count)
        print("power: ", int(2**(len(orig_projected)-len(projected))))
        print("orig: ",len(orig_projected)," now: ",len(projected))
        print("power result: ",problem.model_count * int(2**(len(orig_projected)-len(projected))))
        return problem.model_count * int(2**(len(orig_projected)-len(projected)))
        """
        return problem.model_count

    def solve(self):
        logger.info(f"Original #vars: {self.formula.num_vars}, #clauses: {self.formula.num_clauses}, #projected: {len(self.projected_orig)}")
        self.preprocess()
        if self.maybe_sat == False:
            logger.info("Preprocessor UNSAT")
            return 0
        if self.models != None:
            logger.info(f"Solved by preprocessor: {self.models} models")
            return self.models

        self.non_nested = self.non_nested.intersection(self.projected)
        logger.info(f"Preprocessing #vars: {self.formula.num_vars}, #clauses: {self.formula.num_clauses}, #projected: {len(self.projected)}")

        if len(self.projected.intersection(self.formula.vars)) == 0:
            logger.info("Intersection of vars and projected is empty")
            return self.call_solver("sat")

        self.decompose_nested_primal()

        if self.depth >= 3 or self.graph.tree_decomp.tree_width >= cfg["nesthdb"]["threshold_hybrid"]: #TODO OR PROJECTION SIZE BELOW TRESHOLD OR CLAUSE SIZE BELOW TRESHOLD
            logger.info("Tree width >= hybrid threshold ({})".format(cfg["nesthdb"]["threshold_hybrid"]))
            if self.formula.vars == self.projected:
                pass #TODO
                #return call_solver("sharpsat",num_vars,clauses,projected)
            else:
                pass #TODO
                #return call_solver("pmc",num_vars,clauses,projected)

        if self.graph.tree_decomp.tree_width >= cfg["nesthdb"]["threshold_abstract"]:
            logger.info("Tree width >= abstract threshold ({})".format(cfg["nesthdb"]["threshold_abstract"]))
            self.choose_subset()
            logger.info(f"Subset #non-nested: {len(self.non_nested)}")
            self.decompose_nested_primal()
            # TODO: treewidth check, if above abstract treshold -> fallback to IF above? -> classical solver

        result = self.nestedpmc()
        print("result: ",result)
        print("power: ", int(2**(len(self.projected_orig)-len(self.projected))))
        print("orig: ",len(self.projected_orig)," now: ",len(self.projected))
        print("power result: ",result * int(2**(len(self.projected_orig)-len(self.projected))))
        return result * int(2**(len(self.projected_orig)-len(self.projected)))

    def solve_rec(self, vars, clauses, non_nested, projected, depth=0):
        """
        self.projected = projected
        self.projected_orig = projected
        self.non_nested = non_nested
        self.non_nested_orig = non_nested
        self.formula = Formula(vars,clauses,projected)
        
        return self.solve()
        """
        #print("rec vars:",vars)
        return Problem(Formula(vars,clauses,projected),non_nested,depth).solve()
        
        #(num_vars,covered_vars,len(clauses),clauses,projected,42,node.id)

def read_input(fname):
    input = CnfReader.from_file(fname)
    return input.num_vars, input.vars, input.num_clauses, input.clauses, input.projected

def main():
    global cfg
    cfg = read_cfg("config.json")
    fname = sys.argv[1]

    formula = Formula.from_file(fname)
    prob = Problem(formula,formula.vars)
    print("result: ",prob.solve())

if __name__ == "__main__":
    main()

