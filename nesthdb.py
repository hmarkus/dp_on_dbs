#!/usr/bin/python3
# -*- coding: future_fstrings -*-
import logging
import sys

from collections import defaultdict

from common import *
from dpdb.abstraction import MinorGraph, ClingoControl
from dpdb.problems.sat_util import *
from dpdb.reader import CnfReader
from dpdb.writer import StreamWriter, normalize_cnf

logger = logging.getLogger("nestHDB")
#setup_logging("DEBUG")
setup_logging()

def read_input(fname):
    input = CnfReader.from_file(fname)
    return input.num_vars, input.num_clauses, input.clauses, input.projected

def preprocess(cfg, num_vars, clauses):
    preprocessor = [cfg["path"]]
    if "args" in cfg:
        preprocessor.extend(cfg["args"].split(' '))
    ppmc = subprocess.Popen(preprocessor,stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    StreamWriter(ppmc.stdin).write_cnf(num_vars,clauses)
    ppmc.stdin.close()
    input = CnfReader.from_stream(ppmc.stdout,silent=True)
    ppmc.wait()
    ppmc.stdout.close()
    return input.maybe_sat, input.num_vars, input.vars, input.num_clauses, input.clauses, input.models

def abstract(vars, adj, projected):
    proj_out = vars - projected
    mg = MinorGraph(vars, adj, proj_out)
    mg.abstract()
    mg.add_cliques()
    mg.normalize()
    return mg.normalized_nodes, mg.normalized_adj, mg.normalized_edges, set([mg.normalized_node(p) for p in projected]), mg
    
def choose_subset(edges, projected, asp):
    for enc in asp["encodings"]:
        size = enc["size"]
        timeout = 30 if "timeout" not in enc else enc["timeout"]
        logger.debug("Running clingo %s for size %d and timeout %d", enc["file"],size,timeout)
        c = ClingoControl(edges,projected)
        res = c.choose_subset(min(size,len(projected)),enc["file"],timeout)[2]
        if len(res) == 0:
            logger.warning("Clingo did not produce an answer set, fallback to previous result {}".format(projected))
        else:
            projected = res[0]
        logger.debug("Clingo done%s", " (timeout)" if c.timeout else "")
    return set(projected)

def call_solver(type,num_vars,clauses,projected):
    logger.info(f"Call solver: {type}: {clauses}")
    return 1
    
def nestedpmc(cfg,mg,td,projected,var_clause_dict,depth):
    import itertools
    logger.info("Simulating nested PMC")
    for n in td.nodes:
        orig_vars = [mg.orig_node(v) for v in n.vertices]
        covered_vars = mg.projectionVariablesOf(orig_vars) + orig_vars
        num_vars = len(covered_vars)
        clauses = covered_clauses(var_clause_dict, covered_vars)
        logger.debug(f"Processing bag {n.id}: {orig_vars}")
        local_projected = projected - set(n.vertices)
        orig_projected = [mg.orig_node(p) for p in local_projected]
        logger.debug(f"covered_vars: {covered_vars}, covered_clauses: {clauses}, bag: {orig_vars}")
        assignments = [list(i) for i in itertools.product([0, 1], repeat=len(n.vertices))]
        for ass in assignments:
            extra_clauses = []
            for (i,a) in enumerate(ass):
                if a == 1:
                    extra_clauses.append([orig_vars[i]])
                    clauses.append([orig_vars[i]])
                else:
                    extra_clauses.append([-orig_vars[i]])
                    clauses.append([-orig_vars[i]])
            norm_map = {}
            clauses, local_projected, num_vars = normalize_cnf(clauses,orig_projected,norm_map)
            #logger.info(f"Normalized clauses: {clauses}, projected: {local_projected}")
            logger.info(f"Processing bag {n.id} depth {depth+1} assignment {extra_clauses}")
            nesthdb(cfg,num_vars,len(clauses),clauses,local_projected,depth+1,n.id)

def nesthdb(cfg,num_vars,num_clauses,clauses,projected_orig,depth=0,bag=None):
    logger.info(f"Original #vars: {num_vars}, #clauses: {num_clauses}, #projected: {len(projected_orig)}")
    logger.debug(f"Formula: {clauses}")
    logger.debug(f"Projected: {projected_orig}")

    # Preprocessing
    maybe_sat, num_vars, vars, num_clauses, clauses, models = preprocess(cfg["nesthdb"]["preprocessor"], num_vars, clauses)
    if maybe_sat == False:
        logger.info("Preprocessor UNSAT")
        return 0
    if models != None:
        logger.info(f"Solved by preprocessor: {models} models")
        return models

    projected = projected_orig.intersection(vars)
    logger.info(f"Preprocessing #vars: {num_vars}, #clauses: {num_clauses}, #projected: {len(projected)}")
    logger.debug(f"Formula: {clauses}")
    logger.debug(f"Projected: {projected}")
    if len(projected) == 0:
        logger.info("Intersection of vars and projected is empty")
        return call_solver("sat",num_vars,clauses,projected)

    # Nested primal
    var_clause_dict = defaultdict(set)
    num_vars, edges, adj = cnf2primal(num_vars, clauses, var_clause_dict, True)
    logger.info(f"Primal graph #vertices: {num_vars}, #edges: {len(edges)}")
    nodes, adj, edges, projected, mg = abstract(vars, adj, projected)
    logger.info(f"Nested primal graph #vertices: {len(nodes)}, #edges: {len(edges)}")

    # Decompose
    td = decompose(len(nodes),edges,cfg["htd"])
    if td.tree_width >= cfg["nesthdb"]["threshold_hybrid"]:
        logger.info("Tree width >= hybrid threshold ({})".format(cfg["nesthdb"]["threshold_hybrid"]))
        if vars == projected:
            return call_solver("sharpsat",num_vars,clauses,projected)
        else:
            return call_solver("pmc",num_vars,clauses,projected)

    if td.tree_width >= cfg["nesthdb"]["threshold_abstract"]:
        logger.info("Tree width >= abstract threshold ({})".format(cfg["nesthdb"]["threshold_abstract"]))
        projected = choose_subset(edges, projected, cfg["nesthdb"]["asp"])
        logger.info(f"Subset #projected: {len(projected)}")
        nodes, adj, edges, projected, mg = abstract(nodes, adj, projected)
        logger.info(f"Nested primal graph #vertices: {len(nodes)}, #edges: {len(edges)}")
        td = decompose(len(nodes),edges,cfg["htd"])

    nestedpmc(cfg,mg,td,projected,var_clause_dict,depth)

def main():
    cfg = read_cfg("config.json")
    fname = sys.argv[1]

    # Read input
    logger.info(f"Reading input {fname}")
    num_vars, num_clauses, clauses, projected_orig = read_input(fname)
    nesthdb(cfg,num_vars,num_clauses,clauses,projected_orig)

if __name__ == "__main__":
    main()
