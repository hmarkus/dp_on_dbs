# -*- coding: future_fstrings -*-
from dpdb.problem import *

def cnf2primal (num_vars, clauses):
    edges = set([])
    for clause in clauses:
        atoms = [abs(lit) for lit in clause]
        for i in atoms:
            for j in atoms:
                if i < j:
                    edges.add((i,j))
    return (num_vars, edges)

def td_node_column_def(var):
    return (var2col(var), "BOOLEAN")

def lit2var (lit):
    return var2col(abs(lit))

def lit2val (lit):
    return str(lit > 0)

def lit2expr (lit):
    if lit > 0:
        return var2col(lit)
    else:
        return "NOT {}".format(lit2var(lit))

def filter(clauses, node):
    cur_cl = [clause for clause in clauses if all(abs(lit) in node.vertices for lit in clause)]

    if len(cur_cl) > 0:
        return "WHERE {0}".format(
            "({0})".format(") AND (".join(
                [" OR ".join(map(lit2expr,clause)) for clause in cur_cl]
            )))
    else:
        return ""

def store_clause_table(db, clauses):
    db.drop_table("sat_clause")
    num_vars = len(clauses)
    db.create_table("sat_clause", map(td_node_column_def,range(1,num_vars+1)))
    for clause in clauses:
        db.insert("sat_clause",list(map(lit2var,clause)),list(map(lit2val,clause)))
