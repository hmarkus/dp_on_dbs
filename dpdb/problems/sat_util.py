# -*- coding: future_fstrings -*-
from dpdb.problem import *
from collections import defaultdict

class hashabledict(dict):
    def __hash__(self):
        return hash(frozenset(self))

def _add_directed_edge(edges, adjacency_list, vertex1, vertex2):
    if vertex1 == vertex2:
        return

    if vertex1 in adjacency_list:
        adjacency_list[vertex1].add(vertex2)
    else:
        adjacency_list[vertex1] = set([vertex2])
    if vertex1 < vertex2:
        edges.add((vertex1,vertex2))

def cnf2primal (num_vars, clauses, var_clause_dict = defaultdict(set), ret_adj=False):
    edges = set([])
    adj = {}
    for clause in clauses:
        atoms = [abs(lit) for lit in clause]
        clause_set = hashabledict({frozenset(atoms): frozenset(clause)})
        for i in atoms:
            var_clause_dict[i].add(clause_set)
            for j in atoms:
                _add_directed_edge(edges,adj,i,j)
                _add_directed_edge(edges,adj,j,i)
    if ret_adj:
        return (num_vars, edges, adj)
    else:
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

def covered_clauses(clauses, vertices):
    vertice_set = set(vertices)
    cur_cl = set()
    for v in vertices:
        candidates = clauses[v]
        for d in candidates:
            for key, val in d.items():
                if key.issubset(vertice_set):
                    cur_cl.add(val)

    return list(map(list,cur_cl))

def filter(clauses, node):
    cur_cl = covered_clauses(clauses, node.vertices)
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
