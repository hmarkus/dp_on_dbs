# -*- coding: future_fstrings -*-
import clingo
import importlib
import logging
import subprocess
import sys
import threading
import tempfile
import random

from dpdb.reader import *
from dpdb.writer import StreamWriter, FileWriter

logger = logging.getLogger(__name__)

class Abstraction:
    def __init__(self, sub_procs, sat_solver, asp_encodings=None, sat_solver_seed_arg=None, preprocessor_path=None, preprocessor_args=None, projected_size=8, asp_timeout=30, **kwargs):
        random.seed(kwargs["runid"])
        self.sat_solver = [sat_solver["path"]]
        if "seed_arg" in sat_solver:
            self.sat_solver.append(sat_solver["seed_arg"])
            self.sat_solver.append(str(kwargs["runid"]))
        if "output_parser" in sat_solver:
            self.sat_solver_parser = sat_solver["output_parser"]
            reader_module = importlib.import_module("dpdb.reader")
            self.sat_solver_parser_cls = getattr(reader_module, sat_solver["output_parser"]["class"])
        else:
            self.sat_solver_parser = {"class":"CnfReader","args":{"silent":True},"result":"models"}
            self.sat_solver_parser_cls = CnfReader
        self.asp_encodings = asp_encodings
        if preprocessor_path:
            self.preprocessor = [preprocessor_path]
            if preprocessor_args:
                self.preprocessor.extend(preprocessor_args.split(' '))
        else:
            self.preprocessor = None

        self.projected_size = projected_size
        self.asp_timeout = asp_timeout
        self.sub_procs = sub_procs
        self.interrupted = False

    def abstract(self, num_vars, edges, adj, projected):
        if self.asp_encodings:
            for enc in self.asp_encodings:
                size = self.projected_size if "size" not in enc else enc["size"]
                timeout = self.asp_timeout if "timeout" not in enc else enc["timeout"]
                logger.debug("Running clingo %s for size %d and timeout %d", enc["file"],size,timeout)
                c = ClingoControl(edges,projected)

                res = c.choose_subset(min(size,len(projected)),enc["file"],timeout)[2]
                if len(res) == 0:
                    logger.warning("Clingo did not produce an answer set, fallback to previous result {}".format(projected))
                else:
                    projected = res[0]
                logger.debug("Clingo done%s", " (timeout)" if c.timeout else "")
        proj_out = set(range(1,num_vars+1)) - set(projected)
        self.mg = MinorGraph(range(1,num_vars+1),adj, proj_out)
        self.mg.abstract()
        self.mg.add_cliques()
        return len(projected), self.mg.edges

    def solve_external(self, num_vars, clauses, extra_clauses, proj_vars=None):
        logger.debug("Calling external solver for {} with {} clauses, {} vars, and proj {}".format(extra_clauses, len(clauses), num_vars, proj_vars))
        maybe_sat = True
        tmp = tempfile.NamedTemporaryFile().name
        normalize_cnf = True
        if self.preprocessor:
            logger.debug("Preprocessing")
            ppmc = subprocess.Popen(self.preprocessor,stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            self.sub_procs.add(ppmc)
            StreamWriter(ppmc.stdin).write_cnf(num_vars,clauses, normalize=True)
            normalize_cnf = False
            ppmc.stdin.close()
            input = CnfReader.from_stream(ppmc.stdout,silent=True)
            ppmc.wait()
            ppmc.stdout.close()
            self.sub_procs.remove(ppmc)
            maybe_sat = input.maybe_sat
            num_vars = input.num_vars
            clauses = input.clauses
        if maybe_sat and not self.interrupted:
            with FileWriter(tmp) as fw:
                fw.write_cnf(num_vars,clauses,normalize=normalize_cnf, proj_vars=proj_vars)
                for i in range(0,128,1):
                    if self.interrupted:
                        break
                    if len(self.sat_solver) == 3:	#seed given
                        self.sat_solver[2] = str(random.randrange(13423423471))
                    psat = subprocess.Popen(self.sat_solver + [tmp], stdout=subprocess.PIPE)
                    self.sub_procs.add(psat)
                    output = self.sat_solver_parser_cls.from_stream(psat.stdout,**self.sat_solver_parser["args"])
                    psat.wait()
                    psat.stdout.close()
                    self.sub_procs.remove(psat)
                    result = getattr(output,self.sat_solver_parser["result"])
                    if psat.returncode == 245 or psat.returncode == 250:
                        logger.debug("Retrying call to external solver, returncode {}, index {}".format(psat.returncode, i))
                    else:
                        logger.debug("No Retry, returncode {}, result {}, index {}".format(psat.returncode, result, i))
                        break
        else:
            result = 0
        if result is None:
            logger.warning("Result is None!")
        return result

    def orig_vertex(self,vertex):
        return self.mg.orig_node(vertex)

    def orig_vertices(self,vertices):
        return [self.orig_vertex(v) for v in vertices]

    def abstracted_vertices(self,vertices):
        return self.mg.projectionVariablesOf(vertices)

    def interrupt(self):
        self.interrupted = True

def safe_int(string):
    try:
        return int(string)
    except ValueError:
        return string

class ClingoControl:
    def __init__(self, edges, nodes):
        self._edges = edges
        self._nodes = nodes
        self.grounded = False
        self.timeout = False

    # this method tries integer graph nodes if possible
    # encodingFile - encoding to use, probably guess_v2.lp is the best so far, prevents direct encoding of reachability
    # select_subset - cardinality of subset required
    # timeout - seconds to wait at most for result
    # solve_limit, see clingo documentation
    # clingoctl - start with given clingoctl, for use incremental solving or resolve etc.
    def choose_subset(self, select_subset, encodingFile, timeout=30, usc=False, solve_limit="umax,umax", clingoctl=None):
        if clingo is None:
            raise ImportError()

        c = clingoctl

        aset = [sys.maxsize, False, [], None, []]
        
        def __on_model(model):
            #if len(model.cost) == 0:
            #    return
            
            logger.debug("better answer set found: %s %s %s", model, model.cost, model.optimality_proven)
            
            aset[1] |= model.optimality_proven
            opt = abs(model.cost[0] if len(model.cost) > 0 else 0)
            if opt <= aset[0]:
                if opt < aset[0]:
                    aset[2] = []
                aset[0] = opt
                answer_set = [safe_int(x) for x in str(model).translate(str.maketrans(dict.fromkeys("abs()"))).split(" ")]
                # might get "fake" duplicates :(, with different model.optimality_proven
                if answer_set not in aset[2][-1:]:
                    aset[2].append(answer_set)

        with open(encodingFile,"r") as encoding:
            encodingContent = "".join(encoding.readlines())

        # FIXME: use mutable string
        prog = encodingContent
        
        if clingoctl is None:
            c = clingo.Control()

            if usc:
                c.configuration.solver.opt_strategy = "usc,pmres,disjoint,stratify"
                c.configuration.solver.opt_usc_shrink = "min"
            c.configuration.solve.opt_mode = "opt"
            # c.configuration.solve.models = 0
            c.configuration.solve.solve_limit = solve_limit

            for e in self._edges:
                prog += "edge({0},{1}).\n".format(e[0], e[1])

            for p in self._nodes:
                prog += "p({0}).\n".format(p)

            # subset (buckets) of proj to select upon 
            for b in range(1, select_subset + 1, 1):
                prog += "b({0}).\n".format(b)

        aset[3] = c

        c.add("prog{0}".format(select_subset), [], str(prog))

        def solver(c, om):
            c.ground([("prog{0}".format(select_subset), [])])
            self.grounded = True
            c.solve(on_model=om)

        t = threading.Thread(target=solver, args=(c, __on_model))
        t.start()
        t.join(timeout)
        self.timeout = t.is_alive()
        c.interrupt()
        t.join()

        aset[1] |= c.statistics["summary"]["models"]["optimal"] > 0
        aset[4] = c.statistics
        return aset

class MinorGraph:
    def __init__(self, nodes, adj_list, projected):
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
        self._returned = {}
        self._nodes = set(nodes)
        self.lock = threading.Lock()

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
        if len(self.adj_list) == 0:
            assert(last == 0)
            for u in self._nodes:
                last += 1
                self._node_map[u] = last
                self._node_rev_map[last] = u
        return self._edges

    def orig_node(self,node):
        return self._node_rev_map[node]

    def _nonProjectNgbs(self, v, todo, ngbs, rem=True):
        if v not in self._nodes:
            return False
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
        self._nodes.remove(v)

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
        tn = tuple(nodes)
        with self.lock:
            if tn in self._returned:
                return list(self._returned[tn])
            result = set()
            nodes = set(nodes)
            for k, v in self._clique_uses_project.items():
                if nodes.issuperset(k):
                    result.update(v)

            for k, v in self._returned.items():
                result -= v
            self._returned[tn] = result
        return list(result)

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

