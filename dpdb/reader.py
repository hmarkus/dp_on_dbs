import logging
import sys

logger = logging.getLogger(__name__)

class Reader(object):
    @classmethod
    def from_file(cls, fname):
        with open(fname, "r") as f:
            return cls.from_string(f.read())

    @classmethod
    def from_stream(cls, stream):
        return cls.from_string(stream.read().decode())

    @classmethod
    def from_string(cls, string):
        instance = cls()
        instance.parse(string)
        return instance

    def parse(self, string):
        pass

class DimacsReader(Reader):
    def parse(self, string):
        self.problem_solution_type = "?"
        self.format = "?"
        lines = string.split("\n")
        body_start = self.preamble(lines)
        self.store_problem_vars()
        self.body(lines[body_start:])

    def store_problem_vars(self):
        pass

    def is_comment(self, line):
        return line.startswith("c ") or line == "c"

    def body(self, string):
        pass

    def preamble(self,lines):
        for lineno, line in enumerate(lines):
            if line.startswith("p ") or line.startswith("s "):
                line = line.split()
                self.problem_solution_type = line[0]
                self.format = line[1] 
                self._problem_vars = line[2:]
                return lineno+1
            elif not line or self.is_comment(line):
                continue
            else:
                logger.warning("Invalid content in preamble at line %d: %s", lineno, line)
        logger.error("No type found in DIMACS file!")
        sys.exit(1)
        
class CnfReader(DimacsReader):
    def __init__(self):
        super().__init__()
        self.vars = []
        self.clauses = []
        self.solution = -1

    def store_problem_vars(self):
        # We assume a CNF file containing a solution is pre-solved by pmc and
        # the solution line contains only the number of models for sharpsat
        if self.problem_solution_type == 's':
            logger.info("Problem has %d models (solved by pre-processing)", int(self.format))
        else:
            self.num_vars = int(self._problem_vars[0])
            self.num_clauses = int(self._problem_vars[1])

    def body(self, lines):
        if self.format != "cnf":
            logger.error("Not a cnf file!")
            sys.exit(1)
        
        maxvar = 0
        for lineno, line in enumerate(lines):
            if not line or self.is_comment(line):
                continue
            i = 1
            if line.startswith("c ") or line == "c":
                continue
            while line[-1] != '0':
                if lineno + i >= len(lines):
                    logger.warning("Clause at line %d not terminated with 0", lineno)
                    # Ignore clause instead of "fixing" it?
                    line += " 0"
                    break
                line += lines[lineno + i]
                lines[lineno + i] = None
                i += 1
            clause = [int(v) for v in line.split()[:-1]]
            self.clauses.append(clause)
            atoms = [abs(lit) for lit in clause]
            self.vars.append(atoms)
            maxvar = max(maxvar,max(atoms))

        if maxvar != self.num_vars:
            logger.warning("Effective number of variables mismatch preamble (%d vs %d)", maxvar, self.num_vars)
        if len(self.clauses) != self.num_clauses:
            logger.warning("Effective number of clauses mismatch preamble (%d vs %d)", len(self.clauses), self.num_clauses)

def _add_directed_edge(edges, adjacency_list, vertex1, vertex2):
    if vertex1 in adjacency_list:
        adjacency_list[vertex1].append(vertex2)
    else:
        adjacency_list[vertex1] = [vertex2]
    edges.append((vertex1,vertex2))

class TdReader(DimacsReader):
    def __init__(self):
        super().__init__()
        self.edges = []
        self.bags = {}
        self.adjacency_list = {}
        self.max_bag = 0

    def store_problem_vars(self):
        if self.problem_solution_type == "p":
            self.num_vertices = int(self._problem_vars[0])
            self.num_edges = int(self._problem_vars[1])
        elif self.problem_solution_type == "s":
            self.num_bags = int(self._problem_vars[0])
            self.tree_width = int(self._problem_vars[1]) - 1
            self.num_orig_vertices = int(self._problem_vars[2])
        else:
            logger.error("Unrecognized problem or solution indicator: %s", self.problem_solution_type)

    def _add_directed_edge(self, vertex1, vertex2):
        if vertex1 in self.adjacency_list:
            self.adjacency_list[vertex1].append(vertex2)
        else:
            self.adjacency_list[vertex1] = [vertex2]
        self.edges.append((vertex1,vertex2))

    def body(self, lines):
        if self.format != "td":
            logger.error("Not a td file!")
            sys.exit(1)
        
        for lineno, line in enumerate(lines):
            if not line:
                continue

            if self.is_comment(line):
                line = line.split()
                if len(line) > 2 and line[1] == 'r':
                    self.root = int(line[2])
            elif line.startswith("b "):
                line = line.split()
                self.bags[int(line[1])] = [int(v) for v in line[2:]]
                current_length = len(self.bags[int(line[1])])
                if current_length > self.max_bag:
                	self.max_bag = current_length
            else:
                line = line.split()
                if len(line) != 2:
                    logger.warning("Expected exactly 2 vertices at line %d, but %d found", lineno, len(line))
                vertex1 = int(line[0])
                vertex2 = int(line[1])

                _add_directed_edge(self.edges,self.adjacency_list,vertex1,vertex2)
                _add_directed_edge(self.edges,self.adjacency_list,vertex2,vertex1)

        if self.problem_solution_type == "p":
            if len(self.edges) != self.num_edges * 2:
                logger.warning("Effective number of edges mismatch preamble (%d vs %d)", len(self.edges)/2, self.num_edges)
        elif self.problem_solution_type == "s":
            if len(self.bags) != self.num_bags:
                logger.warning("Effective number of bags mismatch preamble (%d vs %d)", len(self.bags), self.num_bags)

class TwReader(DimacsReader):
    def __init__(self):
        super().__init__()
        self.edges = []
        self.adjacency_list = {}

    def store_problem_vars(self):
        self.num_vertices = int(self._problem_vars[0])
        self.num_edges = int(self._problem_vars[1])

    def body(self, lines):
        if self.format != "tw":
            logger.error("Not a tw file!")
            sys.exit(1)

        for lineno, line in enumerate(lines):
            if not line or self.is_comment(line):
                continue

            line = line.split()
            if len(line) != 2:
                logger.warning("Expected exactly 2 vertices at line %d, but %d found", lineno, len(line))
            vertex1 = int(line[0])
            vertex2 = int(line[1])

            _add_directed_edge(self.edges,self.adjacency_list,vertex1,vertex2)
            _add_directed_edge(self.edges,self.adjacency_list,vertex2,vertex1)

        if len(self.edges) != self.num_edges * 2:
            logger.warning("Effective number of edges mismatch preamble (%d vs %d)", len(self.edges)/2, self.num_edges)

class EdgeReader(DimacsReader):
    def __init__(self):
        super().__init__()
        self.edges = []
        self.adjacency_list = {}

    def store_problem_vars(self):
        self.num_vertices = int(self._problem_vars[0])
        self.num_edges = int(self._problem_vars[1])

    def body(self, lines):
        if self.format != "edge":
            logger.error("Not a edge file!")
            sys.exit(1)

        for lineno, line in enumerate(lines):
            if not line or self.is_comment(line):
                continue

            line = line.split()
            if line[0] != 'e':
                logger.warning("Invalid line %d", lineno)
            vertex1 = int(line[1])
            vertex2 = int(line[2])

            _add_directed_edge(self.edges,self.adjacency_list,vertex1,vertex2)
            _add_directed_edge(self.edges,self.adjacency_list,vertex2,vertex1)

        if len(self.edges) != self.num_edges * 2:
            logger.warning("Effective number of edges mismatch preamble (%d vs %d)", len(self.edges)/2, self.num_edges)
