import logging

logger = logging.getLogger(__name__)

class Reader(object):
    @classmethod
    def from_file(cls, fname):
        with open(fname) as f:
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
                self.format = line[1] 
                self._problem_vars = line[2:]
                return lineno+1
            elif self.is_comment(line):
                continue
            else:
                logger.warning("Invalid content in preamble at line %d: %s", lineno, line)
        logger.error("No type found in DIMACS file!")
        return lineno
        
class CnfReader(DimacsReader):
    def __init__(self):
        super().__init__()
        self.vars = []
        self.clauses = []

    def store_problem_vars(self):
        self.num_vars = int(self._problem_vars[0])
        self.num_clauses = int(self._problem_vars[1])

    def body(self, lines):
        if self.format != "cnf":
            logger.error("Not a cnf file!")
            return
        
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

class TdReader(DimacsReader):
    def __init__(self):
        super().__init__()
        self.bags = {}
        self.adjacency_list = {}

    def store_problem_vars(self):
        self.num_bags = int(self._problem_vars[0])
        self.tree_width = int(self._problem_vars[1]) - 1
        self.num_orig_vertices = int(self._problem_vars[2])

    def _add_directed_edge(self, vertex1, vertex2):
        if vertex1 in self.adjacency_list:
            self.adjacency_list[vertex1].append(vertex2)
        else:
            self.adjacency_list[vertex1] = [vertex2]

    def body(self, lines):
        if self.format != "td":
            logger.error("Not a td file!")
            return
        
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
            else:
                line = line.split()
                if len(line) != 2:
                    logger.warning("Expected exactly 2 vertices at line %d, but %d found", lineno, len(line))
                vertex1 = int(line[0])
                vertex2 = int(line[1])

                self._add_directed_edge(vertex1,vertex2)
                self._add_directed_edge(vertex2,vertex1)

class GrReader(DimacsReader):
    def __init__(self):
        super().__init__()
        self.edges = []

    def store_problem_vars(self):
        self.num_vertices = int(self._problem_vars[0])
        self.num_edges = int(self._problem_vars[1])

    def body(self, lines):
        if self.format != "tw":
            logger.error("Not a tw file!")
            return

        for lineno, line in enumerate(lines):
            if not line or self.is_comment(line):
                continue

            line = line.split()
            if len(line) != 2:
                logger.warning("Expected exactly 2 vertices at line %d, but %d found", lineno, len(line))
            vertex1 = int(line[0])
            vertex2 = int(line[1])

            self.edges.append((vertex1,vertex2))
            self.edges.append((vertex2,vertex1))

        if len(self.edges) != self.num_edges * 2:
            logger.warning("Effective number of edges mismatch preamble (%d vs %d)", len(self.edges)/2, self.num_edges)

class GraphReader(DimacsReader):
    def __init__(self):
        super().__init__()
        self.edges = []

    def store_problem_vars(self):
        self.num_vertices = int(self._problem_vars[0])
        self.num_edges = int(self._problem_vars[1])

    def body(self, lines):
        if self.format != "edge":
            logger.error("Not a tw file!")
            return

        for lineno, line in enumerate(lines):
            if not line or self.is_comment(line):
                continue

            line = line.split()
            if line[0] != 'e':
                logger.warning("Invalid line %d", lineno)
            vertex1 = int(line[1])
            vertex2 = int(line[2])

            self.edges.append((vertex1,vertex2))
            self.edges.append((vertex2,vertex1))

        if len(self.edges) != self.num_edges * 2:
            logger.warning("Effective number of edges mismatch preamble (%d vs %d)", len(self.edges)/2, self.num_edges)
