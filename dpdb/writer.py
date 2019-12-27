import math
def normalize_cnf(clauses):
    var_map = {}
    num_vars = 0
    mapped_clauses = []
    for c in clauses:
        mapped_clause = []
        for v in c:
            if not abs(v) in var_map:
                num_vars += 1
                var_map[abs(v)] = num_vars
            mapped_clause.append(int(math.copysign(var_map[abs(v)],v)))
        mapped_clauses.append(mapped_clause)
    return mapped_clauses

class Writer(object):
    def write(self, str):
        pass

    def writeline(self, str):
        self.write(str)
        self.write("\n")

    def flush(self):
        pass

    def write_gr(self, num_vertices, edges):
        self.writeline("p tw {0} {1}".format(num_vertices,len(edges)))
        for e in edges:
            self.writeline("{0} {1}".format(e[0],e[1]))
        self.flush()

    def write_td(self, num_bags, tree_width, num_orig_vertices, root, bags, edges):
        self.writeline("s td {0} {1} {2}".format(num_bags, tree_width + 1, num_orig_vertices))
        self.writeline("c r {0}".format(root))
        for b, v in bags.items():
            self.writeline("b {0} {1}".format(b, " ".join(map(str,v))))
        for e in edges:
            self.writeline("{0} {1}".format(e[0],e[1]))

    def write_cnf(self, num_vars, clauses, normalize=False):
        if normalize:
            clauses = normalize_cnf(clauses)
        self.writeline("p cnf {} {}".format(num_vars, len(clauses)))
        for c in clauses:
            self.writeline("{} 0".format(" ".join(map(str,c))))
        
class StreamWriter(Writer):
    def __init__(self, stream):
        self.stream = stream

    def write(self, str):
        self.stream.write(str.encode())

    def flush(self):
        self.stream.flush()

class FileWriter(Writer):
    def __init__(self, fname, mode="w"):
        self.file_name = fname
        self.mode = mode
        if self.mode[-1] != "b":
            self.mode += "b"

    def __enter__(self):
        self.fd = open(self.file_name, self.mode)
        self.stream_writer = StreamWriter(self.fd)
        return self.stream_writer

    def __exit__(self, type, value, traceback):
        self.fd.close()

    def write(self, str):
        self.fd.write(str)

    def flush(self):
        self.stream_writer.flush()
