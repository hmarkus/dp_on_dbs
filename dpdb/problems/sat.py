import logging

from dpdb.problem import Problem
from dpdb.reader import CnfReader

logger = logging.getLogger(__name__)

class Sat(Problem):
    num_vars = 0
    num_clauses = 0
    clauses = []

    def __init__(self, name, pool, **kwargs):
        super().__init__(name, "sat", pool, **kwargs)

    def td_node_column_def(self,var):
        return ("v{}".format(var), "BOOLEAN")
        
    def setup(self):
        def drop_tables():
            pass
            # TODO: add option if clauses should be stored
            #self.db.drop_table("sat_clause")

        def create_tables():
            self.db.ignore_next_praefix()
            self.db.create_table("problem_sat", [
                ("id", "INTEGER NOT NULL PRIMARY KEY REFERENCES PROBLEM(id)"),
                ("num_vars", "INTEGER NOT NULL"),
                ("num_clauses", "INTEGER NOT NULL"),
                ("is_sat", "BOOLEAN")
            ])
            # TODO: add option if clauses should be stored
            #self.db.create_table("sat_clause", map(self.td_node_column_def,range(1,self._num_vars+1)))

        def insert_data():
            self.db.ignore_next_praefix()
            self.db.insert("problem_sat",("id","num_vars","num_clauses"),
                (self.id, self.num_vars, self.num_clauses))
            # TODO: add option if clauses should be stored
            """
            for clause in self._clauses:
                self.db.insert("sat_clause",map(_lit2var,clause),map(_lit2val,clause))
            """

        super(Sat,self).setup()
        drop_tables()
        create_tables()
        insert_data()

        for n in self.td.postorder():
            ass_view = self.create_assignment_view(n)
            # ass_view can be None if bag is empty (i.e. when normalized TD is ued)
            if ass_view:
                self.db.create_view("td_n_{}_assignment".format(n.id),self.create_assignment_view(n))
        self.db.commit()

    def prepare_input(self, fname):
        input = CnfReader.from_file(fname)
        self.num_vars = input.num_vars
        self.num_clauses = input.num_clauses
        self.clauses = input.clauses

        return _cnf2primal(input.num_vars, input.clauses)

    def create_assignment_view(self,node):
        bag = node.vertices

        from_tdn = {}
        needs_introduce = False
        needs_join = False

        for n in bag:
            node_in_bag = [b.id for b in node.children if n in b.vertices]
            needs_introduce |= len(node_in_bag) == 0
            needs_join |= len(node_in_bag) > 1
            from_tdn[n] = node_in_bag

        q = "WITH truth_vals AS ("

        if needs_introduce:
            q += (
            """
            with introduce as (
                select true as x
                union
                select false
            ) """
            )

        q += (
        """SELECT {0}
        """).format(",\n\t\t\t".join([_var2tab_col(n,from_tdn[n]) for n in bag]))
        q += (
        """\tFROM {0}
        """).format(",".join(set([_var2tab(n,from_tdn[n]) for n in bag] + ["td_node_{0} t{0}".format(n.id) for n in node.children])))

        if needs_join:
            q += (
            """\tWHERE {0}
            """).format(" AND ".join(filter(None,[_var2join(n,from_tdn[n]) for n in bag])))
        
        cur_cl = [clause for clause in self.clauses if all(abs(lit) in bag for lit in clause)]

        q += (
        """) 
        SELECT {0}
          FROM truth_vals
        """).format(",".join(["v{}".format(n) if n in node.stored_vertices else "null::boolean AS v{}".format(n) for n in node.vertices]))

        if len(cur_cl) > 0:
            q += (
            """ WHERE {0}
            """).format("({0})".format(") AND (".join(
                    [" OR ".join(map(_lit2expr,clause)) for clause in cur_cl]
                )))

        return self.db.replace_dynamic_tabs(q,_dynamic_tabs(node,from_tdn))

    def solve(self):
        super(Sat,self).solve()
        root_tab = "td_node_{}".format(self.td.root.id)
        is_sat = self.db.replace_dynamic_tabs("(select exists(select 1 from {}))".format(root_tab), [root_tab])
        self.db.ignore_next_praefix()
        sat = self.db.update("problem_sat",["is_sat"],[is_sat],["ID = {}".format(self.id)],"is_sat")[0]
        self.db.commit()
        self.db.close()
        logger.info("Problem is %s", "SAT" if sat else "UNSAT")

def _cnf2primal (num_vars, clauses):
    edges = []
    for clause in clauses:
        atoms = [abs(lit) for lit in clause]
        for i in atoms:
            for j in atoms:
                if i < j:
                    edges.append((i,j))
    return (num_vars, edges)

def _lit2var (lit):
    return "v"+str(abs(lit))

def _lit2expr (lit):
    if lit > 0:
        return "v{0}".format(lit)
    else:
        return "NOT v{0}".format(abs(lit))

def _lit2val (lit):
    return str(lit > 0)

def _var2tab(var,from_tdn):
    if len(from_tdn) > 0:
        return "td_node_{0} t{0}".format(from_tdn[0])
    else:
        return "introduce i{}".format(var)

def _var2tab_col(var,from_tdn):
    if len(from_tdn) > 0:
        return "t{0}.v{1}".format(from_tdn[0],var)
    else:
        return "i{0}.x as v{0}".format(var)

def _var2join(var,from_tdn):
    j = ""
    if len(from_tdn) > 0:
        l = from_tdn[0]
        for i in range(1,len(from_tdn)):
            if i > 1:
                j += " AND "
            j += "t{1}.v{0} = t{2}.v{0}".format(var,l,from_tdn[i])
            l = from_tdn[i]
    return j;

def _dynamic_tabs(node, from_tdn):
    ret = set()
    ret.add("sat_clause")
    for t in from_tdn.values():
        if len(t) > 0:
            for tab in t:
                ret.add("td_node_{}".format(tab))
    for t in node.children:
        ret.add("td_node_{}".format(t.id))
    return ret
