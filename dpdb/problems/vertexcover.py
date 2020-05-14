# -*- coding: future_fstrings -*-
import logging

from dpdb.reader import TdReader, TwReader, EdgeReader
from dpdb.problem import *

logger = logging.getLogger(__name__)

class VertexCover(Problem):

    def __init__(self, name, pool, input_format, **kwargs):
        self.input_format = input_format
        super().__init__(name, pool, **kwargs)

    def td_node_column_def(self,var):
        return (var2col(var), "BOOLEAN")

    def td_node_extra_columns(self):
        return [("size","INTEGER")]
        
    def candidate_extra_cols(self,node):
        introduce = [var2size(node,v) for v in node.vertices if node.needs_introduce(v)]
        join = [node2size(n) for n in node.children]

        q = ""
        if introduce:
            q += " + ".join(introduce)
            if join:
                q += " + "
        if join:
            q += "{}".format(" + ".join(join))
            if len(join) > 1:
                children = [vc for c in node.children for vc in c.vertices if vc in node.vertices]
                duplicates = ["case when {} then 1 else 0 end * {}".format(
                                    var2tab_col(node,var,False),len(node.vertex_children(var))-1) 
                                for var in set(children) if len(node.vertex_children(var)) > 1]

                if duplicates:
                    q += " - ({})".format(" + ".join(duplicates))

        if not introduce and not join:
            q += "0"

        q += " AS size"

        return [q]

    def assignment_extra_cols(self,node):
        return ["min(size) AS size"]

    def filter(self, node):
        check = []

        nv = []
        for c in node.vertices:
            [nv.append((c,v)) for v in self.edges[c] if v in node.vertices and (v,c) not in nv]

        for edge in nv:
            check.append(" OR ".join(map(var2col, edge)))
        if check:
            return "WHERE ({})".format(") AND (".join(check))
        else:
            return ""


    def setup_extra(self):
        def create_tables():
            self.db.ignore_next_praefix()
            self.db.create_table("problem_vertexcover", [
                ("id", "INTEGER NOT NULL PRIMARY KEY REFERENCES PROBLEM(id)"),
                ("size", "INTEGER")
            ])

        def insert_data():
            self.db.ignore_next_praefix(1)
            self.db.insert("problem_vertexcover",("id",),(self.id,))

        create_tables()
        insert_data()

    def prepare_input(self, fname):
        if self.input_format == "td":
            input = TdReader.from_file(fname)
        elif self.input_format == "tw":
            input = TwReader.from_file(fname)
        elif self.input_format == "edge":
            input = EdgeReader.from_file(fname)
        self.num_vertices = input.num_vertices
        self.edges = input.adjacency_list

        return (input.num_vertices, input.edges)

    def after_solve(self):
        root_tab = f"td_node_{self.td.root.id}"
        size_sql = self.db.replace_dynamic_tabs(f"(select coalesce(min(size),0) from {root_tab})")
        self.db.ignore_next_praefix()
        size = self.db.update("problem_vertexcover",["size"],[size_sql],[f"ID = {self.id}"],"size")[0]
        logger.info("Min vertex cover size: %d", size)

def var2size(node,var):
    if node.needs_introduce(var):
        return "case when {} then 1 else 0 end".format(var2tab_col(node,var,False))
    else:
        return "{}.size".format(var2tab_alias(node,var))

def node2size(node):
    return "{}.size".format(node2tab_alias(node))

args.specific[VertexCover] = dict(
    help="Solve vertex cover instances (min VC)",
    aliases=["vc"],
    options={
        "--input-format": dict(
            dest="input_format",
            help="Input format",
            choices=["td","tw","edge"],
            default="td"
        )
    }
)

