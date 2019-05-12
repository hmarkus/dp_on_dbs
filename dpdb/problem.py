import logging
import threading

from dpdb.reader import GrReader
from dpdb.db import DB

logger = logging.getLogger(__name__)

class Problem(object):
    id = None
    td = None

    def __init__(self, name, type, pool):
        self.name = name
        self.type = type
        self.pool = pool
        self.db = DB.from_pool(pool)

    def set_td(self, td):
        self.td = td

    def prepare_input(self, fname):
        input = GrReader.from_file(fname)
        return (input.num_vars, input.edges)

    def set_id(self,id):
        self.id = id
        self.db.set_praefix("p{}_".format(self.id))

    def td_node_column_def(self, var):
        pass

    def td_node_extra_columns(self):
        return []

    def setup(self):
        def init_problem():
            self.db.create_table("problem", [
                ("id", "SERIAL NOT NULL PRIMARY KEY"),
                ("name", "VARCHAR(255) NOT NULL"),
                ("type", "VARCHAR(32) NOT NULL"),
                ("num_bags", "INTEGER"),
                ("tree_width", "INTEGER"),
                ("num_vertices", "INTEGER"),
                ("start_time", "TIMESTAMP"),
                ("end_time", "TIMESTAMP")
            ])
            problem_id = self.db.insert("problem",
                ["name","type","num_bags","tree_width","num_vertices"],
                [self.name,self.type,self.td.num_bags,self.td.tree_width,self.td.num_orig_vertices],"id")[0]
            self.set_id(problem_id)
            logger.info("Created problem with ID %d", self.id)
            self.db.create_table("td_node_status", [
                ("node", "INTEGER NOT NULL PRIMARY KEY"),
                ("start_time", "TIMESTAMP"),
                ("end_time", "TIMESTAMP"),
                ("rows", "INTEGER")
            ])
            for n in self.td.nodes:
                self.db.insert("td_node_status", ["node"],[n.id])
            
        def drop_tables():
            self.db.drop_table("td_bag")
            self.db.drop_table("td_edge")
            for n in self.td.nodes:
                self.db.drop_table("td_node_{}".format(n.id))
        def create_tables():
            for n in self.td.nodes:
                # create all columns and insert null if values are not used in parent
                # this only works in the current version of manual inserts without procedure calls in worker
                self.db.create_table("td_node_{}".format(n.id), [self.td_node_column_def(c) for c in n.vertices] + self.td_node_extra_columns())
                #self.db.create_table("td_node_{}".format(n.id), [self.td_node_column_def(c) for c in n.stored_vertices])
            self.db.create_table("td_edge", [("node", "INTEGER NOT NULL"), ("parent", "INTEGER NOT NULL")])
            self.db.create_table("td_bag", [("bag", "INTEGER NOT NULL"),("node", "INTEGER")])

        def insert_data():
            for b in self.td.nodes:
                for v in b.vertices:
                    self.db.insert("td_bag",("bag","node"), (b.id,v))
            for edge in self.td.edges:
                self.db.insert("td_edge",("node","parent"),(edge[1],edge[0]))

        init_problem()
        drop_tables()
        create_tables()
        insert_data()

    def solve(self):
        self.db.ignore_next_praefix()
        # TODO: should be [("ID = %d",self.id)] and update should correctly resolve placeholders
        self.db.update("problem",["start_time"],["statement_timestamp()"],["ID = {}".format(self.id)])
        self.db.commit()
        workers = []
        events = {}
        for n in self.td.nodes:
            events[n.id] = threading.Event()

        for n in self.td.nodes:
            # TODO: get rid of map()
            waitfor = {c: e for c, e in events.items() if c in map(lambda x: x.id, n.children)}
            w = NodeWorker(self.id,n,events[n.id],waitfor,self.pool)
            workers.append(w)
        for w in workers:
            w.start()
        for w in workers:
            w.join()

        self.db.ignore_next_praefix()
        self.db.update("problem",["end_time"],["statement_timestamp()"],["ID = {}".format(self.id)])
        self.db.commit()

class NodeWorker(threading.Thread):
    def __init__(self, problem, node, finish, children, pool):
        self._problem = problem 
        self._node = node
        self._finish = finish
        self._children = children
        self._pool = pool
        super(NodeWorker,self).__init__()

    def run(self):
        for n, e in self._children.items():
            e.wait()

        db = DB.from_pool(self._pool)
        db.set_praefix("p{}_".format(self._problem))
        logger.debug("Creating records for node %d", self._node.id)
        # undecided if manual update/insert or call procedure...
        # with manual less dependent on database type (procedure language)
        # db.call("create_records",[self._problem,self._node.id])
        db.update("td_node_status",["start_time"],["statement_timestamp()"],["node = {}".format(self._node.id)])
        db.commit()
        assignment_tab = "td_n_{}_assignment".format(self._node.id)
        select = "SELECT * from {0}".format(assignment_tab)
        db.insert_select("td_node_{}".format(self._node.id), db.replace_dynamic_tabs(select,[assignment_tab]))
        db.update("td_node_status",["end_time","rows"],["statement_timestamp()",str(db.last_rowcount)],["node = {}".format(self._node.id)])
        db.commit()
        self._finish.set()
        db.close()

