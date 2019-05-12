import logging
import sys
import subprocess
import argparse

from dpdb.db import BlockingThreadedConnectionPool, DEBUG_SQL, setup_debug_sql
from dpdb.reader import TdReader
from dpdb.writer import StreamWriter
from dpdb.treedecomp import TreeDecomp
import dpdb.problems as problems

logger = logging.getLogger("main")

def read_cfg(cfg_file):
    import json

    with open(cfg_file) as c:
        cfg = json.load(c)
    return cfg

def solve_problem(problem_type, cfg, fname):
    pool = BlockingThreadedConnectionPool(1,cfg["db"]["max_connections"],**cfg["db"]["dsn"])
    problem = problem_type(fname, pool)
    # Run htd
    p = subprocess.Popen([cfg["htd"]["path"], *cfg["htd"]["parameters"]], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    StreamWriter(p.stdin).write_gr(*problem.prepare_input(fname))
    p.stdin.close()
    p.wait()

    tdr = TdReader.from_stream(p.stdout)
    td = TreeDecomp(tdr.num_bags, tdr.tree_width, tdr.num_orig_vertices, tdr.root, tdr.bags, tdr.adjecency_list)
    logger.debug("#bags: {0} tree_width: {1} #vertices: {2} edges: {3}".format(td.num_bags, td.tree_width, td.num_orig_vertices, td.edges))
    problem.set_td(td)
    problem.setup()
    problem.solve()

_PROBLEM_CLASS = {
    "sat": problems.Sat,
    "#sat": problems.SharpSat,
    "sharpsat": problems.SharpSat,
}

_LOG_LEVEL_STRINGS = ["DEBUG_SQL", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

if __name__ == "__main__":
    setup_debug_sql()

    parser = argparse.ArgumentParser()
    parser.add_argument("problem", help="Type of the problem to solve", choices=_PROBLEM_CLASS.keys())
    parser.add_argument("file", help="Input file for the problem to solve")
    parser.add_argument("--log-level", dest="log_level", help="Log level", choices=_LOG_LEVEL_STRINGS, default="INFO")

    args = parser.parse_args()

    if args.log_level:
        if args.log_level == "DEBUG_SQL":
            log_level = DEBUG_SQL
        else:
            log_level = getattr(logging,args.log_level)

    logging.basicConfig(format='[%(levelname)s] %(name)s: %(message)s', level=log_level)

    cfg = read_cfg("config.json")
    solve_problem(_PROBLEM_CLASS[args.problem], cfg, args.file)
