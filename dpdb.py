import logging
import sys
import subprocess
import argparse

from dpdb.db import BlockingThreadedConnectionPool, DEBUG_SQL, setup_debug_sql
from dpdb.reader import TdReader
from dpdb.writer import StreamWriter, FileWriter
from dpdb.treedecomp import TreeDecomp
import dpdb.problems as problems

logger = logging.getLogger("main")

def read_cfg(cfg_file):
    import json

    with open(cfg_file) as c:
        cfg = json.load(c)
    return cfg

def solve_problem(cfg, problem_type, file, **kwargs):
    pool = BlockingThreadedConnectionPool(1,cfg["db"]["max_connections"],**cfg["db"]["dsn"])
    problem = _PROBLEM_CLASS[problem_type](file, pool, **kwargs)
    # Run htd
    p = subprocess.Popen([cfg["htd"]["path"], *cfg["htd"]["parameters"]], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    input = problem.prepare_input(file)
    if "gr_file" in kwargs and kwargs["gr_file"]:
        with FileWriter(kwargs["gr_file"]) as fw:
            fw.write_gr(*input)
    StreamWriter(p.stdin).write_gr(*input)
    p.stdin.close()
    p.wait()

    tdr = TdReader.from_stream(p.stdout)
    td = TreeDecomp(tdr.num_bags, tdr.tree_width, tdr.num_orig_vertices, tdr.root, tdr.bags, tdr.adjacency_list)
    logger.debug("#bags: {0} tree_width: {1} #vertices: {2} edges: {3}".format(td.num_bags, td.tree_width, td.num_orig_vertices, td.edges))
    if "td_file" in kwargs and kwargs["td_file"]:
        with FileWriter(kwargs["td_file"]) as fw:
            fw.write_td(tdr.num_bags, tdr.tree_width, tdr.num_orig_vertices, tdr.root, tdr.bags, td.edges)
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

    prob_args = {}
    parser = argparse.ArgumentParser()
    parser.add_argument("problem_type", help="Type of the problem to solve", choices=_PROBLEM_CLASS.keys())
    parser.add_argument("file", help="Input file for the problem to solve")
    parser.add_argument("--config", help="Config file", default="config.json")
    parser.add_argument("--log-level", dest="log_level", help="Log level", choices=_LOG_LEVEL_STRINGS, default="INFO")
    parser.add_argument("--limit-result-rows", type=int, dest="limit_result_rows", help="Limit number of result rows per table")
    parser.add_argument("--randomize-rows", action="store_true", dest="randomize_rows", help="Randomize rows (useful with --limit-result-rows)")
    parser.add_argument("--td-file", dest="td_file", help="Store TreeDecomposition file (htd Output)")
    parser.add_argument("--gr-file", dest="gr_file", help="Store TreeDecomposition file (htd Input)")

    args = parser.parse_args()

    if args.log_level:
        if args.log_level == "DEBUG_SQL":
            log_level = DEBUG_SQL
        else:
            log_level = getattr(logging,args.log_level)

    logging.basicConfig(format='[%(levelname)s] %(name)s: %(message)s', level=log_level)

    cfg = read_cfg(args.config)

    solve_problem(cfg,**vars(args))
