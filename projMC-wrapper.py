#!/usr/bin/env python3
#Wrapper for projMC to enable projMC to use the common input format
import sys
import os.path
import random
import tempfile
import subprocess

import helper # file ./helper.py

fo = tempfile.NamedTemporaryFile(mode='w')
fp = tempfile.NamedTemporaryFile(mode='w')

fi = 1
rarg = 2020
if len(sys.argv) > 3:
    #rarg = int(sys.argv[2])
    rarg = random.randrange(13423423471) #int(sys.argv[2])
    #random.seed(int(sys.argv[2]))
    fi = 3

f = sys.stdin if len(sys.argv) == fi else open(sys.argv[fi], "r")
proj = []
for r in f:
    fo.write(r)
        #print("c {}".format(r)),
    if r.startswith("c ind"):
        var = r.split(" ")[2:-1]
        proj += var

#print proj
fp.write("{0}\n".format(",".join(proj)))
fp.flush()
fp.seek(0)
fp.flush()

fo.flush()
fo.seek(0)
fo.flush()

if len(sys.argv) > fi:
    f.close()

print("c ./bin/projMC {} -fpv={} -rnd-seed{}", fo.name, fp.name, rarg)
pmc = subprocess.Popen([f"{helper.absolutizePath('./bin/projMC')}", "-rnd-init", "-rnd-seed={}".format(rarg), fo.name, "-fpv={0}".format(fp.name)], stdout=sys.stdout, stderr=sys.stderr)
pmc.wait()

fo.close()
fp.close()
sys.exit(pmc.returncode)
