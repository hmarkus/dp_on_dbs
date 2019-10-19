#!/usr/bin/env python3
import csv
import decimal
import math
import os
from os import listdir
from os.path import isfile, isdir

import matplotlib
matplotlib.use('Agg')
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import xml.etree.ElementTree as ET

import pylab
from matplotlib import scale as mscale

from CustomScale import CustomScale

mscale.register_scale(CustomScale)


def sortSecond(val):
    return val[1]


font = {  # 'weight': 'bold',
    'family': 'serif',
    'size': 12}

matplotlib.rc('font', **font)

solversDeviations = [
    'run_gpusat_ESA-1-gpusat',
    'gpusat-2.0.3-gpusat_array',
    'gpusat-2.0.3-gpusat_tree',
    'gpusat-2.0.3-combined',
    'gpusat-2.0.3-tree-array',
    'dsharp-1-dsharp_counting',
    'approxmc-2-approxmc_counting',
    'approxmc-3-approxmc_counting',
    'query_dnnf-1-query_dnnf',
    'd4-1-d4',
    'sts-1.0.0-sts_new',
]

refFile_ = ['./evals_wo-pre/eval_cobra_All_Count.xml'] #, './eval_w/eval_cobra_weighted.xml']
nameMapper_ = [
    {
        'run_gpusat_ESA-1-gpusat': 'gpusat1',
        'gpusat-2.0.3-combined': 'gpusat2 vbest',
        'gpusat-2.0.3-tree-array': 'gpusat2 array+tree',
        'gpusat-2.0.3-gpusat_array': 'gpusat2 array',
        'gpusat-2.0.3-gpusat_tree': 'gpusat2 tree',
        'dsharp-1-dsharp_counting': 'dsharp',
	#'dpdb-1.0-dpdb' : 'dpdb',
        'sts-1.0.0-sts': 'sts',
        'sdd-2.0-sdd': 'sdd',
        'approxmc-2-approxmc_counting': 'approxmc 2',
        'approxmc-3-approxmc_counting': 'approxmc 3',
        'cnf2eadt-1.0-cnf2eadt': 'cnf2eadt',
        'bdd_minisat_all-1.0.2-bdd_minisat_all': 'bdd_minisat_all',
        'miniC2D-1.0.0-miniC2D_sharp': 'miniC2D',
        'cachet-1.21-cachet_counting': 'cachet',
        'sharpSAT-13.02-sharpSAT_counting': 'sharpSAT',
        'c2d-1-c2d_counting': 'c2d',
        'd4-1-d4_counting': 'd4',
        'countAntom-1.0-countAntom_12': 'countAntom 12',
        'sharpCDCL-1-sharpCDCL_counting': 'sharpCDCL',
    },
    {
        'run_gpusat_ESA-1-gpusat': 'gpusat1',
        'gpusat-2.0.3-gpusat_tree': 'gpusat2',
        'dsharp-1-dsharp_counting': 'dsharp',
        'sts-1.0.0-sts': 'sts',
	'dpdb-1.0-dpdb' : 'dpdb',
        'sdd-2.0-sdd': 'sdd',
        'approxmc-3-approxmc_counting': 'approxmc 3',
        'cnf2eadt-1.0-cnf2eadt': 'cnf2eadt',
        'bdd_minisat_all-1.0.2-bdd_minisat_all': 'bdd_minisat_all',
        'miniC2D-1.0.0-miniC2D_sharp': 'miniC2D',
        'cachet-1.21-cachet_counting': 'cachet',
        'sharpSAT-13.02-sharpSAT_counting': 'sharpSAT',
        'c2d-1-c2d_counting': 'c2d',
        'd4-1-d4_counting': 'd4',
        'countAntom-1.0-countAntom_12': 'countAntom 12',
        #'sharpCDCL-1-sharpCDCL_counting': 'sharpCDCL',
        #'approxmc-2-approxmc_counting': 'approxmc 2',
    },
    {
        'run_gpusat_ESA-1-gpusat': 'gpusat1',
        'gpusat-2.0.3-combined': 'gpusat2 vbest',
        'gpusat-2.0.3-tree-array': 'gpusat2 array+tree',
        'gpusat-2.0.3-gpusat_array': 'gpusat2 array',
        'gpusat-2.0.3-gpusat_tree': 'gpusat2 tree',
	#'dpdb-1.0-dpdb' : 'dpdb',
        'query_dnnf-1-query_dnnf': 'd-DNNF-reasoner ',
        'd4-1-d4': 'd4',
        'sts-1.0.0-sts_new': 'sts',
        'cachet-wmc-1.21-cachet_weighted': 'cachet',
        'miniC2D-1.0.0-miniC2D_weighted': 'miniC2D',
    }
]
colourMapper = {
    'pmc_linux-1-pmc_linux': 'pmc',
    'cnf2eadt-1.0-cnf2eadt': 'maroon',
    'bdd_minisat_all-1.0.2-bdd_minisat_all': 'navy',
    'sdd-2.0-sdd': 'gray',
    'bdd_minisat_all-1.0.1-bdd_minisat_all': 'navy',
    'sdd-1.0.0-sdd': 'gray',
    'sts-1.0.0-sts': 'cyan',
    'dpdb-1.0-dpdb': 'red',
    'sts-1.0.0-sts_new': 'cyan',
    'miniC2D-1.0.0-miniC2D_sharp': 'C7',
    'cachet-1.21-cachet_counting': 'c',
    'approxmc-2-approxmc_counting': 'red',
    'sharpSAT-13.02-sharpSAT_counting': 'gold',
    'dsharp-1-dsharp_counting': 'pink',
    'c2d-1-c2d_counting': 'C6',
    'd4-1-d4_counting': 'C5',
    'd4-1-d4': 'C5',
    'sharpCDCL-1-sharpCDCL_counting': 'C4',
    'approxmc-3-approxmc_counting': 'C3',
    'countAntom-1.0-countAntom_6': 'purple',
    'countAntom-1.0-countAntom_12': 'C2',
    'countAntom-1.0-countAntom_24': 'violet',
    'gpusat-2.0.3-combined': 'black',
    'gpusat-2.0.3-gpusat_tree': 'C0',
    'gpusat-2.0.3-gpusat_array': 'C1',
    'run_gpusat_ESA-1-gpusat': 'deepskyblue',
    'miniC2D-1.0.0-miniC2D_weighted': 'C7',
    'query_dnnf-1-query_dnnf': 'C2',
    'cachet-wmc-1.21-cachet_weighted': 'c',
    'gpusat-2.0.3-tree-array': 'lightgreen',
}

compSolvers_ = [['sharpSAT', 'c2d', 'd4', 'countAntom'], ['cachet-wmc']]
plotsize = (8, 5)

for iterations in [
    #("./plots_wo-pre/", "./evals_wo-pre/", "plot_wo-pre", './eval_Width/eval_htd_all_Count.xml', 600, 1150, nameMapper_[0], refFile_[0], compSolvers_[0]),
    ("./plots_pmc/", "./evals_pmc/", "plot_pmc", './eval_Width/eval_htd_all_Count_pmc.xml', 700, 1260, nameMapper_[1], refFile_[0], compSolvers_[0]),
]:
    summaryFiles = []
    nameMapper = iterations[6]
    compSolvers = iterations[8]
    refFile = iterations[7]
    plot_dir = iterations[0]
    eval_dir = iterations[1]
    prefix = iterations[2]
    decompFile = iterations[3]

    if not isdir(plot_dir):
        os.makedirs(plot_dir)

    for a in listdir(eval_dir):
        summaryFiles += [eval_dir + a]

    fieldnames = ['solver', 'solver_version', 'setting', 'error', 'instance', '#models', 'time', 'timeout', 'SAT']
    field_names_models = set()

    print("width")
    # width:
    instanceWidth = {}
    tree = ET.parse(decompFile)
    root = tree.getroot()

    specs = root.findall('.//project/runspec/')
    for sp in specs:
        runs = sp.findall('.//run')
        for run in runs:
            measures = run.findall('.//measure')
            row = {}
            for measure in measures:
                if measure.get('name') in ['solver', 'instance', 'width']:
                    row[measure.get('name')] = measure.get('val')
            if 'width' in row:
                instanceWidth[row['instance']] = float(row['width'])

    print("ref counts")
    # Reference counts:
    modelCounts = {}
    tree = ET.parse(refFile)
    root = tree.getroot()

    specs = root.findall('.//project/runspec/')

    print("post root")
    for sp in specs:
        break
        runs = sp.findall('.//run')
        for run in runs:
            measures = run.findall('.//measure')
            row = {}
            for measure in measures:
                if measure.get('name') in ['solver', 'instance', '#models']:
                    row[measure.get('name')] = measure.get('val')
            if '#models' in row and row['instance'] not in modelCounts and row['solver'] in compSolvers:
                modelCounts[row['instance']] = decimal.Decimal(row['#models'])
        print("next runs")

    print("iterate over every solver")
    # Iterate over all solvers
    allData = {}
    for summaryFile in summaryFiles:
        tree = ET.parse(summaryFile)
        root = tree.getroot()
        systems = root.findall('.//system')
        for system in systems:
            settings = system.findall('.//setting')
            for setting in settings:
                allData[system.get('name') + "-" + system.get('version') + "-" + setting.get('name')] = {}
		print system.get('name') + "-" + system.get('version') + "-" + setting.get('name')

        specs = root.findall('.//project/runspec/')
        for sp in specs:
            runs = sp.findall('.//run')
            for run in runs:
                measures = run.findall('.//measure')
                row = {}
                for measure in measures:
                    if measure.get('name') in fieldnames:
                        row[measure.get('name')] = measure.get('val')
                allData[row['solver'] + "-" + row['solver_version'] + "-" + row['setting']][row['instance']] = {}
                if 'time' in row:
		    #slv = row['solver'] + "-" + row['solver_version'] + "-" + row['setting']
		    #tmx = float(row['time'])
		    #if slv in allData and row['instance'] in allData[slv] and 'time' in allData[slv][row['instance']]:
		    #    tmx = min(tmx, allData[slv][row['instance']]['time'])
		    #allData[slv][row['instance']]['time'] = tmx
                    allData[row['solver'] + "-" + row['solver_version'] + "-" + row['setting']][row['instance']]['time'] = float(row['time'])
                else:
                    allData[row['solver'] + "-" + row['solver_version'] + "-" + row['setting']][row['instance']]['time'] = 901
                if 'timeout' in row:
                    allData[row['solver'] + "-" + row['solver_version'] + "-" + row['setting']][row['instance']]['timeout'] = row['timeout']
                else:
                    allData[row['solver'] + "-" + row['solver_version'] + "-" + row['setting']][row['instance']]['timeout'] = 1
                if '#models' in row and float(row['time']) < 900:
                    if 'x' in row['#models']:
                        mult = decimal.Decimal(row['#models'].split('x')[0])
                        if '^' in row['#models']:
                            base = decimal.Decimal(row['#models'].split('x')[1].split('^')[0])
                            exponent = decimal.Decimal(row['#models'].split('x')[1].split('^')[1])
                        allData[row['solver'] + "-" + row['solver_version'] + "-" + row['setting']][row['instance']]['#models'] = mult * base ** exponent
                    else:
                        models = row['#models'].replace(",", "")
                        allData[row['solver'] + "-" + row['solver_version'] + "-" + row['setting']][row['instance']]['#models'] = decimal.Decimal(models)
                    if row['instance'] in modelCounts:
                        test = modelCounts[row['instance']]
                        test_ = (allData[row['solver'] + "-" + row['solver_version'] + "-" + row['setting']][row['instance']]['#models'])
                        test2 = test - test_
                        if modelCounts[row['instance']] > 0:
                            allData[row['solver'] + "-" + row['solver_version'] + "-" + row['setting']][row['instance']]['deviation'] = abs((allData[row['solver'] + "-" + row['solver_version'] + "-" + row['setting']][row['instance']]['#models'] - modelCounts[row['instance']]) / modelCounts[row['instance']])
                        elif allData[row['solver'] + "-" + row['solver_version'] + "-" + row['setting']][row['instance']]['#models'] == modelCounts[row['instance']]:
                            allData[row['solver'] + "-" + row['solver_version'] + "-" + row['setting']][row['instance']]['deviation'] = 0
                        else:
                            allData[row['solver'] + "-" + row['solver_version'] + "-" + row['setting']][row['instance']]['deviation'] = -1

    allData["gpusat-2.0.3-combined"] = {}
    for a in allData["gpusat-2.0.3-gpusat_tree"]:
        allData["gpusat-2.0.3-combined"][a] = allData["gpusat-2.0.3-gpusat_tree"][a] if allData["gpusat-2.0.3-gpusat_tree"][a]['time'] < allData["gpusat-2.0.3-gpusat_array"][a]['time'] else allData["gpusat-2.0.3-gpusat_array"][a]

    allData["gpusat-2.0.3-tree-array"] = {}
    for a in allData["gpusat-2.0.3-gpusat_tree"]:
        allData["gpusat-2.0.3-tree-array"][a] = allData["gpusat-2.0.3-gpusat_array"][a] if instanceWidth[a] <= 30 else allData["gpusat-2.0.3-gpusat_tree"][a]

    for i in allData:
        if i in solversDeviations:
            field_names_models.add(i)
    field_names_models = ['instance'] + list(field_names_models)

    with open(plot_dir + 'deviations.csv', 'wb') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=field_names_models)
        writer.writeheader()

    for a in modelCounts:
        row = {'instance': a}
        for i in allData:
            if 'deviation' in allData[i][a] and i in solversDeviations:
                row[i] = allData[i][a]['deviation']
        with open(plot_dir + 'deviations.csv', 'ab') as csvf:
            wr = csv.DictWriter(csvf, fieldnames=field_names_models)
            wr.writerow(row)
            csvf.flush()

    for i in allData:
        for a in allData[i]:
            if 'BE_linux' not in i and 'pmc_linux' not in i and 'run_gpusat_B-E' not in i:
                if 'BE_linux-1-BE_linux' in allData:
                    allData[i][a]['time'] += allData['BE_linux-1-BE_linux'][a]['time']
                if 'pmc_linux-1-pmc_linux' in allData:
                    allData[i][a]['time'] += allData['pmc_linux-1-pmc_linux'][a]['time']
            allData[i][a]['time'] = 910 if allData[i][a]['time'] >= 900 else allData[i][a]['time']

    widthRanges = [(0, 20), (21, 30), (31, 40), (41, 60), (61, 100), (101, 120), (121, 500), (501, 1000)]
    fieldnames = ['solver', "0 - 20", "21 - 30", "31 - 40", "41 - 60", "61 - 100", "101 - 120", "121 - 500", "501 - 1000", "1000 +", "???", "Sum"]
    widthnames = ["0 - 20", "21 - 30", "31 - 40", "41 - 60", "61 - 100", "101 - 120", "121 - 500", "501 - 1000", "1000 +", "???"]
    solverWidth = {}

    with open(plot_dir + prefix + '_width.csv', 'wb') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

    for a in allData:
        if not 'BE_linux-1-BE_linux' in a and 'pmc_linux' not in a and a in nameMapper:
            solverWidth[a] = {}
            for c in fieldnames:
                solverWidth[a][c] = 0
            solverWidth[a]['solver'] = nameMapper[a]
            i = 0
            for b in allData[a]:
                if allData[a][b]['time'] < 900:
                    solverWidth[a]['Sum'] += 1
                    if instanceWidth[b] == -1:
                        solverWidth[a]["???"] += 1
                    else:
                        for r in range(0, len(widthRanges)):
                            if float(instanceWidth[b]) >= widthRanges[r][0] and float(instanceWidth[b]) <= widthRanges[r][1]:
                                solverWidth[a][str(widthRanges[r][0]) + " - " + str(widthRanges[r][1])] += 1
            with open(plot_dir + prefix + '_width.csv', 'ab') as csvf:
                wr = csv.DictWriter(csvf, fieldnames=fieldnames)
                wr.writerow(solverWidth[a])
                csvf.flush()


    def sortDatas(val):
        return val['$\sum$']


    bestSolvers = []

    print("writing tex")

    with open(plot_dir + 'overview.tex', 'w') as tableFile:
        tableFile.write("\\begin{table}[t]\n"
                        "  \\centering\n"
                        "  \\resizebox{.95\columnwidth}{!}{%\n"
                        "  \\begin{tabular}{{l|r||rrrrrrrr||r|r}}\n"
                        "    \\toprule\n"
                        "    solver & prec & 0-20 & 21-30 & 31-40 & 41-50 & 51-60 & $>$60 & best & unique & $\sum$ & rank & time[h] \\\\\n"
                        "    \midrule\n")

        datas = []
        for a in allData:
            if a in nameMapper:
                data = {"solver": a, "0-20": 0, "21-30": 0, "31-40": 0, "41-50": 0, "51-60": 0, "$>$60": 0, "best": 0, "gpusat best": 0, "$\sum$": 0, "rank": 0, "unique": 0, "test": [], "gpusat unique": 0, "time[h]" : float(0)}
                for b in allData[a]:
                    if ("gpusat" not in a and 'd4-1-d4' not in a and 'query_dnnf-1-query_dnnf' not in a) or 'deviation' not in allData[a][b] or allData[a][b]['deviation'] < 0.1:
                        if 0 <= instanceWidth[b] <= 20 and allData[a][b]['time'] < 900:
                            data["0-20"] += 1
                            data["$\sum$"] += 1
                        elif 21 <= instanceWidth[b] <= 30 and allData[a][b]['time'] < 900:
                            data["21-30"] += 1
                            data["$\sum$"] += 1
                        elif 31 <= instanceWidth[b] <= 40 and allData[a][b]['time'] < 900:
                            data["31-40"] += 1
                            data["$\sum$"] += 1
                        elif 41 <= instanceWidth[b] <= 50 and allData[a][b]['time'] < 900:
                            data["41-50"] += 1
                            data["$\sum$"] += 1
                        elif 51 <= instanceWidth[b] <= 60 and allData[a][b]['time'] < 900:
                            data["51-60"] += 1
                            data["$\sum$"] += 1
                        elif allData[a][b]['time'] < 900:
                            data["$>$60"] += 1
                            data["$\sum$"] += 1
                        if allData[a][b]['time'] < 900 and allData[a][b]['time'] <= min([allData[c][b]['time'] for c in allData if c in nameMapper]):
                            data["best"] += 1
                        if allData[a][b]['time'] < 900 and allData[a][b]['time'] <= min([allData[c][b]['time'] for c in allData if c in nameMapper and "gpusat" not in c]):
                            data["gpusat best"] += 1
                        if allData[a][b]['time'] < 900 and len([allData[c][b]['time'] for c in allData if c in nameMapper and allData[c][b]['time'] < 900]) <= 1:
                            data["unique"] += 1
                        if allData[a][b]['time'] < 900 and len([allData[c][b]['time'] for c in allData if c in nameMapper and allData[c][b]['time'] < 900 and "gpusat" not in c]) <= 1:
                            data["gpusat unique"] += 1
                        data["time[h]"] += min(900,allData[a][b]['time']) / 3600.0

                devs = [allData[a][c]['deviation'] for c in allData[a] if 'deviation' in allData[a][c] and (("gpusat" not in a and 'd4' not in a and 'query_dnnf' not in a) or allData[a][c]['deviation'] < 0.01)]
                data["prec"] = 0 #max(devs)
                datas += [data]

        datas.sort(key=sortDatas, reverse=True)
        i = 1
        i_ = 1
        for data in datas:
            if i_ <= 18 or "gpusat" in data['solver']:
                bestSolvers += [data['solver']]
                if "gpusat" not in data['solver']:
                    tableFile.write(("    %s & %s & %s & %s & %s & %s & %s & %s & %s & %s & %s & %s %s \\\\\n" %
                                     (nameMapper[data['solver']],
                                      data['prec'],
                                      data['0-20'],
                                      data['21-30'],
                                      data['31-40'],
                                      data['41-50'],
                                      data['51-60'],
                                      data['$>$60'],
                                      data['best'],
                                      data['unique'],
                                      data['$\sum$'],
                                      str(i),data['time[h]'],)).replace("_", "\_"))
                else:
                    tableFile.write(("    %s & %s & %s & %s & %s & %s & %s & %s & %s/%s & %s/%s & %s & %s & %s \\\\\n" %
                                     (nameMapper[data['solver']],
                                      data['prec'],
                                      data['0-20'],
                                      data['21-30'],
                                      data['31-40'],
                                      data['41-50'],
                                      data['51-60'],
                                      data['$>$60'],
                                      data['best'],
                                      data['gpusat best'],
                                      data['unique'],
                                      data['gpusat unique'],
                                      data['$\sum$'],
                                      str(i),data['time[h]'],)).replace("_", "\_"))
                i += 1
                if "gpusat" not in data['solver']:
                    i_ += 1

        tableFile.write("    \\bottomrule\n"
                        "  \\end{tabular}\n"
                        "  }\n"
                        "\\end{table}")
    frame = None
    for y in [30, 35, 40, 45, 50]:
        types = []
        for i in allData:
            if not 'BE_linux-1-BE_linux' in i and 'pmc_linux' not in i and i in nameMapper and i in bestSolvers:  # and not 'sharpCDCL' in i:  # and not 'approxmc' in i and not 'cachet' in i and not 'cnf2eadt' in i and not 'dsharp' in i :
                frame = sorted([allData[i][x]['time'] for x in allData[i] if x in instanceWidth and y >= instanceWidth[x] >= 0])
                types += [(i, len([a for a in allData[i] if a in instanceWidth and y >= instanceWidth[a] >= 0 and int(allData[i][a]['time']) < 900 and (("gpusat" not in i and 'd4-1-d4' not in i and 'query_dnnf-1-query_dnnf' not in i) or 'deviation' not in allData[i][a] or allData[i][a]['deviation'] < 0.01)]), pd.DataFrame(frame))]

        types.sort(key=sortSecond, reverse=True)

        frame = pd.concat([a[2] for a in types], ignore_index=True, axis=1)
        ax = frame.plot(figsize=plotsize, color=[colourMapper[t[0]] for t in types], legend=False, style='.-', linewidth=1)
        ax.set_xlim(0, iterations[5])
        ax.set_ylim(0, 900)
        ax.legend([nameMapper[a[0]] for a in types], loc='upper left', handletextpad=0.4, handlelength=0.5)
        plt.savefig(plot_dir + prefix + '_' + str(y) + '.pdf', bbox_inches='tight', transparent=True)

        frame = pd.concat([a[2] for a in types], ignore_index=True, axis=1)
        ax = frame.plot(figsize=plotsize, color=[colourMapper[t[0]] for t in types], legend=False, style='.-', linewidth=1)
        ax.set_xlim(iterations[4], iterations[5])
        ax.set_ylim(0, 900)
        ax.legend([nameMapper[a[0]] for a in types], loc='upper left', handletextpad=0.4, handlelength=0.5)
        plt.savefig(plot_dir + prefix + '_' + str(y) + '_enlarged.pdf', bbox_inches='tight', transparent=True)

        frame = pd.concat([a[2] for a in types], ignore_index=True, axis=1)
        ax = frame.plot(figsize=plotsize, color=[colourMapper[t[0]] for t in types], legend=False, style='.-', linewidth=1)
        ax.set_xlim(0, max([a[1] for a in types]) + 4)
        ax.set_ylim(0, 900)
        plt.gca().set_xscale('custom')
        ax.legend([nameMapper[a[0]] for a in types], loc='upper left', handletextpad=0.4, handlelength=0.5)
        plt.xticks([i for i in [0, 400, 600, 800, 900, 1000, 1100, 1150, 1200, 1240] if i < max([a[1] for a in types]) + 4])
        for label in ax.get_xticklabels():
            label.set_rotation(25)
        plt.savefig(plot_dir + prefix + '_' + str(y) + '_exp.pdf', bbox_inches='tight', transparent=True)

        frame = pd.concat([a[2] for a in types], ignore_index=True, axis=1)
        ax = frame.plot(figsize=plotsize, color=[colourMapper[t[0]] for t in types], legend=False, style='.-', linewidth=1)
        ax.set_xlim(iterations[4], max([a[1] for a in types]) + 4)
        ax.set_ylim(0, 900)
        plt.gca().set_xscale('custom')
        ax.legend([nameMapper[a[0]] for a in types], loc='upper left', handletextpad=0.4, handlelength=0.5)
        plt.xticks([i for i in [700, 900, 1000, 1100, 1150, 1200, 1240] if i < max([a[1] for a in types]) + 4])
        for label in ax.get_xticklabels():
            label.set_rotation(25)
        plt.savefig(plot_dir + prefix + '_' + str(y) + '_exp_enlarged.pdf', bbox_inches='tight', transparent=True)

        plt.close('all')

    types = []
    for i in allData:
        if not 'BE_linux-1-BE_linux' in i and 'pmc_linux' not in i and i in nameMapper and i in bestSolvers:
            frame = sorted([allData[i][x]['time'] for x in allData[i]])
            types += [(i, len([a for a in allData[i] if int(allData[i][a]['time']) < 900 and (("gpusat" not in i and 'd4-1-d4' not in i and 'query_dnnf-1-query_dnnf' not in i) or 'deviation' not in allData[i][a] or allData[i][a]['deviation'] < 0.01)]), pd.DataFrame(frame))]

    types.sort(key=sortSecond, reverse=True)

    open(plot_dir + 'solved.txt', "w").close()

    frame = pd.concat([a[2] for a in types], ignore_index=True, axis=1)
    ax = frame.plot(figsize=plotsize, color=[colourMapper[t[0]] for t in types], legend=False, style='.-', linewidth=1)
    ax.set_xlim(0, iterations[5])
    ax.set_ylim(0, 900)
    ax.legend([nameMapper[a[0]] for a in types], loc='upper left', handletextpad=0.4, handlelength=0.5)
    plt.savefig(plot_dir + prefix + '.pdf', bbox_inches='tight', transparent=True)

    frame = pd.concat([a[2] for a in types], ignore_index=True, axis=1)
    ax = frame.plot(figsize=plotsize, color=[colourMapper[t[0]] for t in types], legend=False, style='.-', linewidth=1)
    ax.set_xlim(iterations[4], iterations[5])
    ax.set_ylim(0, 900)
    ax.legend([nameMapper[a[0]] for a in types], loc='upper left', handletextpad=0.4, handlelength=0.5)
    plt.savefig(plot_dir + prefix + '_enlarged.pdf', bbox_inches='tight', transparent=True)

    frame = pd.concat([a[2] for a in types], ignore_index=True, axis=1)
    ax = frame.plot(figsize=plotsize, color=[colourMapper[t[0]] for t in types], legend=False, style='.-', linewidth=1)
    ax.set_xlim(0, max([a[1] for a in types]) + 4)
    ax.set_ylim(0, 900)
    plt.gca().set_xscale('custom')
    ax.legend([nameMapper[a[0]] for a in types], loc='upper left', handletextpad=0.4, handlelength=0.5)
    plt.xticks([i for i in [0, 400, 600, 800, 900, 1000, 1100, 1150, 1200, 1240] if i < max([a[1] for a in types]) + 4])
    for label in ax.get_xticklabels():
        label.set_rotation(25)
    plt.savefig(plot_dir + prefix + '_exp.pdf', bbox_inches='tight', transparent=True)

    plt.close('all')

    frame = pd.concat([a[2] for a in types], ignore_index=True, axis=1)
    ax = frame.plot(figsize=plotsize, color=[colourMapper[t[0]] for t in types], legend=False, style='.-', linewidth=1)
    ax.set_xlim(iterations[4], max([a[1] for a in types]) + 4)
    ax.set_ylim(0, 900)
    plt.gca().set_xscale('custom')
    ax.legend([nameMapper[a[0]] for a in types], loc='upper left', handletextpad=0.4, handlelength=0.5)
    plt.xticks([i for i in [700, 900, 1000, 1100, 1150, 1200, 1240] if i < max([a[1] for a in types]) + 4])
    for label in ax.get_xticklabels():
        label.set_rotation(25)
    plt.savefig(plot_dir + prefix + '_exp_enlarged.pdf', bbox_inches='tight', transparent=True)
    # plt.show()

    plt.close('all')

    frame = pd.concat([pd.DataFrame([1]) for a in nameMapper if a != 'pmc_linux-1-pmc_linux'], ignore_index=True, axis=1)
    ax = frame.plot(figsize=(1, 1), color=[colourMapper[t] for t in nameMapper if t != 'pmc_linux-1-pmc_linux'], style='.-')
    ax.set_xlim(0, 0)
    ax.set_ylim(0, 0)
    plt.axis('off')
    ax.legend([nameMapper[a] for a in nameMapper if a != 'pmc_linux-1-pmc_linux'], loc='upper left', ncol=3, handletextpad=0.4)
    plt.savefig(plot_dir + 'legend.pdf', bbox_inches='tight', transparent=True)

    for a in allData:
        for b in allData:
            if a != b and 'gpusat' in a and 'pmc_linux' not in a and 'pmc_linux' not in b and 'BE_linux' not in a and 'BE_linux' not in b and a in nameMapper and b in nameMapper:
                frame = pd.DataFrame([[allData[a][c]['time'], allData[b][c]['time']] for c in allData[a]], columns=[nameMapper[a], nameMapper[b]])
                ax = frame.plot.scatter(figsize=(8, 8), x=nameMapper[a], y=nameMapper[b], color='black', marker="+", linewidth=3)
                # ax.set_xlim(-10, 910)
                # ax.set_ylim(-10, 910)
                ax.set_xlim(0.1, 1000)
                ax.set_ylim(0.1, 1000)
                ax.set_xscale('log')
                ax.set_yscale('log')
                ax.set_xticklabels([0, 0.1, 1, 10, 100, 1000])
                ax.set_yticklabels([0, 0.1, 1, 10, 100, 1000])
                ax.plot([0, 1], [0, 1], transform=ax.transAxes, color='black')
                plt.savefig(plot_dir + prefix + '_scatter_' + nameMapper[a] + '-' + nameMapper[b] + '.pdf', bbox_inches='tight', transparent=True)
                # plt.show()
                plt.close('all')

    print("finished: " + iterations[0] + "\n")
