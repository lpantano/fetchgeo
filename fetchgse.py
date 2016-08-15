#!/usr/bin/env python
# encoding: utf-8
"""
Downloads gene expression raw data from NCBI's Gene Expression Omnibus (GEO)

Ex: #> python fetchgse.py GSE68599

"""

import sys
import os
import time
import gzip
import subprocess
from argparse import ArgumentParser

import pandas as pd

HOMEDIR = os.getcwd()

def get_soft_url(gse, datatype):
	"""Get soft file depending on file type and ID"""
	if datatype == 'GSE':
            nnn = "%snnn" % gse[:-3]
            soft_file = "ftp://ftp.ncbi.nlm.nih.gov/geo/series/%s/%s/matrix/%s_series_matrix.txt.gz" % (nnn, gse, gse)
            return soft_file

def parse_soft(softzip):
	""" Take a SOFT gz file and parse it for specific data parse_soft
		Dictionary with platform and available suppfile(s) when available
		Type: results['platform'] (string)
		Type: results['suppfiles'] (list)
	"""
	z = gzip.open(softzip, 'rb')
	results = {}
	suppfiles = []
        colmeta = []
        metadata = []
	platform = ''
	for line in z:
	# Get platform title
            line = line.strip()
            if line.startswith("!Series_title"):
                results["title"] = line.replace("!Series_title", "")
            if line.startswith('!Series_platform_id'):
                index = line.replace('!Series_platform_id', "")
                platform = line
            if line.startswith('!Series_supplementary_file'):
                index = line.replace('!Series_supplementary_file', "")
                suppfiles.append(line.strip())
            if line.startswith('!Sample_title'):
                results['samples'] = line.replace("\"", "").split("\t")[1:]
            if line.startswith('!Sample_geo_accession'):
                results['samples_geo'] = line.split("\t")[1:]
            if line.startswith('!Sample_characteristics_ch1'):
                values = line.replace("\"", "").split("\t")[1:]
                colmeta.append(values[1].split(": ")[0].replace(" ", "_"))
                values = [v.split(": ")[1] for v in values]
                metadata.append(values)
            if line.startswith('!series_matrix_table_begin'):
                line = z.next().strip()
                results['header'] = line.replace("\"", "").split()
                line = z.next().strip()
                results['table'] = list()
                while not line.startswith("!series_matrix_table_end"):
                    results['table'].append(line.replace("\"", "").split())
                    line = z.next()
        results['meta_header'] = colmeta
        results['metadata'] = metadata
	results['platform'] = platform.strip()
	results['suppfiles'] = suppfiles
	return results

def urlretrieve(urlfile, fpath):
    """Download with wget"""
    cmd = "wget -O {0} {1}".format(fpath, urlfile)
    print cmd
    if not os.path.exists(fpath):
        p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, stderr = p.communicate()
        p.stdout.close()
        p.stderr.close()
    if os.path.exists(fpath):
        return fpath

def write_data(res):
    df = pd.DataFrame(res['table'], columns=res['header'])
    table_fn = "expression.csv"
    df.to_csv(table_fn, index=False)
    df = pd.DataFrame(res['metadata'], index=res['meta_header'], columns=res['samples']).T
    table_fn = "metadata.csv"
    df.to_csv(table_fn)
    return None

if __name__ == "__main__":
    parser = ArgumentParser(description="Download GSE data")
    parser.add_argument("--gse", help = "GSE ID like GSENNNNN")
    parser.add_argument("--out", help = "folder to put the data")
    args = parser.parse_args()

    if not os.path.exists(args.out):
        os.mkdir(args.out)
    os.chdir(args.out)
    url = get_soft_url(args.gse, "GSE")
    matrix = urlretrieve(url, "%s.txt.gz" % args.gse)
    res = parse_soft(matrix)
    write_data(res)
