# -*- coding: utf-8 -*-

import argparse
import subprocess
import json

parser = argparse.ArgumentParser()
parser.add_argument('-v', '--version', help='display version number and exit', action='version', version='%(prog)s 0.0.4')
parser.add_argument('-c', '--config', help='configuration file location', type=argparse.FileType('r'), required=True)
args = parser.parse_args()

cdict = {'params':[], 'samples':[]}
with args.config as o:
    for r in o:
        if r[0] == '#':
            continue
        print r
        temp = r.split(':')
        if temp[0].upper() == 'MIN_LENGTH':
            cdict['min-len'] = temp[1].strip()
        elif temp[0].upper() == 'OUTPUT':
            print '\nOUTPUT'*5
            cdict['output'] = temp[1].strip()
        elif temp[0].upper() == 'PATH_TO_SPADES':
            #print 'setting up p2s'
            cdict['path2spades'] = temp[1].strip()
        elif temp[0].upper() == 'SAMPLE':
            tmp_sample = temp[1].split(',')
            #print tmp_sample
            files = []
            for i in tmp_sample[2:]:
                files.append(i.strip())
            cdict['samples'].append({'name':tmp_sample[0], 'type':tmp_sample[1], 'files':files})
        else:
            print r
            if len(temp) == 2:
                cdict['params'].append({'name':temp[0],'value':temp[1].strip()})
            else:
                cdict['params'].append({'name':temp[0]})
            
print json.dumps(cdict)
subprocess.call(['luigi', '--module', 'diffind-wf', 'clusterise', '--param', json.dumps(cdict)])
