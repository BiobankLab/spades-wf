# -*- coding: utf-8 -*-

import luigi
import subprocess
from fatool import *

#if clear flag set firs task
class clear_cut(luigi.Task):
    param = luigi.DictParameter()
    
    def requires(self):
        return []
        
    def run(self):
        for r in self.param['samples']:
            for f in r['files']:
                fa2clr = Fa.load_from_file(f)
                nf = fa2clr.cut_min_len(self.param['minlen']
                tmp_file = f.rsplit(',',1)
                nf.write(tmp_file[0]+'_cleared.'+tmp_file[1])
                
        
    def output(self):
        rarray = []
        for r in self.param['samples']:
            for f in r['files']:
                tmp_file = f.rsplit(',',1)
                rarray.append(luigi.LocalTarget(tmp_file[0]+'_cleared.'+tmp_file[1])
        return rarray
        
class spades(luigi.Task):
    param = luigi.DictParameter()
    
    def requires(self):
        return [clear_cut(self.param)]
        
    def run(self):
        params = []
        
        #preparing params to call spades same for every run
        for r in self.param['params']:
            if 'value' in r:
                params += [r['name'], r['value']]
            else:
                params.append(r['name'].strip())
        if 'output' in self.param:
             output = self.param['output'].rstrip(' /')
        else:
            output = ''
                    
        
        #preparing files pe|s important        
        for r in self.param['samples']:
            q = 1
            files = []
            if r['type'] == 'pe':
                #pairend
                for f in r['files']:
                    tmp_file = f.rsplit('.',1)
                    files += ['pe-1-'+str(q), tmp_file[0]+'_cleared.'+tmp_file[1]]
                    if q == 1:
                        q = 2
                    else:
                        q = 1
            elif r['type'] == 's':
                for f in r['files']:
                    tmp_file = f.rsplit('.',1)
                    files += ['s'+str(q), tmp_file[0]+'_cleared.'+tmp_file[1]]
                    q += 1
        
            subprocess.call(['python', self.param['path2spades']+'spades.py']+params+files+['-0',output+'/'+r['name'].replace(' ','_')])
        '''
        ​python spades.py -k 21,33,55,77,99 --careful --s1 /media/blul/HS/FASTQ/Genomy/Bakteriofagi/ZIB_UL/72A/TRIM/72-A_S17_L001_R1_001_trimmed.fq --s2 /media/blul/HS/FASTQ/Genomy/Bakteriofagi/ZIB_UL/72A/TRIM/72-A_S17_L002_R1_001_trimmed.fq --s3 /media/blul/HS/FASTQ/Genomy/Bakteriofagi/ZIB_UL/72A/TRIM/72-A_S17_L003_R1_001_trimmed.fq --s4 /media/blul/HS/FASTQ/Genomy/Bakteriofagi/ZIB_UL/72A/TRIM/72-A_S17_L004_R1_001_trimmed.fq -o ../72-A_run -t 32
        '''
        
    def output(self):
        
        rlist = []
        if 'output' in self.param:
             output = self.param['output'].rstrip(' /')
        else:
            output = ''
        for r in self.param['samples']:
            rlist.append(luigi.LocalTarget(output+'/'+r['name']+'/contigs.fasta'))
        return rlist
        
        
class single_spades(luigi.Task):
    param = luigi.DictParameter()
    
    def requires(self):
        return []
        
    def run(self):
        params = []
        
        #preparing params to call spades same for every run
        for r in self.param['params']:
            if 'value' in r:
                params += [r['name'], r['value']]
            else:
                params.append(r['name'].strip())
        if 'output' in self.param:
             output = self.param['output'].rstrip(' /')
        else:
            output = ''
                    
        
        #preparing files pe|s important        
        for r in self.param['samples']:
            q = 1
            files = []
            if r['type'] == 'pe':
                #pairend
                for f in r['files']:
                    tmp_file = f.rsplit('.',1)
                    files += ['pe-1-'+str(q), tmp_file[0]+'_cleared.'+tmp_file[1]]
                    if q == 1:
                        q = 2
                    else:
                        q = 1
            elif r['type'] == 's':
                for f in r['files']:
                    tmp_file = f.rsplit('.',1)
                    files += ['s'+str(q), tmp_file[0]+'_cleared.'+tmp_file[1]]
                    q += 1
        
            subprocess.call(['python', self.param['path2spades']+'spades.py']+params+files+['-0',output+'/'+r['name'].replace(' ','_')])
        
    def output(self):
        rlist = []
        if 'output' in self.param:
             output = self.param['output'].rstrip(' /')
        else:
            output = ''
        for r in self.param['samples']:
            rlist.append(luigi.LocalTarget(output+'/'+r['name']+'/contigs.fasta'))
        return rlist
