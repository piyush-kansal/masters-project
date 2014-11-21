#!/usr/bin/env python

# Extension functions for calculating gene expression using TopHat and Cufflinks
# Piyush Kansal

import subprocess
import sys
import os

global HDFS_ROOT, SE_ROOT, BOWTIE2_ROOT
 
# Init regular parameters
HDFS_ROOT = 'geaTC'
SE_ROOT = HDFS_ROOT + '/se/'
BOWTIE2_ROOT = HDFS_ROOT + '/bowtie2/'


# Execute command and handle exceptions
def execCmd(cmd, args = None, ip_file = None, op_file = None):
    try:
        ip_file_p = None
        op_file_p = None

        if ip_file:
            ip_file_p = open(ip_file, 'r')

        if op_file:
            op_file_p = open(op_file, 'w')

        l = cmd.split()
        if args:
            l.append(args)

        r = subprocess.call(l, stdin=ip_file_p, stdout=op_file_p)

        if ip_file_p:
            ip_file_p.close()

        if op_file_p:
            op_file_p.close()

        return r

    except OSError:
        exitProgram(1)


# Exit program gracefully by closing all required files
def exitProgram(status):
    sys.exit(status)


# Create SE Reads on HDFS
def createSEReadsOnHDFS(file_name, index, local_run_count, JAR_PATH):
    print 'Creating single-ended-reads on HDFS ...'

    op_dir_name = '%s/%d/%d/' % (SE_ROOT, index, local_run_count)
    op_file_name = '%s/1' % (op_dir_name)

    cmd = 'hadoop dfs -rmr %s' % (op_dir_name)
    execCmd(cmd)

    cmd = 'hadoop dfs -mkdir %s' % (op_dir_name)
    execCmd(cmd)
    
    cmd = 'hadoop jar %s loader-se %s %s' % (JAR_PATH, file_name, op_file_name)
    r = execCmd(cmd)
    return r


# Process unmapped reads
def processUnmappedReads(i):
    cmd = 'rm -f UNMAPPED-%d*' % (i)
    os.system(cmd)

    cmd = 'hadoop fs -libjars %s -text %s/%d/sort/UNMAPPED-0' % (JAR_PATH, BOWTIE2_ROOT, i)
    out = 'UNMAPPED-%d' % (i)
    execCmd(cmd, None, None, out)

    cmd = 'sort -k1 UNMAPPED-%d' % (i)
    out = 'UNMAPPED-%d.sort' % (i)
    execCmd(cmd, None, None, out)

    cmd = 'awk'
    args = '''{if (NR%2) {read=1;} else {read=2;} printf "@%s/%s\\n%s\\n+\\n%s\\n", $1, read, $10, $11}'''
    ipf = 'UNMAPPED-%d.sort' % (i)
    out = 'UNMAPPED-%d.sort.reads' % (i)
    execCmd(cmd, args, ipf, out)

    cmd = 'grep --no-group-separator -A 3 "@*\/1" UNMAPPED-%d.sort.reads >> UNMAPPED-%d.1.fq' % (i, i)
    os.system(cmd)

    cmd = 'grep --no-group-separator -A 3 "@*\/2" UNMAPPED-%d.sort.reads >> UNMAPPED-%d.2.fq' % (i, i)
    os.system(cmd)

    cmd = 'hadoop dfs -copyFromLocal UNMAPPED-%d.1.fq %s/%d/sort/' % (i, BOWTIE2_ROOT, i)
    execCmd(cmd)

    cmd = 'hadoop dfs -copyFromLocal UNMAPPED-%d.2.fq %s/%d/sort/' % (i, BOWTIE2_ROOT, i)
    execCmd(cmd)

    cmd = 'rm -f UNMAPPED-%d*' % (i)
    os.system(cmd)


# Create merge SAM
def createMergeSAM(i):
    cmd = '''hadoop dfs -ls %s/%d/sort/%s* | awk '{print $8}' ''' % (BOWTIE2_ROOT, i, BOWTIE2_INDEX_PREFIX)
    allFiles = os.popen(cmd).read()
    allFiles = allFiles.split('\n')
    allFiles = filter(None, allFiles)
    allFiles = sorted( allFiles, key=lambda x: int(re.findall(r'\d+$', x)[0]) )
    firstIteration = True

    for curFile in allFiles:
        if firstIteration:
            redirect_oper = '>'
            firstIteration = False
        else:
            redirect_oper = '>>'

        cmd = '''hadoop fs -libjars %s -text %s \
                | awk 'BEGIN {OFS="\t"}{print $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $18, $16, $15, $17, $13, $14, $12, $20, $19}'\
                %s merged-%d.sam ''' % (JAR_PATH, curFile, redirect_oper, i)
        os.system(cmd)


# Create alignments on local m/c
def createAlignmentsOnLocal(readfile_basename, index, local_run_count, JAR_PATH):
    ip_file_name = '%s/se/%d/%d/part-m-00000' % (BOWTIE2_ROOT, index, local_run_count)
    op_file_name = '%s_%d_%d.aln' % (readfile_basename, index, local_run_count)

    cmd = '''hadoop fs -libjars %s -text %s \
        | awk 'BEGIN {OFS="\t"} \
        { sub(/:\[/,"",$1); sub(/\]/,"",$(NF-1)); \
        if(NF == 13) { \
            print $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12; \
        } \
        else if(NF == 20) { \
            print $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $18, $16, $15, $17, $13, $14, $12, $19; \
        } \
        else if(NF == 21) { \
            print $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $18, $19, $16, $15, $17, $13, $14, $12, $20; \
        }}' \
        > %s ''' % (JAR_PATH, ip_file_name, op_file_name)

    r = os.system(cmd)
    if r:
        return None
    else:
        return op_file_name


# Create Bowtie2 alignments on HDFS
def createBowtie2AlignOnHDFS(index, local_run_count, idx_prefix, JAR_PATH, opts):
    print 'Creating bowtie2-aligns on HDFS ...'
    cmd = 'hadoop dfs -rmr %s/se/%d/%d' % (BOWTIE2_ROOT, index, local_run_count)
    execCmd(cmd)

    idx_prefix_name = idx_prefix.split('/')[-1]
    op_dir_name = '%s/%d/%d/' % (SE_ROOT, index, local_run_count)
    op_file_name = '%s/1' % (op_dir_name)

    cmd = 'hadoop jar %s job -archives archive.tar#archive -mapper bowtie2_map -in %s.se -out %s/se/%d/%d -bowtie_index archive/%s -bowtie_binary archive/bowtie2-align -bowtie_opts "%s"' % (JAR_PATH, op_file_name, BOWTIE2_ROOT, index, local_run_count, idx_prefix_name, opts)
    r = os.system(cmd)
    return r
