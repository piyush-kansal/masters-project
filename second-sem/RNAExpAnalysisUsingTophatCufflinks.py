#!/usr/bin/env python

# Main script for calculating gene expression using Tophat and Cufflinks
# Piyush Kansal

import sys
import os
import subprocess
from multiprocessing import Process


# Validate number of i/p arguments
def validateNumberOfArguments():
    if len(sys.argv) != 7:
        print "Invalid number of arguments"
        print "Usage:"
        print "RNAExpAnalysisUsingTophatCufflinks.py <fasta-file> <number-of-experiments> <jnomics-jar> <binaries-location> <tophat-op-files-path-on-hdfs> <cufflinks-op-files-path-on-hdfs>"
        print "NOTE: Experiment names should be in increasing order of number, starting from 1. For eg,"
        print "1.1.fq\n1.2.fq"
        print "2.1.fq\n2.2.fq"
        sys.exit(1)


def copyBinariesInCurrentDir():
    cmd = 'cp %s/cuff* %s/bowtie2* %s/samtools .' % (BINS_PATH, BINS_PATH, BINS_PATH)
    os.system(cmd)


# Accepts user i/p, validate them and initialize required params
def acceptUserInput():
    validateNumberOfArguments()

    # Declare params global
    global FST_FILE, EXP_CONT, JAR_PATH, BINS_PATH, BOWTIE2_INDEX_PREFIX, TOPH_PATH, CUFF_PATH, _NUM_PARALLEL_PROCESSES_

    # Initialize i/p arguments
    FST_FILE = sys.argv[1]
    if not os.path.isfile(FST_FILE):
        print 'File %s does not exit on the file system' % (FST_FILE)
        sys.exit(1)

    BOWTIE2_INDEX_PREFIX = FST_FILE.split('.')[0]
    EXP_CONT = int(sys.argv[2])

    JAR_PATH = sys.argv[3]
    if not os.path.isfile(JAR_PATH):
        print 'File %s does not exit on the file system' % (JAR_PATH)
        sys.exit(1)

    BINS_PATH = sys.argv[4]
    if not os.path.isdir(BINS_PATH):
        print 'Directory %s does not exit on the file system' % (BINS_PATH)
        sys.exit(1)
    else:
        copyBinariesInCurrentDir()
    
    FNULL = open(os.devnull, 'w')

    TOPH_PATH = sys.argv[5]
    cmd = 'hadoop dfs -ls %s' % (TOPH_PATH)
    try:
        subprocess.check_call(cmd.split(), stdout=FNULL, stderr=subprocess.STDOUT)
        print 'Directory %s already exits on HDFS. Either remove it or give another directory name' % (TOPH_PATH)
        sys.exit(1)
    except subprocess.CalledProcessError:
        pass

    CUFF_PATH = sys.argv[6]
    cmd = 'hadoop dfs -ls %s' % (CUFF_PATH)
    try:
        subprocess.check_call(cmd.split(), stdout=FNULL, stderr=subprocess.STDOUT)
        print 'Directory %s already exits on HDFS. Either remove it or give another directory name' % (CUFF_PATH)
        sys.exit(1)
    except subprocess.CalledProcessError:
        pass

    FNULL.close()
    _NUM_PARALLEL_PROCESSES_ = 10


# Renames i/p files
def renameIpFiles():
    i = 1
    while i <= EXP_CONT:
        if os.path.isfile('t%d.1.fq' % (i)):
            cmd = 'mv t%d.1.fq %d.1.fq' % (i, i)
            os.system(cmd)
        
        if os.path.isfile('t%d.2.fq' % (i)):
            cmd = 'mv t%d.2.fq %d.2.fq' % (i, i)
            os.system(cmd)

        i += 1


def createBowtie2Index():
    print "Creating Bowtie2 Index ..."
    cmd = 'bowtie2-build %s %s' % (FST_FILE, BOWTIE2_INDEX_PREFIX)
    r = os.system(cmd)
    if r:
        print 'Some error occured while creating Bowtie2 Index. Exiting ...'
        sys.exit(1)


def createArchives():
    cmd = 'tar -cf archive.tar %s* bowtie2*' % (BOWTIE2_INDEX_PREFIX)
    os.system(cmd)

    cmd = 'tar -cf archive2.tar cuff*'
    os.system(cmd)


def processViaTophatAndCufflinks(i):
    cmd = 'tophat_jnomics -o tophat_t%d %s %d.1.fq %d.2.fq %s %s %d' % (i, BOWTIE2_INDEX_PREFIX, i, i, BINS_PATH, JAR_PATH, i)
    r = os.system(cmd)
    if r:
        print "Some error occured while creating tophat output. Exiting ..."
        sys.exit(1)

    cmd = 'hadoop dfs -mkdir %s/%d' % (TOPH_PATH, i)
    r = os.system(cmd)

    cmd = 'hadoop dfs -copyFromLocal tophat_t%d/accepted_hits.bam %s/%d' % (i, TOPH_PATH, i)
    r = os.system(cmd)
    if r:
        print "Some error occured while creating tophat output. Exiting ..."
        sys.exit(1)

    cmd = 'rm -fR tophat_t%d' % (i)
    os.system(cmd)

    cmd = 'rm -f left_kept_reads_%d*' % (i)
    os.system(cmd)

    cmd = 'rm -f left_kept_reads_seg?_%d*' % (i)
    os.system(cmd)

    cmd = 'rm -f right_kept_reads_%d*' % (i)
    os.system(cmd)

    cmd = 'rm -f right_kept_reads_seg?_%d*' % (i)
    os.system(cmd)

    cmd = 'hadoop jar %s job -archives archive2.tar#archive2 -mapper cufflinks_map -in geaTC/bowtie2/se/1/1/part-m-00000 -out %s/%d -cufflinks_binary archive2/cufflinks -cufflinks_ip_file %s/%d/accepted_hits.bam -cufflinks_output_folder cufflinks_t%d' % (JAR_PATH, CUFF_PATH, i, TOPH_PATH, i, i)
    print cmd
    r = os.system(cmd)
    if r:
        print "Some error occured while creating cufflinks output. Exiting ..."
        sys.exit(1)

    cmd = 'rm -fR cufflinks_t%d' % (i)
    os.system(cmd)


def callTophatAndCufflinks():
    cmd = 'hadoop dfs -mkdir %s' % (TOPH_PATH)
    os.system(cmd)

    i = 1
    processID = [None]*EXP_CONT
    while i <= EXP_CONT:
        processID[i-1] = Process(target=processViaTophatAndCufflinks, args=(i,))
        processID[i-1].start()

        if i%_NUM_PARALLEL_PROCESSES_ == 0:
            j = 1
            while j <= _NUM_PARALLEL_PROCESSES_:
                processID[i-_NUM_PARALLEL_PROCESSES_ + j-1].join()
                j += 1

        i += 1


def cleanup():
    cmd = 'rm -f *.bt2'
    os.system(cmd)

    cmd = 'rm -f archive*.tar'
    os.system(cmd)


# Main program
if __name__ == "__main__":
    acceptUserInput()
    renameIpFiles()
    createBowtie2Index()
    createArchives()
    callTophatAndCufflinks()
    cleanup()
    print "Program finished successfully"
    sys.exit(0)