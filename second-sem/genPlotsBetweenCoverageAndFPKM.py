#!/usr/bin/env python

# Main script for generating plots between coverage and FPKM values
# Piyush Kansal

import sys
import os
import subprocess
import csv
import rpy2.robjects as robjects
import pickle


# Validate number of i/p arguments
def validateNumberOfArguments():
    if len(sys.argv) != 5:
        print "Invalid number of arguments"
        print "Usage:"
        print "genPlotsBetweenCoverageAndFPKM.py <number-of-experiments> <cufflinks-op-files-path-on-hdfs> <expression-matrix-path> <ref-ptt-file>"
        sys.exit(1)


# Accepts user i/p, validate them and initialize required params
def acceptUserInput():
    validateNumberOfArguments()
    FNULL = open(os.devnull, 'w')

    # Declare params global
    global EXP_CONT, CUFF_PATH, EXPM_PATH, REF_FILE, PLOT_FILE
    global coverageVals, fpkmVals, ranges

    # Initialize i/p arguments
    EXP_CONT = int(sys.argv[1])
    
    CUFF_PATH = sys.argv[2]
    cmd = 'hadoop dfs -ls %s' % (CUFF_PATH)
    try:
        subprocess.check_call(cmd.split(), stdout=FNULL, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError:
        print 'Directory %s does not exits on HDFS.' % (CUFF_PATH)
        sys.exit(1)

    FNULL.close()

    EXPM_PATH = sys.argv[3]
    if not os.path.isfile(EXPM_PATH):
        print 'File %s does not exits on file system.' % (EXPM_PATH)
        sys.exit(1)

    REF_FILE = sys.argv[4]
    if not os.path.isfile(REF_FILE):
        print 'File %s does not exits on file system.' % (REF_FILE)
        sys.exit(1)

    PLOT_FILE = 'XY.plot'


def copyFilesInCurrentDir():
    i = 1
    while i <= EXP_CONT:
        cmd = 'hadoop dfs -copyToLocal %s/%d/genes.fpkm_tracking genes.fpkm_tracking.%d' % (CUFF_PATH, i, i)
        r = os.system(cmd)
        if r:
            print 'Some error occured while copying Cufflinks output on local file system for i=%d. Exiting ...' % (i)
            sys.exit(1)

        i += 1

    cmd = 'cp %s .' % (EXPM_PATH)
    os.system(cmd)

    cmd = 'cp %s .' % (REF_FILE)
    os.system(cmd)


def rangeExists(s, e):
    for index, r in enumerate(ranges):
        if s >= long(r[0]) and s < long(r[1]):
            return index

        if s < long(r[0]):
            return None

    return None


def genValues():
    coverageFile = open(EXPM_PATH, 'r')
    records = csv.reader(coverageFile, delimiter=' ')

    i = 0
    coverageVals = []
    while i < EXP_CONT:
        coverageVals.append(','.join([r[i] for r in records]))
        coverageFile.seek(0)
        i += 1

    coverageFile.close()

    refFile = open(REF_FILE, 'r')
    records = csv.reader(refFile, delimiter='\t')
    ranges = [tuple(r[0].split('..')) for r in records]
    refFile.close()

    i = 1
    fpkmVals = []
    while i <= EXP_CONT:
        fpkmFileName = 'genes.fpkm_tracking.%d' % (i)
        fpkmFile = open(fpkmFileName, 'r')
        records = csv.reader(fpkmFile, delimiter='\t')

        j = 1
        row = ['0']*len(ranges)
        for r in records:
            if j == 1:
                j = 2
                continue
            else:
                s,e = r[6].split(':')[1].split('-')
                ret = rangeExists(long(s), e)
                if ret:
                    row[ret] = r[9]

        fpkmVals.append(','.join(row))
        fpkmFile.close()
        i += 1


def writeValsToFile():
    oFile = open(PLOT_FILE, 'wb')
    pickle.dump(coverageVals, oFile)
    pickle.dump(fpkmVals, oFile)
    oFile.close()


def genPlots():
    iFile = open(PLOT_FILE, 'rb')
    coverageVals = pickle.load(iFile)
    fpkmVals = pickle.load(iFile)
    iFile.close()

    r = robjects.r
    i = 0
    while i < EXP_CONT:
        x = robjects.IntVector([long(item) for item in coverageVals[i].split(',')])
        y = robjects.FloatVector([float(item) for item in fpkmVals[i].split(',')])
        r.jpeg('t%d.jpg' % (i+1))
        r.plot(x, y, xlab="Coverage", ylab="FPKM")
        i += 1


def cleanup():
    cmd = 'rm -f genes.fpkm_tracking*'
    os.system(cmd)


# Main program
if __name__ == "__main__":
    acceptUserInput()
    copyFilesInCurrentDir()
    genValues()
    writeValsToFile()
    cleanup()
    genPlots()
    print "Program finished successfully"
    sys.exit(0)