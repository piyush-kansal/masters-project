masters-project
=============

- This project is part of my 2 semester long Masters project with Cold Spring Harbor Laboratory, New York under the guidance of Prof. Michael Schatz

- The aim is to build a pipeline and to add new functionality in an open source genome analysis suite Jnomics, http://sourceforge.net/apps/mediawiki/jnomics/index.php?title=Jnomics

- As a part of first semester, I implemented following:
	- to globally sort the alignments produced by bwa/bowtie2 program
	- to extract these alignments from HDFS to local file system


- As a part of second semester, I implemented following:
	- to leverage Tophat and Cufflinks and plug them in Jnomics to do RNA expression analysis much more efficiently in terms of time
	- reduced the traditional execution time from 45 hours to just 2 hours !!


- Used MapReduce, Hadoop, Java, sequence aligners Bowtie2 and Bwa, sequence storage map Samtools, Linux

- Note: The code present in this repository is only for the part which I developed. So, it has to plugged in into the core Jnomics framework to build and use it
