package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.io.AlignmentReaderContextWriter;
import edu.cshl.schatz.jnomics.io.ThreadedStreamConnector;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.File;
import java.io.IOException;


/**
 * Runs Cufflinks. Input must be ReadCollectionWritable
 */

public class CufflinksMap extends AlignmentBaseMap{

    private String cmd, cmdToCopyFilesToHDFS;
    private String cufflinks_op_folder_str, cufflinks_op_folder_on_hdfs_str, cufflinks_ip_str;
    private AlignmentReaderContextWriter reader = null;

    private final JnomicsArgument cufflinks_binary = new JnomicsArgument("cufflinks_binary", "Cufflinks binary", true);
    private final JnomicsArgument cufflinks_op_folder = new JnomicsArgument("cufflinks_output_folder", "Cufflinks output folder", true);
    private final JnomicsArgument cufflinks_opts = new JnomicsArgument("cufflinks_opts", "Cufflinks options",false);
    private final JnomicsArgument cufflinks_ip = new JnomicsArgument("cufflinks_ip_file", "Cufflinks input BAM file", true);

    @Override
    public JnomicsArgument[] getArgs(){
        JnomicsArgument[] superArgs = super.getArgs();
        JnomicsArgument[] newArgs = new JnomicsArgument[]{cufflinks_binary, cufflinks_op_folder, cufflinks_opts, cufflinks_ip};
        return ArrayUtils.addAll(superArgs, newArgs);
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        setup(context);
        align(context);
    }

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        conf.set("mapred.task.timeout", "6000000");
        super.setup(context);

        // Fetch cufflinks binary
        String cufflinks_binary_str = conf.get(cufflinks_binary.getName());

        // Fetch cufflinks output folder
        cufflinks_op_folder_str = conf.get(cufflinks_op_folder.getName());

        // Fetch cufflinks other options
        String cufflinks_opts_str = conf.get(cufflinks_opts.getName());
        cufflinks_opts_str = cufflinks_opts_str == null ? "" : cufflinks_opts_str;

        // Fetch i/p file
        cufflinks_ip_str = conf.get(cufflinks_ip.getName());

        // Fetch o/p folder on HDFS
        cufflinks_op_folder_on_hdfs_str = conf.get("mapred.output.dir");

        cmd = String.format(
                "%s --no-update-check -q -o %s %s ",
                cufflinks_binary_str,
                cufflinks_op_folder_str,
                cufflinks_opts_str);

        cmdToCopyFilesToHDFS = String.format(
                                            "hadoop dfs -copyFromLocal %s/* %s",
                                            cufflinks_op_folder_str,
                                            cufflinks_op_folder_on_hdfs_str
                                            );
    }

    @Override
    public void align(final Context context) throws IOException, InterruptedException {
        System.out.println("Starting cufflinks Process");
        
        Configuration conf = context.getConfiguration();
        FileSystem hdfs = FileSystem.get(conf);
        LocalFileSystem localFs = FileSystem.getLocal(conf);
        
        String[] localDirs = conf.get("mapred.local.dir").split(",");
        String localDir = localDirs[0] + "/work/";
        String[] fileName = cufflinks_ip_str.split("/");
        hdfs.copyToLocalFile(new Path(cufflinks_ip_str), new Path(localDir + fileName[fileName.length-1]));
        String command = cmd + localDir + fileName[fileName.length-1];
        Path cwd = new Path(localDir + cufflinks_op_folder_str);
        
        Process cufflinksProcess = Runtime.getRuntime().exec(command);
        ThreadedStreamConnector cufflinksProcessERRThread = new ThreadedStreamConnector(cufflinksProcess.getErrorStream(), System.err);
        cufflinksProcessERRThread.run();
        int ret = cufflinksProcess.waitFor();
        if( ret != 0 ) {
            System.err.println("Some error occured while running cufflinks. Exiting...");
            return;
        }

        Path opFolder = new Path(cufflinks_op_folder_on_hdfs_str);
        FileStatus [] ipFiles;
        for( int i = 0 ; i < localDirs.length ; i++ ) {
            ipFiles = localFs.listStatus(new Path(localDirs[i] + "/work/" + cufflinks_op_folder_str));
            
            if( ipFiles.length > 0 ) {
                for( int j = 0 ; j < ipFiles.length ; j++ ) {
                    System.out.println(ipFiles[j].getPath());
                    hdfs.copyFromLocalFile(ipFiles[j].getPath(), opFolder);
                }

                return;
            }
        }
    }
}