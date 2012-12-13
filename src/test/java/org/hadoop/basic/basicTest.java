package org.hadoop.basic;

import junit.framework.Assert;

import org.apache.hadoop.io.IOUtils;
import org.hadoop.util.Utils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class basicTest
{

    private final static Logger logger = LoggerFactory.getLogger( basicTest.class );

    @Test
    public void wordCount()
    {
        try
        {

            String[] args = { "/usr/local/hadoop-0.20.1/conf/hadoop-env.sh", "output-wordcount/" };

            Utils.removeDIR( "/root/workspace/hadoop_basic/" + args[1] );

            WordCount.main( args );

        }
        catch ( Exception e )
        {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void readFile()
    {
        try
        {
            String[] args = { "hdfs://192.168.140.128:9000/user/root/input/mapred-site.xml" };

            URLCat_readFile.main( args );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void FileCopy()
    {
        try
        {
            String[] args = {
                "/usr/local/hadoop-0.20.1/build.xml",
                "hdfs://192.168.140.128:9000/user/root/output/copy_build.xml" };
            
            String[] output = {"hdfs://192.168.140.128:9000/user/root/output/"};

            FileCopyWithProgress.main( args );

            logger.debug( "========  shell command ============ " );
            //callShellCommand( "/usr/local/hadoop-0.20.1/bin/hadoop fs -ls output/", true );
            
            logger.debug( "======== List FileStatus ============ " );
            FileListStatus.main( output);
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            Assert.fail();
        }
    }

    private void callShellCommand( String command, boolean display )
        throws Exception
    {
        java.lang.Runtime runTime = java.lang.Runtime.getRuntime();
        java.lang.Process process = runTime.exec( command );

        System.out.println( process.waitFor() );

        if ( display )
        {
            IOUtils.copyBytes( process.getInputStream(), System.out, 4096, false );
        }
    }
}
