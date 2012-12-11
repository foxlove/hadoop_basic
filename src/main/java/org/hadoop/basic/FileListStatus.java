package org.hadoop.basic;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * Directory String[] args =>  FileList print.....
 * 
 * @author root
 *
 */
public class FileListStatus
{
    private static final Logger logger = LoggerFactory.getLogger( FileListStatus.class );

    public static void main( String[] args )
        throws Exception
    {
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get( URI.create( uri ), conf );

        Path[] path = new Path[args.length];

        for ( int i = 0; i < args.length; i++ )
        {
            path[i] = new Path( args[i] );
        }

        FileStatus[] status = fs.listStatus( path );
        Path[] listedPaths = FileUtil.stat2Paths( status );

        for ( Path p : listedPaths )
        {
            logger.info( ">> " + p );
        }
    }
}
