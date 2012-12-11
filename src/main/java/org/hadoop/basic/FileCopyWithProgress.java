package org.hadoop.basic;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *   result : hadoop fs -ls output/
 */
public class FileCopyWithProgress
{
    private static final Logger logger = LoggerFactory.getLogger( FileCopyWithProgress.class );
    
    public static void main( String[] args )
        throws Exception
    {
        String localSrc = args[0];
        String dst = args[1];

        InputStream in = new BufferedInputStream( new FileInputStream( localSrc ) );

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get( URI.create( dst ), conf );
        OutputStream out = fs.create( new Path( dst ), new Progressable()
        {

            @Override
            public void progress()
            {
                System.out.print( ">>" );
            }
        } );

        IOUtils.copyBytes( in, out, 4096, true );
        
        logger.info( "=============   END  ====================" );
    }
}
