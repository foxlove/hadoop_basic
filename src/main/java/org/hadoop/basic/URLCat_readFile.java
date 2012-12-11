package org.hadoop.basic;

import java.io.InputStream;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * arg0 : hdfs://192.168.140.128:9000/user/root/input/mapred-site.xml
 * ==>hdfs://192.168.140.128:9000/ -> $HADOOP_HOME/conf/core-site.xml
 * 
 * @author root
 */
public class URLCat_readFile
{
    private final static Logger logger = LoggerFactory.getLogger( URLCat_readFile.class );

    static
    {
        URL.setURLStreamHandlerFactory( new FsUrlStreamHandlerFactory() );
    }

    public static void main( String[] args )
        throws Exception
    {
        logger.debug( ">>>>>>>>>>>>>>>>>>>>>> URL open Start." );
        hadoopURL( args );
        logger.debug( ">>>>>>>>>>>>>>>>>>>>>> FileSystem open Start." );
        hadoopFileSystemAPI( args );
        
        logger.info( "=============   END  ====================" );
    }

    public static void hadoopURL( String[] args )
        throws Exception
    {
        InputStream in = null;
        try
        {
            in = new URL( args[0] ).openStream();
            IOUtils.copyBytes( in, System.out, 4096, false );
        }
        finally
        {
            IOUtils.closeStream( in );
        }
    }

    public static void hadoopFileSystemAPI( String[] args )
        throws Exception
    {
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get( URI.create( uri ), conf );
        FSDataInputStream in = null;

        try
        {
            in = fs.open( new Path( uri ) );
            IOUtils.copyBytes( in, System.out, 4096, false );

            in.seek( 0 );

            IOUtils.copyBytes( in, System.out, 4096, false );
        }
        finally
        {
            IOUtils.closeStream( in );
        }
    }
}
