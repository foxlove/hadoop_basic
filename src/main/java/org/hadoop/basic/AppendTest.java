package org.hadoop.basic;

import java.io.BufferedOutputStream;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class AppendTest
{
    static byte[] data = new byte[1024];

    public static void main( String[] args )
        throws Exception
    {
        if ( args.length < 3 )
        {
            System.out.println( "Usage java AppendTest <file path> <# datas> <1:write,2:append>" );
            System.exit( 0 );
        }

        Random rand = new Random( System.currentTimeMillis() );
        rand.nextBytes( data );

        long startTime = System.currentTimeMillis();
        if ( "1".equals( args[2] ) )
        {
            write( args[0], Integer.parseInt( args[1] ) );
        }
        else
        {
            append( args[0], Integer.parseInt( args[1] ) );
        }
        System.out.println( "Lap time: " + ( System.currentTimeMillis() - startTime ) + " ms, data: "
            + Integer.parseInt( args[1] ) );
    }

    public static void write( String path, int numDatas )
        throws Exception
    {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get( conf );

        BufferedOutputStream out = new BufferedOutputStream( fs.create( new Path( path ) ) );

        long time1 = System.currentTimeMillis();
        for ( int i = 0; i < numDatas; i++ )
        {
            out.write( data );
            if ( i % 1000 == 0 )
            {
                System.out.println( "1 MB written, " + ( System.currentTimeMillis() - time1 ) + " ms" );
                time1 = System.currentTimeMillis();
            }
        }
        out.close();
    }

    public static void append( String path, int numDatas )
        throws Exception
    {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get( conf );

        FSDataOutputStream out = fs.append( new Path( path ) );

        long time1 = System.currentTimeMillis();
        for ( int i = 0; i < numDatas; i++ )
        {
            out.write( data );
            //out.hflush();
            if ( i % 1000 == 0 )
            {
                out.flush();
                System.out.println( "1 MB written, " + ( System.currentTimeMillis() - time1 ) + " ms" );
                time1 = System.currentTimeMillis();
            }
        }
        out.close();
    }
}
