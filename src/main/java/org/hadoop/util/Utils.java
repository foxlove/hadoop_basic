package org.hadoop.util;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils
{
    private final static Logger logger = LoggerFactory.getLogger( Utils.class );

    public static void removeDIR( String source )
    {
        File dir = new File( source );
        
        if(!dir.exists()) return;
        
        File[] listFile = dir.listFiles();
        try
        {
            if ( listFile.length > 0 )
            {
                for ( int i = 0; i < listFile.length; i++ )
                {
                    if ( listFile[i].isFile() )
                    {
                        listFile[i].delete();
                    }
                    else
                    {
                        removeDIR( listFile[i].getPath() );
                    }
                    listFile[i].delete();
                }
            }

            if ( dir.delete() )
            {
                logger.debug( "dir was deleted" );
            }
        }
        catch ( Exception e )
        {
            System.err.println( System.err );
            System.exit( -1 );
        }
    }
}
