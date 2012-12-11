package org.hadoop.sample;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppendFile
    extends Configured
    implements Tool
{

    private static final Logger logger = LoggerFactory.getLogger( PutFile.class );

    private static String[] args;

    public static class DeleteFileMapper
        extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, Text>
    {

        @Override
        public void map( LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter )
            throws IOException
        {
            logger.info( " value : >>> " + value.toString() );

            String val = "";

            Configuration config = new Configuration();
            FileSystem fs = FileSystem.get( config );

            System.out.println( ">>>> " + args[0] );

            // java.io.IOException: Not supported  ?????????????
            //=> http://lucene.472066.n3.nabble.com/Appending-to-existing-files-in-HDFS-td1517827.html
            FSDataOutputStream out = fs.append( new Path( args[0] ) );

            out.write( value.toString().getBytes() );
            out.flush();
            out.close();
        }
    }

    public static class DeleteFileReducer
        extends MapReduceBase
        implements Reducer<Text, Text, Text, Text>
    {

        @Override
        public void reduce( Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter )
            throws IOException
        {

        }
    }

    @Override
    public int run( String[] arg )
        throws Exception
    {
        this.args = arg;

        // Job create
        JobConf conf = new JobConf( getConf(), PutFile.class );
        conf.setJobName( "DeleteFile" );

        conf.setOutputKeyClass( Text.class );
        conf.setOutputValueClass( Text.class );

        conf.setMapperClass( DeleteFileMapper.class );
        conf.setCombinerClass( DeleteFileReducer.class );
        conf.setReducerClass( DeleteFileReducer.class );

        FileInputFormat.setInputPaths( conf, arg[0] );
        FileOutputFormat.setOutputPath( conf, new Path( arg[1] ) );

        JobClient.runJob( conf );

        logger.info( "=== The END ===" );

        return 0;
    }

}
