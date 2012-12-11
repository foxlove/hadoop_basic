package org.hadoop.basic;

import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * if(args[1] == "hdfs://192.168.140.128:9000/user/root/output/wordcount") result location -> HDFS
 * director => org.apache.hadoop.mapred.FileAlreadyExistsException: $HADOOP_HOME]hadoop dfs -rmr
 * (arg1) ex) /usr/local/hadoop-1.0.4/bin/hadoop dfs -rmr output/wordcount if(args[1] ==
 * "output/wordcount") : result location -> eclipse workspace =>
 * /root/workspace/hadoop_basic/output/wordcount/...
 * 
 * @author root
 */

public class WordCount
{
    public static void main( String[] args )
        throws Exception
    {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser( conf, args ).getRemainingArgs();
        
        if ( otherArgs.length != 2 )
        {
            System.err.println( "Usage: wordcount <in> <out>" );
            System.exit( 2 );
        }
        
        Path path0 = new Path( args[0] );
        Path path1 = new Path( args[1] );

        // if Directory exist in HDFS then delete Directory....        
        FileSystem fs = FileSystem.get( URI.create( args[1] ), new Configuration() );
        if ( fs.isDirectory( path1 ))
        {
            fs.delete( path1, true );
        }

        Job job = new Job( conf, "word count" );

        job.setJarByClass( WordCount.class );

        job.setMapperClass( TokenizerMapper.class );
        job.setCombinerClass( IntSumReducer.class );
        job.setReducerClass( IntSumReducer.class );

        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( IntWritable.class );

        FileInputFormat.addInputPath( job, path0 );
        FileOutputFormat.setOutputPath( job, path1 );

        System.exit( ( job.waitForCompletion( true ) ) ? 0 : 1 );
    }

    public static class TokenizerMapper
        extends Mapper<Object, Text, Text, IntWritable>
    {
        private static final IntWritable one = new IntWritable( 1 );

        private Text word;

        public TokenizerMapper()
        {
            this.word = new Text();
        }

        public void map( Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context )
            throws IOException, InterruptedException
        {
            StringTokenizer itr = new StringTokenizer( value.toString() );
            while ( itr.hasMoreTokens() )
            {
                this.word.set( itr.nextToken() );
                context.write( this.word, one );
                System.out.println("this.word : " + this.word);
            }
        }
    }

    public static class IntSumReducer
        extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        private IntWritable result;

        public IntSumReducer()
        {
            this.result = new IntWritable();
        }

        public void reduce( Text key, Iterable<IntWritable> values,
                            Reducer<Text, IntWritable, Text, IntWritable>.Context context )
            throws IOException, InterruptedException
        {
            
            System.out.println("reduce key : " + key.toString());
            System.out.println("reduce values : " + values.toString());
            
            int sum = 0;
            for ( IntWritable val : values )
            {
                sum += val.get();
            }
            this.result.set( sum );
            context.write( key, this.result );
        }
    }
}