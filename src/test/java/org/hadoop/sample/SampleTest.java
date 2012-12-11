package org.hadoop.sample;

import org.apache.hadoop.util.ToolRunner;
import org.hadoop.util.Utils;
import org.junit.Assert;
import org.junit.Test;

public class SampleTest
{

    @Test
    public void putFile()
    {
        try
        {
            String[] args = { "/root/workspace/hadoop_basic/input2/putTest.txt", "output2/", "PutFile.txt" };

            Utils.removeDIR( "/root/workspace/hadoop_basic/output2" );

            int res = ToolRunner.run( new PutFile(), args );

            System.exit( res );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void appendFile()
    {
        try
        {
            String[] args = { "hdfs://192.168.140.128:9000/user/root/input//test.txt", "output2/", "PutFile.txt" };

            Utils.removeDIR( "/root/workspace/hadoop_basic/output2" );

            int res = ToolRunner.run( new AppendFile(), args );

            System.exit( res );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            Assert.fail();
        }
    }
}
