package org.hadoop.MaxTemp;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.junit.Test;
import org.mockito.Mockito;

public class MaxTemperatureMapperTest
{

    @Test
    public void processValidRecord()
        throws IOException
    {
    	// test 111
    	
        MaxTemperatureMapper mapper = new MaxTemperatureMapper();

        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                              "99999V0203201N00261220001CN9999999N9-00111+99999999999");
        
        OutputCollector<Text, IntWritable> output = Mockito.mock(OutputCollector.class);

        mapper.map(null, value, output, null);

        Mockito.verify(output).collect(new Text("1950"), new IntWritable(-11));
    }
    
    @Test
    public void mainTest() throws Exception{
        String[] args = {"", ""};
        
        MaxTemperature.main(args);
    }
}
