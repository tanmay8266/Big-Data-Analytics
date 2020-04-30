import java.io.IOException;
import java.util.*;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
 

public class Map extends Mapper<LongWritable, Text, Text, Text> 
{

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException 
	{
		//Create a configuration object using context
		Configuration conf = context.getConfiguration();
		
		// Consider a matrix of size m*n and other matrix is of size n*p. Get the values of m and p using get() method of configuration object	
		int m = Integer.parseInt(conf.get("m"));
        int p = Integer.parseInt(conf.get("p"));
        
        //convert Text value to string 
        String line = value.toString();
        
        // Split each line into tokens separated by comma.
        String[] indicesAndValue = line.split(",");
        
        // Define outputkey of type Text()
        Text outputKey = new Text();
        
        //Define outputvalue of type Text()
        Text outputValue = new Text();

        //If first token is A
        if (indicesAndValue[0].equals("A"))
        {
        
        	// Vary k from 0 to no. of columns-1 of 2nd matrix
        	for (int k=0; k < p; k++) {
        		//set outputkey as tokens[1],k i.e.i,k
        		outputKey.set(indicesAndValue[1] + "," + k);
        		// set outputkeyvalue as A,tokens[2],tokens[3] i.e. A,j,mij
                outputValue.set(indicesAndValue[0] + "," + indicesAndValue[2] + "," + indicesAndValue[3]);
                //outputValue
                context.write(outputKey, outputValue);
                
        }
	}

        	// Vary i from 0 to no.of rows-1 of 1st matrix
            for (int i=0; i < m; i++)
            {
            	//set outputkey as i,tokens[2] i.e.i,k
            	 outputKey.set(i + "," + indicesAndValue[2]);
             
            	 //set outputkey value B,toknes[1],tokens[3] i.e. B,j,nkj
            	 outputValue.set("N," + indicesAndValue[1] + ","
                         + indicesAndValue[3]);
            	 // write key and value to context
            	 context.write(outputKey, outputValue);
            }
      

	}

}