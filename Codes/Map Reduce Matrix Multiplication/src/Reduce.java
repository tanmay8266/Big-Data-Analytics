
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

public class Reduce extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// process values
		
		// define value as String type array
		String[] value;
		//create 2 hashmaps (hashA,hasB) of Integer,Float to store values from A and B matrix
		HashMap<Integer, Float> hashA = new HashMap<Integer, Float>();
        HashMap<Integer, Float> hashB = new HashMap<Integer, Float>();
        //for each value in values
        for (Text val : values) {
	    // convert each value to string as it is Text and split it by comma. Store this in value i.e. string type array defined above.
        	value = val.toString().split(",");
	    //if value[0] is A then
        	if(value[0].equals("A")) {
        		hashA.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
        	}
		//put value[1] i.e.j and value[2] i.e. mij in hashA
        	else
        	{	
        		hashB.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
        	}
        }
		//else put value[1] i.e.j and value[2] i.e. njk in hashB
        
        // take value of n which is common in both the matrices here from context object
        int n = Integer.parseInt(context.getConfiguration().get("n"));
        // define result of type float and assign value 0 to it.
        float result = 0.0f;
        float m_ij;// define m_ij of type float
        float n_jk;// define n_jk of type float
	
        //define a loop variable j and iterate till it is less than n
        for (int j = 0; j < n; j++) 
        {
        	// check if value exists for a key j in hashA. if yes assign it to m_ij else assign 0 to m_ij.
        	m_ij = hashA.containsKey(j) ? hashA.get(j) : 0.0f;
        	// check if value exists for a key j in hashB. if yes assign it to n_jk else assign 0 to n_jk.
            n_jk = hashB.containsKey(j) ? hashB.get(j) : 0.0f;
         // multiply m_ij with n_jk and add this product to result which is of type float and declared above.
            result += m_ij * n_jk;
 
        }
        // if value of result is not 0 then
        if (result != 0.0f) 
        {
        	// write key,value to context. key is null and value is (i,k,result)
        	context.write(null, new Text(key.toString() + "," + Float.toString(result)));
        }
        }
	}