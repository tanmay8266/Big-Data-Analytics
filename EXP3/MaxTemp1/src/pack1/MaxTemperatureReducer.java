package pack1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;

public class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		// process values
		//declare integer variable for storing maximum value 
		
		//find maximum value of temperature in given year here
		
		//write key and value to the context
	}
		
	}