import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	// Create object of type Text to hold strings created per word of given document
	
	// Create final static variable of type IntWritable with value equal to 1 as according to algorithm map function puts 1 for each word encountered.
	
	//write a map function here
	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		//Define a String type variable and assign value which is equal to string equivalent of Text value passed to map function
		
		//Convert this variable to tokens using StringTokenizer class as it separates out each word of the document
		
		//Until there are tokens in StringTokenizer, 
		
			// set the Text object created in first line of the code in WCMapper to next token in the StringTokenizer
		 
		 // Write this Text Variable and final static IntWritable Variable created to the context so that key-value pairs are generated.
		 
		}
	}

