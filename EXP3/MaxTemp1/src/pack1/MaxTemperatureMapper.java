package pack1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;

public class MaxTemperatureMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	//declare a public static final integer variable for assigning missing value and assign value 9999 as a missing value 
	

	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		//Convert value given to map function to string
		
		//extract year(4 digits)which is at 16th position of each record in given data set 
		
		//define integer variable for measuring air temperature
		
		//check the sign of temperature at 88th position of record. If it is +, then extract only integer value of temperature(4 digits).
		//If sign is -, then extract integer value of temperature(4 digit) along with - sign.
		
		//extract quality parameter at 93rd position of record.
		
		//check if temperature is not missing and quality code belongs to any one of 0,1,4,5,9.If yes then write year and temperature to context.
		
		
	}
	}

