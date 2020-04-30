import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		// process values
		//initialize sum=0
		int sum = 0;
		
		//Iterate over the collection of values to get count of each word i.e. key 
		 for(IntWritable val:values)
		 {
			 sum+=val.get();
		 }
		 //Write this count to the context. 
		context.write(key,new IntWritable(sum));
		 
	}
}

