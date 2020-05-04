import java.io.IOException;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;


public class HighestReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {
		// process values
		int max_temp = 0;
		for(IntWritable val:values) {
			max_temp = Math.max(max_temp,val.get());
		}
		
		context.write(key, new IntWritable(max_temp));
	}

}
