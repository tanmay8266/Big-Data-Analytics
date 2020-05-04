import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

	public void reduce(IntWritable key, Iterable<Text>values, Context context) throws IOException, InterruptedException {
		// process values
		System.out.println("Hi from reducer");
		String[] value;
		String coords = "";
		int num = 0;
		float mean_x = 0.0f;
		float mean_y = 0.0f;
		for (Text val : values) {
			num++;
			value = val.toString().split(",");
			float x = Float.parseFloat(value[0]);
			float y = Float.parseFloat(value[0]);
			coords += " "+String.valueOf(x)+","+String.valueOf(y)+" ";
			mean_x += x;
		    mean_y += y;
			
		}
		mean_x = mean_x/num;
		mean_y = mean_y/num;
		
		String preres = String.format("%f %f", mean_x,mean_y)+coords;
		Text result = new Text(preres);
		context.write(key, result);
	}

}
