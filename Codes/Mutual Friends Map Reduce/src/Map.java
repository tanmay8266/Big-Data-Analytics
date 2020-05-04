import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Map extends Mapper<LongWritable, Text, Text, Text> {

	private Text word = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split(", ");
		if (line.length == 2) {
			String friend1 = line[0];
			List<String> values = Arrays.asList(line[1].split(" "));
			for (String friend2 : values) {
				int f1 = Integer.parseInt(friend1);
				int f2 = Integer.parseInt(friend2);
				if (f1 < f2)
					word.set(friend1 + "," + friend2);
				else
					word.set(friend2 + "," + friend1);
				context.write(word, new Text(line[1]));
			}
		}
	}

}