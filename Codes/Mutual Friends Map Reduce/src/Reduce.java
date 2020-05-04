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

public class Reduce extends Reducer<Text, Text, Text, Text> {
	private Text result = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		StringBuilder sb = new StringBuilder();
		for (Text friends : values) {
			List<String> temp = Arrays.asList(friends.toString().split(" "));
			for (String friend : temp) {
				if (map.containsKey(friend))
					sb.append(friend + ',');
				else
					map.put(friend, 1);

			}
		}
		if (sb.lastIndexOf(",") > -1) {
			sb.deleteCharAt(sb.lastIndexOf(","));
		}

		result.set(new Text(sb.toString()));
		context.write(key, result);
	}
}
