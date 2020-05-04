
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import java.util.*;

public class MaxTemperature {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "maxtemp");
		job.setJarByClass(MaxTemperature.class);
		// TODO: specify a mapper
		job.setMapperClass(HighestMapper.class);
		//POSTLAB COMBINER
		//job.setCombinerClass(HighestReducer.class);
		// TODO: specify a reducer
		job.setReducerClass(HighestReducer.class);
		

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("/media/tanmay/Data/SEM-8/BDA/EXP3/Temperature_data"));
		FileOutputFormat.setOutputPath(job, new Path("/home/tanmay/postlabout"));

		if (!job.waitForCompletion(true)) {
			System.out.println("Job Completed");
			return;
		}
	}

}
