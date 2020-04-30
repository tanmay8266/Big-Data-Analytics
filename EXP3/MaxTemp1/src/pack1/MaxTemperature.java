package pack1;

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
		job.setMapperClass(MaxTemperatureMapper.class);
		
		// TODO: specify a reducer
		job.setReducerClass(MaxTemperatureReducer.class);
		//job.setNumReduceTasks(3);
		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("/home/universe/Downloads/hadoop/hadoop_required_artifacts_neebal/Temperature_data"));
		FileOutputFormat.setOutputPath(job, new Path("/home/universe/Downloads/hadoop/hadoop_required_artifacts_neebal/BaseHadoopProject/out4"));

		
		System.out.println(job.waitForCompletion(true));
		
	}

}
