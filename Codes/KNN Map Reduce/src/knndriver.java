import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class knndriver {

	public static void main(String[] args) throws Exception
	{
		// Create configuration
		Configuration conf = new Configuration();
		
//		if (args.length != 3)
//		{
//			System.err.println("Usage: KnnPattern <in> <out> <parameter file>");
//			System.exit(2);
//		}

		// Create job
		Job job = Job.getInstance(conf, "Find K-Nearest Neighbour");
		job.setJarByClass(knndriver.class);
		// Set the third parameter when running the job to be the parameter file and give it an alias
		job.addCacheFile(new URI(args[0]+"#knnParamFile")); // Parameter file containing test data
//		GenericOptionsParser parser = new GenericOptionsParser(conf, args);
//		args = parser.getRemainingArgs();
		// Setup MapReduce job
		job.setMapperClass(KnnMapper.class);
		job.setReducerClass(KnnReducer.class);
		job.setNumReduceTasks(1); // Only one reducer in this design

		// Specify key / value
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(DoubleString.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
				
		// Input (the data file) and Output (the resulting classification)
		FileInputFormat.setInputPaths(job, new Path("/media/tanmay/Data/SEM-8/BDA/EXP8/Exp8bda1920/CarOwners.csv"));
		FileOutputFormat.setOutputPath(job, new Path("/media/tanmay/Data/SEM-8/BDA/EXP8/Exp8bda1920/out2"));
		
		// Execute job and return status
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
