
import java.io.IOException;
import java.util.*;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class MatDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
        // A is an m-by-n matrix; B is an n-by-p matrix.
		long startTime = System.nanoTime();
        conf.set("m", "100");
        conf.set("n", "100");
        conf.set("p", "100");
        @SuppressWarnings("deprecation")
        	Job job = new Job(conf, "MatrixMatrixMultiplicationOneStep");
        job.setJarByClass(MatDriver.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
 
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
 
        FileInputFormat.addInputPath(job, new Path("/media/tanmay/Data/SEM-8/BDA/EXP4/post_input_1"));
        FileOutputFormat.setOutputPath(job, new Path("/media/tanmay/Data/SEM-8/BDA/EXP4/postout"));
        if (job.waitForCompletion(true)) {
        	long endTime = System.nanoTime();
        	long timeElapsed = endTime - startTime;
    		System.out.println("Execution time in milliseconds  : " + timeElapsed/1000000);
			System.out.println("Job Completed");
			return;
		}
        
        
}
	
}
