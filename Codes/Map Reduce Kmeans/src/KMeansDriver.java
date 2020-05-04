import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class KMeansDriver {

	public static void main(String[] args) throws Exception {
		int k = 3; //Set K here for K clusters
		int count = 0;
		Configuration conf = new Configuration();
		try {
		      File myObj = new File("/media/tanmay/Data/SEM-8/BDA/EXP7/input/kmeaninput");
		      Scanner myReader = new Scanner(myObj);
		      while (myReader.hasNextLine() && count < k) {
		        String data = myReader.nextLine();
		        conf.set(String.valueOf(count),data);
		        System.out.println(conf);
		        count = count + 1;
		      }
		      myReader.close();
		    } catch (FileNotFoundException e) {
		      System.out.println("An error occurred.");
		      e.printStackTrace();
		    }
		conf.setInt("k", k);
		Job job = Job.getInstance(conf, "JobName");
		job.setJarByClass(KMeansDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(KMeansMapper.class);
		// TODO: specify a reducer
		job.setReducerClass(KMeansReducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
        
		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("/media/tanmay/Data/SEM-8/BDA/EXP7/input"));
		FileOutputFormat.setOutputPath(job, new Path("/media/tanmay/Data/SEM-8/BDA/EXP7/outfinal"));

		if (!job.waitForCompletion(true))
			return;
	}

}
