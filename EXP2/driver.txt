import java.io.IOException;
import java.util.Date;
import java.util.Formatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.*;

public class WCDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		 GenericOptionsParser parser = new GenericOptionsParser(conf, args);
		 args = parser.getRemainingArgs();
		 
		//Job job = new Job(conf, "wordcount");
		 Job job=new Job(conf,"wordcount");
		 job.setJarByClass(WCDriver.class);
		 
		//job.setOutputKeyClass(Text.class);
		// job.setOutputValueClass(IntWritable.class);
		 
		 job.setMapOutputKeyClass(Text.class);
		 job.setMapOutputValueClass(IntWritable.class);
		 
		job.setInputFormatClass(TextInputFormat.class);
		 job.setOutputFormatClass(TextOutputFormat.class);
		 
		Formatter formatter = new Formatter();
		 String outpath = "Out"
		 + formatter.format("%1$tm%1$td%1$tH%1$tM%1$tS", new Date());
		 FileInputFormat.setInputPaths(job, new Path("/media/tanmay/Data/SEM-8/BDA/EXP2/testfiles"));
		 FileOutputFormat.setOutputPath(job, new Path("/media/tanmay/Data/SEM-8/BDA/EXP2/output"));
		 job.setMapperClass(WCMapper.class);
		 job.setReducerClass(WCReducer.class);
		 
		System.out.println(job.waitForCompletion(true));
	}

}