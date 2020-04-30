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

public class kdriver {
	public static void main(String[] args) throws Exception {
		int rec = 0; 
		Configuration conf = new Configuration();
		while(true) {
		if(rec==0) {
		int k = 3; //Set K here for K clusters
		int count = 0;
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
		job.setJarByClass(kdriver.class);
		// TODO: specify a mapper
		job.setMapperClass(kmapper.class);
		// TODO: specify a reducer
		job.setReducerClass(kreducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
        
		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("/media/tanmay/Data/SEM-8/BDA/EXP7/input"));
		FileOutputFormat.setOutputPath(job, new Path("/media/tanmay/Data/SEM-8/BDA/EXP7/outfinal"+String.valueOf(rec)));
		if (!job.waitForCompletion(true))
			System.out.println("True"+String.valueOf(rec));
			rec++;
		}
		else {
			
			try {
				  int count = 0;
				  boolean flag = true;
				  int k = conf.getInt("k", 3);
			      File myObj = new File("/media/tanmay/Data/SEM-8/BDA/EXP7/outfinal"+String.valueOf(rec-1)+"/part-r-00000");
			      Scanner myReader = new Scanner(myObj);
			      while (myReader.hasNextLine()) {
			        String data = myReader.nextLine();
			        String[] inter = data.split(";");
			        data = inter[0];
			        String prev = conf.get(String.valueOf(count));
			        conf.set(String.valueOf(count),data.substring(2));
//			        System.out.println("current"+data.substring(2));
//			        System.out.println("prev"+ prev);
			        String[] temp = data.substring(2).split(",");
			        float data_x = Float.parseFloat(temp[0]);
			        float data_y = Float.parseFloat(temp[1]);
			        String[] temp1 = prev.split(",");
			        float prev_x = Float.parseFloat(temp1[0]);
			        float prev_y = Float.parseFloat(temp1[1]);
			        String result = String.format("%f %f    %f %f",data_x,prev_x,data_y,prev_y);
			        System.out.println(result);
			        if(Math.abs(data_x - prev_x)>1.5 || Math.abs(data_y - prev_y)>1.5) {
			        	flag = false;
			        }
			        count++;
			      }
			      if(rec==10) {
			      System.exit(0);
			      }
			      if(flag == true) {
			    	  myReader.close();
			    	  System.out.println("Done");
			    	  break;
			      }
			      else
			      {
			    	  Job job = Job.getInstance(conf, "JobName");
			  		job.setJarByClass(kdriver.class);
			  		// TODO: specify a mapper
			  		job.setMapperClass(kmapper.class);
			  		// TODO: specify a reducer
			  		job.setReducerClass(kreducer.class);

			  		// TODO: specify output types
			  		job.setOutputKeyClass(IntWritable.class);
			  		job.setOutputValueClass(Text.class);
			  		
			          
			  		// TODO: specify input and output DIRECTORIES (not files)
			  		FileInputFormat.setInputPaths(job, new Path("/media/tanmay/Data/SEM-8/BDA/EXP7/input"));
			  		FileOutputFormat.setOutputPath(job, new Path("/media/tanmay/Data/SEM-8/BDA/EXP7/outfinal"+String.valueOf(rec)));
			  		if (!job.waitForCompletion(true))
			  			System.out.println("True"+String.valueOf(rec));
			  			rec++;
			  		
			      }
			      
			    } catch (Exception e) {
			      System.out.println("An error occurred.");
			      e.printStackTrace();
			    }
			
		}
	}
			
	}

}
