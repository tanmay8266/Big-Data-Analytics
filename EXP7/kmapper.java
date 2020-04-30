import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class kmapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	public float[][] centroids = new float[3][3];
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
		Configuration conf = context.getConfiguration();
		int k = conf.getInt("k", 2);
    	for (int  i=0; i<k; i++ ) {
    		String coords = conf.get(String.valueOf(i));
//    		System.out.println(coords);
    		String[] coords_x_y = coords.split(",");
    		
    		centroids[i][0] = Float.parseFloat(coords_x_y[0]);
    		centroids[i][1] = Float.parseFloat(coords_x_y[1]);
	  }
		String line = value.toString();
		String[] x_y = line.split(",");
		float x = Float.parseFloat(x_y[0]);
		float y = Float.parseFloat(x_y[1]);
		float distance = 0;
		int winnercentroid = -1;
		float min_distance = 999999999.9f;
		for(int i = 0; i<k;i++) {
			distance = ( x-centroids[i][0])*(x-centroids[i][0]) + 
					  (y - centroids[i][1])*(y-centroids[i][1]);
			if (distance < min_distance) {
				min_distance = distance;
				winnercentroid = i;
			}
		}
		
		IntWritable winnerCentroid = new IntWritable(winnercentroid);
		context.write(winnerCentroid, new Text(value));
//		System.out.printf("Map: Centroid = %d distance = %f\n", winnercentroid, min_distance);
		
	}

}
