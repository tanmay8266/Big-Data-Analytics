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
public class KnnMapper extends Mapper<Object, Text, NullWritable, DoubleString>
	{
		DoubleString distanceAndModel = new DoubleString();
		TreeMap<Double, String> KnnMap = new TreeMap<Double, String>();
		
		// Declaring some variables which will be used throughout the mapper
		int K;
		double normalisedSAge;
		double normalisedSIncome;
		String sStatus;
		String sGender;
		double normalisedSChildren;
		
		// The known ranges of the dataset, which can be hardcoded in for the purposes of this example
		double minAge = 18;
		double maxAge = 77;
		double minIncome = 5000;
		double maxIncome = 67789;
		double minChildren = 0;
		double maxChildren = 5;
			
		// Takes a string and two double values. Converts string to a double and normalises it to
		// a value in the range supplied to reurn a double between 0.0 and 1.0 
		private double normalisedDouble(String n1, double minValue, double maxValue)
		{
			return (Double.parseDouble(n1) - minValue) / (maxValue - minValue);
		}
		
		// Takes two strings and simply compares then to return a double of 0.0 (non-identical) or 1.0 (identical).
		// This provides a way of evaluating a numerical distance between two nominal values.
		private double nominalDistance(String t1, String t2)
		{
			if (t1.equals(t2))
			{
				return 0;
			}
			else
			{
				return 1;
			}
		}
		
		// Takes a double and returns its squared value.
		private double squaredDistance(double n1)
		{
			return Math.pow(n1,2);
		}
		

		// Takes ten pairs of values (three pairs of doubles and two of strings), finds the difference between the members
		// of each pair (using nominalDistance() for strings) and returns the sum of the squared differences as a double.
		private double totalSquaredDistance(double R1, double R2, String R3, String R4, double R5, double S1,
				double S2, String S3, String S4, double S5)
		{	
			double ageDifference = S1 - R1;
			double incomeDifference = S2 - R2;
			double statusDifference = nominalDistance(S3, R3);
			double genderDifference = nominalDistance(S4, R4);
			double childrenDifference = S5 - R5;
			
			// The sum of squared distances is used rather than the euclidean distance
			// because taking the square root would not change the order.
			// Status and gender are not squared because they are always 0 or 1.
			return squaredDistance(ageDifference) + squaredDistance(incomeDifference) + statusDifference + genderDifference + squaredDistance(childrenDifference);
		}

		// The @Override annotation causes the compiler to check if a method is actually being overridden
		// (a warning would be produced in case of a typo or incorrectly matched parameters)
		@Override
		// The setup() method is run once at the start of the mapper and is supplied with MapReduce's
		// context object
		protected void setup(Context context) throws IOException, InterruptedException
		{
			if (context.getCacheFiles() != null && context.getCacheFiles().length > 0)
			{
				// Read parameter file using alias established in main()
				String knnParams = FileUtils.readFileToString(new File("./knnParamFile"));
				StringTokenizer st = new StringTokenizer(knnParams, ",");
		    	
		    	// Using the variables declared earlier, values are assigned to K and to the test dataset, S.
		    	// These values will remain unchanged throughout the mapper
				K = Integer.parseInt(st.nextToken());
//				System.out.println(K);
				normalisedSAge = normalisedDouble(st.nextToken(), minAge, maxAge);
				normalisedSIncome = normalisedDouble(st.nextToken(), minIncome, maxIncome);
				sStatus = st.nextToken();
				sGender = st.nextToken();
				normalisedSChildren = normalisedDouble(st.nextToken(), minChildren, maxChildren);
			}
		}
				
		@Override
		// The map() method is run by MapReduce once for each row supplied as the input data
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			// Tokenize the input line (presented as 'value' by MapReduce) from the csv file
			// This is the training dataset, R
			String rLine = value.toString();
//			System.out.println("Hello Mapper");
			StringTokenizer st = new StringTokenizer(rLine, ",");
			
			double normalisedRAge = normalisedDouble(st.nextToken(), minAge, maxAge);
			double normalisedRIncome = normalisedDouble(st.nextToken(), minIncome, maxIncome);
			String rStatus = st.nextToken();
			String rGender = st.nextToken();
			double normalisedRChildren = normalisedDouble(st.nextToken(), minChildren, maxChildren);
			String rModel = st.nextToken();
			
			// Using these row specific values and the unchanging S dataset values, calculate a total squared
			// distance between each pair of corresponding values.
			double tDist = totalSquaredDistance(normalisedRAge, normalisedRIncome, rStatus, rGender,
					normalisedRChildren, normalisedSAge, normalisedSIncome, sStatus, sGender, normalisedSChildren);		
			
			// Add the total distance and corresponding car model for this row into the TreeMap with distance
			// as key and model as value.
			KnnMap.put(tDist, rModel);
			// Only K distances are required, so if the TreeMap contains over K entries, remove the last one
			// which will be the highest distance number.
			if (KnnMap.size() > K)
			{
				KnnMap.remove(KnnMap.lastKey());
			}
		}

		@Override
		// The cleanup() method is run once after map() has run for every row
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			// Loop through the K key:values in the TreeMap
			for(Map.Entry<Double, String> entry : KnnMap.entrySet())
			{
				  Double knnDist = entry.getKey();
				  String knnModel = entry.getValue();
				  // distanceAndModel is the instance of DoubleString declared aerlier
				  distanceAndModel.set(knnDist, knnModel);
				  // Write to context a NullWritable as key and distanceAndModel as value
				  context.write(NullWritable.get(), distanceAndModel);
			}
		}
	}