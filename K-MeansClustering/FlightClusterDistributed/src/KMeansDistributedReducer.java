import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * Joins data on the basis of the correct arrival time and departure
 * time and calculates new centroid and writes to HDFS
 * 
 * @author amansharma
 */
public class KMeansDistributedReducer extends
		Reducer<IntWritable, Text, DoubleWritable, Text> {

	private double frequency = 0;
	private double sum = 0;
	private double newCentroid = 0, oldCentroid = 0;
	private static double DELTA = 2;
	private NumberFormat formatter = new DecimalFormat("#0.00");
	
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String oldCentroidStr = "";
		for(Text value : values) { 
			String[] vals = value.toString().split(",");
			sum += Double.parseDouble(vals[0]);
			frequency += Integer.parseInt(vals[1]); 
			oldCentroidStr = vals[2];
		}
		
		oldCentroid = Double.parseDouble(oldCentroidStr);
		newCentroid = Double.parseDouble(formatter
				.format(sum/frequency)); 
		String res = String.valueOf(newCentroid);

		FileSystem hdfs = FileSystem.get(context.getConfiguration()); 
		
		if(Math.abs(newCentroid - oldCentroid) <= DELTA) { 
			context.getCounter(GlobalCounter.CONVERGE).increment(1);
		} 
		Utils.writeDataToHDFS(hdfs, res + "-" + frequency , key.get() + "");
		sum=0;
		frequency=0;
		

	}
}