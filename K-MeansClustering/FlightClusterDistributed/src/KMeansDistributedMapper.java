import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import au.com.bytecode.opencsv.CSVReader;
import edu.neu.mr.project.data.KMeansInfo;


/**
 * Calculates distance of a particular record from the centroids and send record
 * to a particular Reducer 
 * @author amansharma
 */
public class KMeansDistributedMapper extends Mapper<Object, Text, IntWritable, Text> {

	private String[] records;
	private double minDistance;
	private double[] centroids;
	private HashMap<Integer, KMeansInfo> sumFreqMap = new HashMap<Integer, KMeansInfo>();
	
	@Override 
	protected void setup(
			Mapper<Object, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		String centroidsArr = context.getConfiguration().get("centroids");
		populateCentroidArray(centroidsArr.split(","));
	}
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
    	CSVReader csvReader = new CSVReader(new StringReader(value.toString()));
    	records = csvReader.readNext(); 
    	csvReader.close(); 
 
    	int clusterId = calculateDistance(Double.parseDouble(records[2])); 
    	
    	if(!sumFreqMap.containsKey(clusterId)) { 
    		sumFreqMap.put(clusterId, new KMeansInfo(Double.parseDouble(records[2]), 1, centroids[clusterId])); 
    	} else {
    		KMeansInfo info = sumFreqMap.get(clusterId);
    		info.setFrequency(info.getFrequency() + 1);
    		info.setSum(info.getSum() + Double.parseDouble(records[2]));
    	}
	}
	
	@Override
	protected void cleanup(Mapper<Object, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
		for(Entry<Integer, KMeansInfo> entry : sumFreqMap.entrySet()) { 
			context.write(new IntWritable(entry.getKey()), 
					new Text(entry.getValue().getSum() + 
							"," + entry.getValue().getFrequency() + 
							"," + entry.getValue().getCentroid())); 
		}
		sumFreqMap.clear();
	}
	
	private void populateCentroidArray(String[] centroid){
		int length = centroid.length;
		centroids = new double[length];
		for(int i=0 ; i < length ; i++) {
			centroids[i] = Double.parseDouble(centroid[i]);
		}
 	}
	
	private int calculateDistance(double d) {
		int l = centroids.length;
		minDistance = Integer.MAX_VALUE;
		int index = -1;
		
		for(int i=0 ; i<l ; i++) {
			double distance = Math.abs(d - centroids[i]);
			if(minDistance > distance) {
				minDistance = distance;
				index = i;
			}
		}
		
		return index;
	}
}