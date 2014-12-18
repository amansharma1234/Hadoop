package com.neu.mr.main;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * KMeans Local Mapper which runs for each configuration.
 * @author amansharma
 */
public class KMeansLocalMapper extends Mapper<Object, Text, IntWritable, Text> {

	private double minDistance;
	
	private double[] centroids;
	private double[] oldCentroids;
	
	private double[] distanceSum;
	private double[] frequecies;
	 
	private String centroidStr = "", numberOfRecords="";
	private Path[] localFiles;
	
	private static NumberFormat formatter = new DecimalFormat("#0.00");
	
	private double DELTA = 2;
	private int length;
	
	@Override
	protected void setup(Mapper<Object, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		
		/* get path of distributed cache */
		localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());  
	}
	
	@Override
	protected void map(Object key, Text value,
			Mapper<Object, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		String vals[] = value.toString().split(" ");
		
		centroidStr = vals[1];
		System.out.println(centroidStr);
		
		context.write(null, new Text(centroidStr)); 
		
		boolean runLoop = true;
		populateCentroidArray(centroidStr.split(","));
		
		// run loop till convergence
		while(runLoop) {
			for(int i=0 ; i < length ; i++) { 
				oldCentroids[i] = centroids[i];
			}
			centroidStr = ""; 
			numberOfRecords = "";
			BufferedReader br  = new BufferedReader(new FileReader(localFiles[0].toString()));
			try { 
				String line = br.readLine();
				while (line != null) {
					processEachRecord(line);
					line = br.readLine();
				}
			} finally {
				br.close();
			}
			
			for(int i=0 ; i < length ; i++) {
				centroids[i] = Double.parseDouble(formatter
						.format(distanceSum[i] / frequecies[i]));
				centroidStr += centroids[i] + ",";
				numberOfRecords += frequecies[i] + ",";
				distanceSum[i] = 0;
				frequecies[i] = 0;
			}
			if(checkIfAllConverged()) {
				runLoop = false;
			}
			System.out.println(centroidStr);
			System.out.println(numberOfRecords);
			context.write(null, new Text(centroidStr.substring(0, centroidStr.length() - 1))); 
		} 
	
	} 
	
	/* checks if all the centroids have converged */
	private boolean checkIfAllConverged(){
		for(int i=0 ; i < centroids.length ; i++) {
			if(Math.abs(centroids[i] - oldCentroids[i]) > DELTA) {
				return false;
			}
		}
		return true;
	}
	
	/* process each record read from file */
	private void processEachRecord(String line) {
		String[] s = line.split(",");
		double layoverTime = Double.parseDouble(s[2]);
		int index = calculateDistance(layoverTime); 
		distanceSum[index] += layoverTime;
		frequecies[index]++; 

	}
	
	/* populare centroid array */
	private void populateCentroidArray(String[] centroid){
		length = centroid.length;
		oldCentroids = new double[length];
		centroids = new double[length];
		distanceSum = new double[length]; 
		frequecies = new double[length]; 
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