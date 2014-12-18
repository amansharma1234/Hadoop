import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


/**
 * Map Reduce Driver class.
 * @author amansharma
 *
 */
public class KMeansDistributed {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		int numberOfReducers = Integer.parseInt(args[2]);
		String centroids = args[3]; 
		conf.set("centroids", centroids); 
		System.out.println(centroids);
		
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 4) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		
		int counter = 0;
		while(true) {
			Job job = new Job(conf, "flight mr");
			job.setJarByClass(KMeansDistributed.class);
			job.setMapperClass(KMeansDistributedMapper.class);

			job.setReducerClass(KMeansDistributedReducer.class); 
			job.setNumReduceTasks(numberOfReducers);
						
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1] + counter));
			counter++;
			
			boolean isComplete =  job.waitForCompletion(true);

			FileSystem hdfs = FileSystem.get(conf); 
			FileStatus[] status = hdfs.listStatus(
					new Path(hdfs.getHomeDirectory().toString() + Utils.PATH)); 
			
			String result = "";
			
			/* Read data from all files in HDFS */
			for (int i=0;i<status.length;i++) {
				BufferedReader br=new BufferedReader(new InputStreamReader(
						hdfs.open(status[i].getPath())));
				String line = br.readLine();
				result += line + ",";
				br.close();
			}				
			
			String[] res = result.split(",");
			String numberOfRecordsInCluster = "";
			centroids = "";
			for(String str : res) {
				centroids += str.split("-")[0] + ",";
				numberOfRecordsInCluster += str.split("-")[1] + ",";
			}
			centroids = centroids.substring(0,centroids.length() - 1); 
			numberOfRecordsInCluster =numberOfRecordsInCluster
					.substring(0,numberOfRecordsInCluster.length() - 1); 
			
			System.out.println(centroids); 
			System.out.println(numberOfRecordsInCluster + "\n"); 
			conf.set("centroids", centroids); 
			
			Counters c = job.getCounters();
			CounterGroup group = c.getGroup("GlobalCounter");
			Counter convergeVal = group.findCounter("CONVERGE");
			
			long converge = convergeVal.getValue();
			
			if(converge == numberOfReducers && isComplete) {
				System.out.println("Converged after " + counter + " iterations."); 
				deleteFilesInHDFS(hdfs, status);
				System.exit(0); 
			}
						
			if (!isComplete) {
				System.exit(1);
			}
		}
	}
	
	/* delete existing files from HDFS */
	private static void deleteFilesInHDFS(FileSystem hdfs, FileStatus[] status) {
		for (int i=0;i<status.length;i++) {
			Path path = status[i].getPath();
			try {
				if(hdfs.exists(path))
				{
				      hdfs.delete(path, true); //Delete existing Directory
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
