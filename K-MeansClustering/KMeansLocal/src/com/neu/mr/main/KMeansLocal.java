package com.neu.mr.main;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Local KMeans
 * @author amansharma
 *
 */
public class KMeansLocal {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
				
		DistributedCache.createSymlink(conf);
		DistributedCache.addCacheFile(new URI(args[2] + "#data_file"), conf);
		
		Job job = new Job(conf);
		
		job.setJarByClass(KMeansLocal.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class); 
		
		job.setMapperClass(KMeansLocalMapper.class);
		job.setNumReduceTasks(0); 
		
		// set the number of lines per split as 1 so that each
		// configurations go to a single mapper
		NLineInputFormat.setNumLinesPerSplit(job, 1);
		job.setInputFormatClass(NLineInputFormat.class); 		
		job.waitForCompletion(true);
		
	}

}
