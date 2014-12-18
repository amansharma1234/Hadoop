import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class Utils {
	
	public static final String PATH = "/flightClusters";
	
	public static void writeDataToHDFS(FileSystem hdfs, String data, String pathId) throws IOException {
		Path path = new Path(hdfs.getHomeDirectory() + PATH + "/" + pathId + ".txt");  
		FSDataOutputStream fsOutStream = null;
		fsOutStream = hdfs.create(path);
		fsOutStream.write(data.getBytes()); 
		fsOutStream.close(); 
	}

}
