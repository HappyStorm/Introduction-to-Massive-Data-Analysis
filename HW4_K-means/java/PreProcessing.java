package cwwu.haley;

import java.io.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

public class PreProcessing {
	protected static int MAX_CLUSTERS;
	
	// "/hw4/out/iter00/" + "data.txt"("c1.txt", "c2.txt")
	public PreProcessing(String inPath_data, String inPath_c1, String inPath_c2,
						 String outPath_data, String outPath_c1, String outPath_c2, int num_cluster){
		try{
			FileSystem fs = FileSystem.get(new Configuration());
			PreProcessing.MAX_CLUSTERS = num_cluster;
			int clusterID;
			
			// format c1.txt (cluster_c1)
			Path in_c1 = new Path(inPath_c1);
			Path out_c1 = new Path(outPath_c1);
			BufferedReader br_c1 = new BufferedReader(new InputStreamReader(fs.open(in_c1)));
			BufferedWriter bw_c1 = new BufferedWriter(new OutputStreamWriter(fs.create(out_c1, true)));
			clusterID = 0;
			while(br_c1.ready())
				bw_c1.write(String.valueOf(clusterID++) + "\t" + br_c1.readLine() + "\n"); 
			br_c1.close();
			bw_c1.close();
			
			// format c2.txt (cluster_c2)
			Path in_c2 = new Path(inPath_c2);
			Path out_c2 = new Path(outPath_c2);
			BufferedReader br_c2 = new BufferedReader(new InputStreamReader(fs.open(in_c2)));
			BufferedWriter bw_c2 = new BufferedWriter(new OutputStreamWriter(fs.create(out_c2, true)));
			clusterID = 0;
			while(br_c2.ready())
				bw_c2.write(String.valueOf(clusterID++) + "\t" + br_c2.readLine() + "\n");
			br_c2.close();
			bw_c2.close();
	      
			// format data.txt (point data for c1)
			Path in_data_c1 = new Path(inPath_data);
			Path out_data_c1 = new Path(outPath_data + "c1_00.txt");
			BufferedReader br_data_c1 = new BufferedReader(new InputStreamReader(fs.open(in_data_c1)));
			BufferedWriter bw_data_c1 = new BufferedWriter(new OutputStreamWriter(fs.create(out_data_c1, true)));
			while(br_data_c1.ready()) // -1	d1 d2 d3 d4 ... d_last
				bw_data_c1.write("-1\t" + br_data_c1.readLine() + "\n");
			br_data_c1.close();
			bw_data_c1.close();
			
			// format data.txt (point data for c1)
			Path in_data_c2 = new Path(inPath_data);
			Path out_data_c2 = new Path(outPath_data + "c2_00.txt");
			BufferedReader br_data_c2 = new BufferedReader(new InputStreamReader(fs.open(in_data_c2)));
			BufferedWriter bw_data_c2 = new BufferedWriter(new OutputStreamWriter(fs.create(out_data_c2, true)));
			while(br_data_c2.ready()) // -1	d1 d2 d3 d4 ... d_last
				bw_data_c2.write("-1\t" + br_data_c2.readLine() + "\n");
			br_data_c2.close();
			bw_data_c2.close();
			
			fs.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}
