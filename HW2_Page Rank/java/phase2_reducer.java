package cwwu.hw2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class phase2_reducer extends Reducer<Text, Text, Text, Text> {

	private static final double BETA = 0.80;
	private static long numNode;
	
	public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String param = conf.get("NumNode");
		numNode = Long.parseLong(param);
		
		double sumFromOtherLinks = 0.0;
		String outLinks = "";
		int countOutLinks = 0;

		// sample input string:
		//		Linked Page 	Source Page		Source PageRank		Source OutLinks
		//		5 				0 				1.0 				10 
		//		9				0 				1.0 				10
		// process values
		for (Text val : values) {
			String line = val.toString();
			if (line.startsWith(">")){
				outLinks = line.substring(1);
				continue;
			}
			String[] token = line.split(" ");
			double prValueOld = Double.valueOf(token[1]);
            countOutLinks = Integer.valueOf(token[2]);
            sumFromOtherLinks += (prValueOld/(double)countOutLinks);
		}
		
		// sample input string:
		// 				Source Page		Source PageRank		OutLinkSet
		//				0				1.0					5,3,2,1,4,10,9,8,7,6
		//				[0]				[1]					[2]
//		double prValueNew = BETA * sumFromOtherLinks + (1-BETA);
		double prValueNew = BETA * sumFromOtherLinks + (1-BETA) / (double) numNode;
		context.write(_key, new Text(prValueNew + "\t" + outLinks));
	}
}
