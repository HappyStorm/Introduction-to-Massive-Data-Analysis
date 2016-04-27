package cwwu.hw2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class phase2_reducer extends Reducer<Text, Text, Text, Text> {

	private static final double BETA = 0.80;
	private static long numNode, numDeadEnd;
	private static double deadEndContribution, allContribution;
	
	public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String param1 = conf.get("NumNode"), param2 = conf.get("NumDeadEnd"),
			   param3 = conf.get("DeadEndContribution"), param4 = conf.get("AllContribution");
		numNode = Long.parseLong(param1);
		numDeadEnd = Long.parseLong(param2);
		deadEndContribution = Double.parseDouble(param3);
		allContribution = Double.parseDouble(param4);
		
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
		
		// sample output string:
		// 				Source Page		Source PageRank		OutLinkSet
		//				0				1.0					5,3,2,1,4,10,9,8,7,6
		//				[0]				[1]					[2]
		double prValueNew;
		prValueNew = BETA * sumFromOtherLinks +
					((1-BETA) * (allContribution / (double) numNode)) +
					(BETA * deadEndContribution / (double) numNode);
//		prValueNew = BETA * sumFromOtherLinks + (1-BETA);
		context.write(_key, new Text(prValueNew + "\t" + outLinks));
	}
}
