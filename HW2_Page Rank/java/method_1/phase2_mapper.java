package cwwu.hw2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class phase2_mapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		// sample input string:
		// 				Source Page		Source PageRank		OutLinkSet
		//				0				1.0					5,3,2,1,4,10,9,8,7,6
		//				[0]				[1]					[2]
		// split the input string from value
		String line = ivalue.toString();
		String[] token = line.split("\t");
		
		if (token.length < 3) return;
		String[] pageLinked = token[2].split(",");
		
		double prValue = Double.parseDouble(token[1]);
		Integer numOutPage = pageLinked.length;
		
		
		// sample output string:
		//		Linked Page 	Source Page		Source PageRank		Source OutLinks
		//		5 				0 				1.0 				10 
		//		9				0 				1.0 				10
		for (int i=0; i<numOutPage; ++i){
			String out = token[0];
			out += (" " + prValue + " " + numOutPage);
			context.write(new Text(pageLinked[i]), new Text(out));
		}
		context.write(new Text(token[0]), new Text(">" + token[2]));
	}
}
