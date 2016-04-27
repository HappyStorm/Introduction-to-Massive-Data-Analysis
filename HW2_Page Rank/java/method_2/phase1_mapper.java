package cwwu.hw2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class phase1_mapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		
		// split the input string from value
		String line = ivalue.toString();
		String[] token = line.split("\t");

		// parse the token string(only content)
		if (token.length==2 && token[0].charAt(0)!='#')
			context.write(new Text(token[0]), new Text(token[1]));
	}
}
