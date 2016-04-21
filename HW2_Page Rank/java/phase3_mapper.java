package cwwu.hw2;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class phase3_mapper extends Mapper<LongWritable, Text, FloatWritable, Text> {

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		// split the input string from value
        String line = ivalue.toString();
        String[] token = line.split("\t");
        context.write(new FloatWritable(Float.parseFloat(token[1])), new Text(token[0]));
	}
}
