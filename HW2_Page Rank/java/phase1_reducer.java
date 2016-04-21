package cwwu.hw2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class phase1_reducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String pr_links = "1.0\t";
		boolean first = true;
		
		// process values
		for (Text val : values) {
			if (!first) pr_links += ',';
			pr_links += val.toString();
			first = false;
		}
		
		// write the output to phase_2
		context.write(_key, new Text(pr_links));
	}

}
