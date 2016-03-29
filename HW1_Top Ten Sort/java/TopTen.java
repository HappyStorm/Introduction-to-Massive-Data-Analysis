package haley.cwwu;

import java.util.TreeMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Comparator;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


class cmp_ds implements Comparator<LongWritable>{
    @Override
    public int compare(LongWritable a, LongWritable b){
    return (a.get() - b.get() > 0) ? 1 : -1;
  }
}

public class TopTen {

  public static class TopTenMapper
       extends Mapper<LongWritable, Text, LongWritable, NullWritable>{
          
          private static TreeMap<LongWritable, LongWritable> Tmap = 
                    new TreeMap<LongWritable, LongWritable>(new cmp_ds());

          public void map(LongWritable key, Text value, Context context
                          ) throws IOException, InterruptedException {

            // split the input string from value
            String line = value.toString();
            String[] token = line.split("\t");

            // parse the string to Long num, then push it into TreeMap
            for(int i=0; i<token.length; ++i)
              Tmap.put(new LongWritable(Long.parseLong(token[i])),
                        new LongWritable(Long.parseLong(token[i])));

            // iterate the TreeMap to remain its size < 10
            Iterator<Entry<LongWritable, LongWritable>> iter = Tmap.entrySet().iterator();
            Entry<LongWritable, LongWritable> entry = null;
            while(Tmap.size()>10){
              entry = iter.next();
              iter.remove();
            }
          }

          // write the map output to reducer
          protected void cleanup(Context context) 
            throws IOException, InterruptedException {
              for(LongWritable lw: Tmap.values())
                context.write(lw, NullWritable.get());
          }

  }

  public static class TopTenReducer
       extends Reducer<LongWritable, NullWritable, LongWritable, NullWritable> {
          
          private static TreeMap<LongWritable, LongWritable> Tmap =
                    new TreeMap<LongWritable, LongWritable>(new cmp_ds());

          public void reduce(Iterable<LongWritable> key, NullWritable values,
                             Context context
                             ) throws IOException, InterruptedException {
            
            // read the value from key
            for(LongWritable val : key)
              Tmap.put(val, val);
            
            // iterate the TreeMap to remain its size < 10
            Iterator<Entry<LongWritable, LongWritable>> iter = Tmap.entrySet().iterator();
            Entry<LongWritable, LongWritable> entry = null;
            while(Tmap.size()>10){
              entry = iter.next();
              iter.remove();
            }

            // write the file output
            for(LongWritable lw: Tmap.descendingMap().values())
              context.write(lw, NullWritable.get());
          }
  }

  public static void main(String[] args) throws Exception {
    
    // set job configuration
    Configuration conf = new Configuration();
    Job job = new Job(conf, "Top 10 Sort");
    job.setJarByClass(TopTen.class);
    
    // set mapper's attributes
    job.setMapperClass(TopTenMapper.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(NullWritable.class);
    
    // set reducer's attributes
    job.setReducerClass(TopTenReducer.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(NullWritable.class);

    // add file input/output path
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}