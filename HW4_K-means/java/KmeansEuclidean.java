package cwwu.haley;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KmeansEuclidean {
	private static NumberFormat nf = new DecimalFormat("00");
	protected final static int MAX_CLUSTERS = 10;
	protected final static int MAX_DIMENSIONS = 58;
	private static int iterID;
	
	public KmeansEuclidean(int NumClusters, int NumDimensions, int iterationID){
//		KmeansEuclidean.MAX_CLUSTERS = NumClusters;
//		KmeansEuclidean.MAX_DIMENSIONS = NumDimensions;
		KmeansEuclidean.iterID = iterationID;
	}
	
	public void runKmeansEuclideanAll(String inPath_data, String inPath_cluster_c1, String inPath_cluster_c2,
			 					      String outPath_data, String outPath_cluster_c1, String outPath_cluster_c2,
			 					      String outPath_costFunc) throws Exception{
		this.runKmeansEuclideanC1(inPath_data, inPath_cluster_c1, outPath_data, outPath_cluster_c1, outPath_costFunc);
		this.runKmeansEuclideanC2(inPath_data, inPath_cluster_c2, outPath_data, outPath_cluster_c2, outPath_costFunc);
	}
	
	// inPath_data:		 "/hw4/eu/data/"
	// inPath_cluster:	 "/hw4/eu/c1/"
	// outPath_data:	 "/hw4/eu/data/"
	// outPath_cluster:	 "/hw4/eu/c1/"
	// outPath_costFunc: "/hw4/eu/ans/"
	public void runKmeansEuclideanC1(String inPath_data, String inPath_cluster, String outPath_data,
									 String outPath_cluster, String outPath_costFunc) throws Exception{
		Configuration conf = new Configuration();
		conf.set("clusterInPath", inPath_cluster);
		conf.set("clusterOutPath", outPath_cluster);
		conf.set("dataInPath", inPath_data);
		conf.set("dataOutPath", outPath_data);
		conf.set("costFuncOutPath", outPath_costFunc);
		conf.set("iterID", String.valueOf(iterID));
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "K-Means Algorithm with Euclidean Cost Function(c1)");
		job.setJarByClass(KmeansEuclidean.class);
		job.setMapperClass(KmeansEuclideanMapperC1.class);
		job.setReducerClass(KmeansEuclideanReducerC1.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(inPath_data + "data_c1_" + nf.format(iterID) + ".txt"));
		FileOutputFormat.setOutputPath(job, new Path(outPath_data + "data_c1_" + nf.format(iterID+1) + ".txt"));
		
		job.waitForCompletion(true);
	}
	
	
	// inPath_data:		 "/hw4/eu/data/"
	// inPath_cluster:	 "/hw4/eu/c2/"
	// outPath_data:	 "/hw4/eu/data/"
	// outPath_cluster:	 "/hw4/eu/c2/"
	// outPath_costFunc: "/hw4/eu/ans/"
	public void runKmeansEuclideanC2(String inPath_data, String inPath_cluster, 
			   String outPath_data, String outPath_cluster, String outPath_costFunc) throws Exception{
		Configuration conf = new Configuration();
		conf.set("clusterInPath", inPath_cluster);
		conf.set("clusterOutPath", outPath_cluster);
		conf.set("dataInPath", inPath_data);
		conf.set("dataOutPath", outPath_data);
		conf.set("costFuncOutPath", outPath_costFunc);
		conf.set("iterID", String.valueOf(iterID));
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "K-Means Algorithm with Euclidean Cost Function(c2)");
		job.setJarByClass(KmeansEuclidean.class);
		job.setMapperClass(KmeansEuclideanMapperC2.class);
		job.setReducerClass(KmeansEuclideanReducerC2.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(inPath_data + "data_c2_" + nf.format(iterID) + ".txt"));
		FileOutputFormat.setOutputPath(job, new Path(outPath_data + "data_c2_" + nf.format(iterID+1) + ".txt"));
		
		job.waitForCompletion(true);
	}
	
	public static class KmeansEuclideanMapperC1 extends Mapper<Object, Text, IntWritable, Text> {
		private Point[] cluster;

		@Override
	    protected void setup(Context context) throws IOException, InterruptedException{
	    	cluster = new Point[MAX_CLUSTERS];
	    	for(int i=0; i<MAX_CLUSTERS; ++i)
	    		cluster[i] = new Point(MAX_DIMENSIONS);

	    	Configuration conf = context.getConfiguration();
	    	String pathIn = conf.get("clusterInPath");
	    	FileSystem fs = FileSystem.get(conf);
	    	
	    	int iter = Integer.parseInt(conf.get("iterID"));
	    	Path path = new Path(pathIn + "c1_" + nf.format(iter) + ".txt");
	    	BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));

	    	while(br.ready()) {
	    		String read = br.readLine();
	    		String[] seperate = read.split("\t");
	    		String[] tokens = seperate[1].split(" ");
	    		int clusterID = Integer.parseInt(seperate[0]);
	    		cluster[clusterID].setPoint(tokens);
	    	}
	    	br.close();
	    }
	    
		@Override
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
	    	String text = value.toString();
	    	String[] seperate = text.split("\t");
	    	String[] tokens = seperate[1].split(" ");
	    	int clusterID = 0;
	    	Point point = new Point(MAX_DIMENSIONS);
	    	point.setPoint(tokens);

	    	double dist;
	    	double min = Double.MAX_VALUE;
	    	double temp_sum = 0.0f;
	    	for(int i=0 ; i<MAX_CLUSTERS; ++i) {
	    		temp_sum = 0.0f;
	    		for(int j=0; j<MAX_DIMENSIONS; ++j) {
	    			dist = point.getDimensionValue(j) - cluster[i].getDimensionValue(j);
	    			temp_sum += dist * dist;
	    		}
	    		if(min >= temp_sum) {
	    			min = temp_sum;
	    			clusterID = i;
	    		}
	    	}
	    	context.write(new IntWritable(clusterID), new Text(String.valueOf(min) + "\t" + seperate[1]));
	    }
	}
	
	public static class KmeansEuclideanReducerC1 extends Reducer<IntWritable, Text, NullWritable, Text> {
		private Point[] oldCluster;
		private Point[] newCluster;
		private double[] sum;
		private int[] clusterSize;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException{
			oldCluster = new Point[MAX_CLUSTERS];
			newCluster = new Point[MAX_CLUSTERS];
			clusterSize = new int[MAX_CLUSTERS];
			sum = new double[MAX_CLUSTERS];
	      
			for(int i=0; i<MAX_CLUSTERS; ++i) {
				oldCluster[i] = new Point(MAX_DIMENSIONS);
				newCluster[i] = new Point(MAX_DIMENSIONS);
				clusterSize[i] = 0;
				sum[i] = 0.0f;
			}

			Configuration conf = context.getConfiguration();
	    	String pathIn = conf.get("clusterInPath");
	    	FileSystem fs = FileSystem.get(conf);
	    	
	    	int iter = Integer.parseInt(conf.get("iterID"));
	    	Path path = new Path(pathIn + "c1_" + nf.format(iter) + ".txt");
	    	BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));

	    	while(br.ready()) {
	    		String read = br.readLine();
	    		String[] seperate = read.split("\t");
	    		String[] tokens = seperate[1].split(" ");
	    		int clusterID = Integer.parseInt(seperate[0]);
	    		oldCluster[clusterID].setPoint(tokens);
	    	}
	    	br.close();
		}
    
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			for(Text t : values){
				clusterSize[key.get()]++;
				Point tempPoint = new Point(MAX_DIMENSIONS);
				String read = t.toString();
				String[] seperate = read.split("\t");
				String[] tokens = seperate[1].split(" ");
				tempPoint.setPoint(tokens);
				double temp_sum = Double.parseDouble(seperate[0]);
				sum[key.get()] += temp_sum;

				for(int i=0; i<MAX_DIMENSIONS; ++i)
					newCluster[key.get()].setDimensionValue(i, newCluster[key.get()].getDimensionValue(i) + tempPoint.getDimensionValue(i));

				context.write(NullWritable.get(), new Text(String.valueOf(key.get()) + "\t" + seperate[1]));
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			int iter = Integer.parseInt(conf.get("iterID"));
			String ansPathOut = conf.get("costFuncOutPath");
			Path pathCost = new Path(ansPathOut + "cost_c1_" + nf.format(iter+1) + ".txt");
			FileSystem fs = FileSystem.get(conf);
			BufferedWriter bwCost = new BufferedWriter(new OutputStreamWriter(fs.create(pathCost, true)));

			double total = 0.0f;
			for(int i=0; i<MAX_CLUSTERS; ++i){
				bwCost.write(String.valueOf(i) + "\t" + String.valueOf(sum[i]) + "\n");
				total += sum[i];
			}
			bwCost.write("Total Cost: " + String.valueOf(total) + "\n");
			bwCost.close();

			for(int i=0; i<MAX_CLUSTERS; ++i)
				for(int j=0; j<MAX_DIMENSIONS; ++j)
					newCluster[i].setDimensionValue(j, newCluster[i].getDimensionValue(j) / (double)clusterSize[i]);
			
			String clusterPathOut = conf.get("clusterOutPath");
			Path pathNewC1 = new Path(clusterPathOut + "c1_" + nf.format(iter+1) + ".txt");
			BufferedWriter bwNewC1 = new BufferedWriter(new OutputStreamWriter(fs.create(pathNewC1, true)));

			for(int i=0; i<MAX_CLUSTERS; ++i)
				bwNewC1.write(String.valueOf(i) + "\t" + newCluster[i].toString() + "\n");
			bwNewC1.close();
		}
	}

	public static class KmeansEuclideanMapperC2 extends Mapper<Object, Text, IntWritable, Text> {
		private Point[] cluster;
	    
		@Override
	    protected void setup(Context context) throws IOException, InterruptedException{
	    	cluster = new Point[MAX_CLUSTERS];
	    	for(int i=0; i<MAX_CLUSTERS; ++i)
	    		cluster[i] = new Point(MAX_DIMENSIONS);

	    	Configuration conf = context.getConfiguration();
	    	String pathIn = conf.get("clusterInPath");
	    	FileSystem fs = FileSystem.get(conf);
	    	
	    	int iter = Integer.parseInt(conf.get("iterID"));
	    	Path path = new Path(pathIn + "c2_" + nf.format(iter) + ".txt");
	    	BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));

	    	while(br.ready()) {
	    		String read = br.readLine();
	    		String[] seperate = read.split("\t");
	    		String[] tokens = seperate[1].split(" ");
	    		int clusterID = Integer.parseInt(seperate[0]);
	    		cluster[clusterID].setPoint(tokens);
	    	}
	    	br.close();
	    }
	    
		@Override
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
	    	String text = value.toString();
	    	String[] seperate = text.split("\t");
	    	String[] tokens = seperate[1].split(" ");
	    	int clusterID = 0;
	    	Point point = new Point(MAX_DIMENSIONS);
	    	point.setPoint(tokens);

	    	double dist;
	    	double min = Double.MAX_VALUE;
	    	double temp_sum = 0.0f;
	    	for(int i=0 ; i<MAX_CLUSTERS; ++i) {
	    		temp_sum = 0.0f;
	    		for(int j=0; j<MAX_DIMENSIONS; ++j) {
	    			dist = point.getDimensionValue(j) - cluster[i].getDimensionValue(j);
	    			temp_sum += dist * dist;
	    		}
	    		if(min >= temp_sum) {
	    			min = temp_sum;
	    			clusterID = i;
	    		}
	    	}
	    	context.write(new IntWritable(clusterID), new Text(String.valueOf(min) + "\t" + seperate[1]));
	    }
	}
	
	public static class KmeansEuclideanReducerC2 extends Reducer<IntWritable, Text, NullWritable, Text> {
		private Point[] oldCluster;
		private Point[] newCluster;
		private double[] sum;
		private int[] clusterSize;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException{
			oldCluster = new Point[MAX_CLUSTERS];
			newCluster = new Point[MAX_CLUSTERS];
			clusterSize = new int[MAX_CLUSTERS];
			sum = new double[MAX_CLUSTERS];
	      
			for(int i=0; i<MAX_CLUSTERS; ++i) {
				oldCluster[i] = new Point(MAX_DIMENSIONS);
				newCluster[i] = new Point(MAX_DIMENSIONS);
				clusterSize[i] = 0;
				sum[i] = 0.0f;
			}

			Configuration conf = context.getConfiguration();
	    	String pathIn = conf.get("clusterInPath");
	    	FileSystem fs = FileSystem.get(conf);
	    	
	    	int iter = Integer.parseInt(conf.get("iterID"));
	    	Path path = new Path(pathIn + "c2_" + nf.format(iter) + ".txt");
	    	BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));

	    	while(br.ready()) {
	    		String read = br.readLine();
	    		String[] seperate = read.split("\t");
	    		String[] tokens = seperate[1].split(" ");
	    		int clusterID = Integer.parseInt(seperate[0]);
	    		oldCluster[clusterID].setPoint(tokens);
	    	}
	    	br.close();
		}
    
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			for(Text t : values){
				clusterSize[key.get()]++;
				Point tempPoint = new Point(MAX_DIMENSIONS);
				String read = t.toString();
				String[] seperate = read.split("\t");
				String[] tokens = seperate[1].split(" ");
				tempPoint.setPoint(tokens);
				double temp_sum = Double.parseDouble(seperate[0]);
				sum[key.get()] += temp_sum;

				for(int i=0; i<MAX_DIMENSIONS; ++i)
					newCluster[key.get()].setDimensionValue(i, newCluster[key.get()].getDimensionValue(i) + tempPoint.getDimensionValue(i));

				context.write(NullWritable.get(), new Text(String.valueOf(key.get()) + "\t" + seperate[1]));
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			int iter = Integer.parseInt(conf.get("iterID"));
			String ansPathOut = conf.get("costFuncOutPath");
			Path pathCost = new Path(ansPathOut + "cost_c2_" + nf.format(iter+1) + ".txt");
			FileSystem fs = FileSystem.get(conf);
			BufferedWriter bwCost = new BufferedWriter(new OutputStreamWriter(fs.create(pathCost, true)));

			double total = 0.0f;
			for(int i=0; i<MAX_CLUSTERS; ++i){
				bwCost.write(String.valueOf(i) + "\t" + String.valueOf(sum[i]) + "\n");
				total += sum[i];
			}
			bwCost.write("Total Cost: " + String.valueOf(total) + "\n");
			bwCost.close();

			for(int i=0; i<MAX_CLUSTERS; ++i)
				for(int j=0; j<MAX_DIMENSIONS; ++j)
					newCluster[i].setDimensionValue(j, newCluster[i].getDimensionValue(j) / (double)clusterSize[i]);
			
			String clusterPathOut = conf.get("clusterOutPath");
			Path pathNewC1 = new Path(clusterPathOut + "c2_" + nf.format(iter+1) + ".txt");
			BufferedWriter bwNewC1 = new BufferedWriter(new OutputStreamWriter(fs.create(pathNewC1, true)));

			for(int i=0; i<MAX_CLUSTERS; ++i)
				bwNewC1.write(String.valueOf(i) + "\t" + newCluster[i].toString() + "\n");
			bwNewC1.close();
		}
	}
}
