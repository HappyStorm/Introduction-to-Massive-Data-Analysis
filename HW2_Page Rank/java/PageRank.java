package cwwu.hw2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

class cmp_id implements Comparator<Integer>{
    public int compare(Integer a, Integer b){
    return (a.intValue() - b.intValue() > 0) ? 1 : -1;
  }
}
class cmp_pr implements Comparator<Double>{
    public int compare(Double a, Double b){
    return (a.doubleValue() - b.doubleValue() > 0) ? -1 : 1;
  }
}

public class PageRank {

	private static NumberFormat nf = new DecimalFormat("00");
	private int NumberNode, NumberDeadEnd;
	private static String PREFIX = "part-r-00000";
	
	public static void main(String[] args) throws Exception {
		PageRank pagerank = new PageRank();
		pagerank.NumberNode = pagerank.getNumNode(args[0]);
		pagerank.NumberDeadEnd = pagerank.getNumDeadEnd(args[0]);
		
	    // phase_1: input formatted
	    pagerank.runInputFormatting(args[0], "/hw2/out/iter00");
	    
	    // phase_2: calculate new page rank
	    int iter = 0;
	    for (; iter < 20; ++iter)	    	
	    	 pagerank.runPageRankCalculation("/hw2/out/iter"+nf.format(iter), "/hw2/out/iter"+nf.format(iter + 1),
	    			 						 new Integer(pagerank.NumberNode), new Integer(pagerank.NumberDeadEnd));
	    // calculate page rank until converge
	    for (iter=19; ; ++iter){
	    	if (isConverge("/hw2/out/iter"+nf.format(iter) + "/" + PREFIX, "/hw2/out/iter"+nf.format(iter + 1) + "/" + PREFIX))
	    		break;
	    	pagerank.runPageRankCalculation("/hw2/out/iter"+nf.format(iter+1), "/hw2/out/iter"+nf.format(iter + 2),
	    									new Integer(pagerank.NumberNode), new Integer(pagerank.NumberDeadEnd));
	    }

	    // phase_3: re-order the results by page rank in descending order
	    pagerank.runPageRankReOrdering("/hw2/out/iter"+nf.format(20), "/hw2/ans");
	    pagerank.runPageRankReOrdering("/hw2/out/iter"+nf.format(iter), "/hw2/converge"); 
	    sortAnswer("/hw2/ans/part-r-00000", "/hw2/ans/ans_20.txt"); 			// sort 20-iter
	    sortAnswer("/hw2/converge/part-r-00000", "/hw2/converge/converge.txt");	// sort converge
	}

	private int getNumNode(String inputPath) throws IOException{
		int numNode = 0;
		try{
            Path pt = new Path(inputPath);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            while (br.ready()) {
            	String line = br.readLine();
            	if (line.startsWith("# Nodes: ")){
            		String[] token = line.split(" ");
            		numNode = Integer.parseInt(token[2]);
            		break;
            	}
            	else continue;
            }
            br.close();
        }catch(Exception e){
        }
		return numNode;
	}
	
	private int getNumDeadEnd(String inputPath) throws IOException{
		int numNode = 0;
		Set<Integer> pageSet = new HashSet<Integer>();
		try{
            Path pt = new Path(inputPath);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            while (br.ready()) {
            	String line = br.readLine();
            	if (Character.isDigit(line.charAt(0))){
            		String[] token = line.split("\t");
            		pageSet.add(new Integer(Integer.parseInt(token[0])));
            	}
            	else if (line.startsWith("# Nodes: ")){
            		String[] token = line.split(" ");
            		numNode = Integer.parseInt(token[2]);
            	}
            	else continue;
            }
            br.close();
        }catch(Exception e){
        }
		return numNode - pageSet.size();
	}
	
	private static boolean isConverge(String prevIter, String nextIter){
		try{
            Path pathPrev = new Path(prevIter), pathNext = new Path(nextIter);
            FileSystem fsPrev = FileSystem.get(new Configuration()),
            		   fsNext = FileSystem.get(new Configuration());
            BufferedReader brPrev = new BufferedReader(new InputStreamReader(fsPrev.open(pathPrev))),
            			   brNext = new BufferedReader(new InputStreamReader(fsNext.open(pathNext)));
            while (brPrev.ready() && brNext.ready()) {
            	String linePrev = brPrev.readLine(), lineNext = brNext.readLine();
            	String[] tokenPrev = linePrev.split("\t"), tokenNext = lineNext.split("\t");
            	double valPrev = Double.parseDouble(tokenPrev[1]), valNext = Double.parseDouble(tokenNext[1]);
            	if (Math.abs(valPrev - valNext) > 1e-9){
            		brPrev.close();
            		brNext.close();
            		return false;
            	}
            }
            brPrev.close();
    		brNext.close();
        }catch(Exception e){
        }
		return true;
	}
	
	private static void sortAnswer(String input, String output){
		try{
            Path pathIn = new Path(input), pathOut = new Path(output);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pathIn)));
            BufferedWriter wr = new BufferedWriter(new OutputStreamWriter(fs.create(pathOut)));
            
            ArrayList<WebPage> ansList = new ArrayList<WebPage>();
            while (br.ready()) {
            	String line = br.readLine();
            	String[] token = line.split("\t");
            	ansList.add(new WebPage(Integer.parseInt(token[1]), Double.parseDouble(token[0])));
            }
            br.close();
            
            Collections.sort(ansList, new Comparator<WebPage>(){  
				public int compare(WebPage w1, WebPage w2) {
					// TODO Auto-generated method stub
					return Integer.compare(w1.id, w2.id);
				}  
            });
            Collections.sort(ansList, new Comparator<WebPage>(){  
				public int compare(WebPage w1, WebPage w2) {
					// TODO Auto-generated method stub
					return Double.compare(w2.pr, w1.pr);
				}  
            });
            
            WebPage iter;
            for(int i=0; i<ansList.size(); ++i){
            	iter = ansList.get(i);
            	wr.write(String.format("%5d", iter.id) + "\t" + String.format("%.13f", iter.pr));
            	wr.newLine();
            }
            wr.close();
        }catch(Exception e){
        }
	}
	
	private void runInputFormatting(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
	  
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Input Formatting");
		job.setJarByClass(PageRank.class);
  
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setMapperClass(phase1_mapper.class);
		job.setReducerClass(phase1_reducer.class);

		job.waitForCompletion(true);
	}
  
	private void runPageRankCalculation(String inputPath, String outputPath, Integer numNode, Integer numDeadEnd) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("NumNode", numNode.toString());
		conf.set("NumDeadEnd", numDeadEnd.toString());
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Calculate PageRank");
		job.setJarByClass(PageRank.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setMapperClass(phase2_mapper.class);
		job.setReducerClass(phase2_reducer.class);

		job.waitForCompletion(true);
	}
	
	private void runPageRankReOrdering(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		  
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Re-Order PageRank");
		job.setJarByClass(PageRank.class);
 
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
 
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
 
        job.setMapperClass(phase3_mapper.class);
 
        job.waitForCompletion(true);
    }
}