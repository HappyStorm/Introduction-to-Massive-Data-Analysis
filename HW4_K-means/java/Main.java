package cwwu.haley;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Main {
	private static String PREFIX_ROOT = "/hw4/";
	private static String PREFIX_DATA_EU = "/hw4/eu/data/";
	private static String PREFIX_ANS_EU = "/hw4/eu/ans/";
	private static String PREFIX_C1_EU = "/hw4/eu/c1/";
	private static String PREFIX_C2_EU = "/hw4/eu/c2/";
	private static String PREFIX_DATA_MA = "/hw4/ma/data/";
	private static String PREFIX_ANS_MA = "/hw4/ma/ans/";
	private static String PREFIX_C1_MA = "/hw4/ma/c1/";
	private static String PREFIX_C2_MA = "/hw4/ma/c2/";
	private int NumClusters;
	private int NumDimensions;
	
	// args[0] = data.txt, args[1] = c1.txt, args[2] = c2.txt
	public static void main(String[] args) throws Exception {
		Main main = new Main();
		main.NumClusters = main.getNumCluster(args[1]);
		main.NumDimensions = main.getNumDimension(args[0]);
		
	    // pre-processing: input formatted(for Euclidean)
		long timePreProcessingEUStart = System.nanoTime();
		@SuppressWarnings("unused")
		PreProcessing preprocessingEU = new PreProcessing(args[0], args[1], args[2],
														  PREFIX_DATA_EU + "data_",
														  PREFIX_C1_EU + "c1_00.txt",
														  PREFIX_C2_EU + "c2_00.txt",
														  main.NumClusters);
		long timePreProcessingEUElapsed = System.nanoTime() - timePreProcessingEUStart;
		System.out.println("PreProcessing-EU Elapsed Time: " + timePreProcessingEUElapsed + "\n");
		
		// pre-processing: input formatted(for Manhattan)
		long timePreProcessingMAStart = System.nanoTime();
		@SuppressWarnings("unused")
		PreProcessing preprocessingMA = new PreProcessing(args[0], args[1], args[2],
												  		  PREFIX_DATA_MA + "data_",
													  	  PREFIX_C1_MA + "c1_00.txt",
														  PREFIX_C2_MA + "c2_00.txt",
														  main.NumClusters);
		long timePreProcessingMAElapsed = System.nanoTime() - timePreProcessingMAStart;
		System.out.println("PreProcessing-MA Elapsed Time: " + timePreProcessingMAElapsed + "\n");
		
		// run kmenasEuclidean
		long timeEuclideanStart = System.nanoTime();
		for(int i=0; i<20; ++i){
			KmeansEuclidean KmeanEu = new KmeansEuclidean(main.NumClusters, main.NumDimensions, i);
			KmeanEu.runKmeansEuclideanAll(PREFIX_DATA_EU, PREFIX_C1_EU, PREFIX_C2_EU,
										  PREFIX_DATA_EU, PREFIX_C1_EU, PREFIX_C2_EU, PREFIX_ANS_EU);
		}
		long timeEuclideanElapsed = System.nanoTime() - timeEuclideanStart;
		System.out.println("Euclidean Elapsed Time: " + timeEuclideanElapsed + "\n");
		
		
		// run kmenasManhattan
		long timeManhattanStart = System.nanoTime();
		for(int i=0; i<20; ++i){
			KmeansManhattan KmeanMa = new KmeansManhattan(main.NumClusters, main.NumDimensions, i);
			KmeanMa.runKmeansManhattanAll(PREFIX_DATA_MA, PREFIX_C1_MA, PREFIX_C2_MA,
										  PREFIX_DATA_MA, PREFIX_C1_MA, PREFIX_C2_MA, PREFIX_ANS_MA);
		}
		long timeManhattanElapsed = System.nanoTime() - timeManhattanStart;
		System.out.println("Manhattan Elapsed Time: " + timeManhattanElapsed + "\n");
		
		long timeTotalElapsed = timePreProcessingEUElapsed + timePreProcessingMAElapsed +
								timeEuclideanElapsed + timeManhattanElapsed;
		System.out.println("Total Elapsed Time: " + timeTotalElapsed + "\n");
		
		Path pathTime = new Path(PREFIX_ROOT + "time.txt");
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedWriter brTime = new BufferedWriter(new OutputStreamWriter(fs.create(pathTime)));
        brTime.write("PreProcessing-EU Elapsed Time: " + timePreProcessingEUElapsed + "\n");
        brTime.write("PreProcessing-MA Elapsed Time: " + timePreProcessingMAElapsed + "\n");
        brTime.write("Euclidean Elapsed Time: " + timeEuclideanElapsed + "\n");
        brTime.write("Manhattan Elapsed Time: " + timeManhattanElapsed + "\n");
        brTime.write("Total Elapsed Time: " + timeTotalElapsed + "\n");
        brTime.close();
	}
	
	private int getNumCluster(String inputPath) throws IOException{
		int numCluster = 0;
		try{
            Path pt = new Path(inputPath);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            while (br.ready()) {
            	br.readLine();
            	++numCluster;
            }
            br.close();
        }catch(Exception e){
        }
		return numCluster;
	}
	
	@SuppressWarnings("unused")
	private int getNumPoint(String inputPath) throws IOException{
		int numPoint = 0;
		try{
            Path pt = new Path(inputPath);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            while (br.ready()) {
            	br.readLine();
            	++numPoint;
            }
            br.close();
        }catch(Exception e){
        }
		return numPoint;
	}
	
	private int getNumDimension(String inputPath) throws IOException{
		int numDimension = 0;
		try{
            Path pt = new Path(inputPath);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            while(br.ready()) {
            	String line = br.readLine();
            	String[] token = line.split(" ");
            	numDimension = token.length;
            	break;
            }
            br.close();
        }catch(Exception e){
        }
		return numDimension;
	}

	@SuppressWarnings("unused")
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
}
