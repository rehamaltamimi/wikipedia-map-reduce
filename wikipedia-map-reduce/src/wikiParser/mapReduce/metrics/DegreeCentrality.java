package wikiParser.mapReduce.metrics;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;



public class DegreeCentrality {

	/*
	 * Cd(G) = (Sigma{i=1}{mag(V)}(Cd(v*)-Cd(vi)))/(n-2)
	 * where v* is the node with the highest degree centrality and
	 * n is the number of vertices (mag(V)), and
	 * Cd(v) = deg(v)/(n-1), where deg(v) is the number of links
	 * 1. Map/Reduce
	 *  -About 3 steps: one for each deg(v), one for n, one for math.
	 * 2. Normal
	 * 	-for each line, count the deg(v), save it in a hashtable key:deg(v)
	 * 	-also, n++
	 * 	-at the end, find the max, do the math (easy w/ hashtable)
	 * 	-O(4n) = O(n)
	 */
	
	public static void main(String [] args) throws IOException {
		calculateDegreeCentrality2N(new File("/data/colin/comm_links.txt"), new File("/data/colin/results/degree_centrality.txt"));
	}

	private static void calculateDegreeCentrality(File inFile, File outFile)
			throws FileNotFoundException, IOException {
		BufferedReader in  = new BufferedReader(new FileReader(inFile));
		BufferedWriter out = new BufferedWriter(new FileWriter(outFile));
		HashMap<String, Integer> degreeMap = new HashMap<String, Integer>();
		String largestKey = "";
		double largestValue = 0;
		double n = 0;
		String line = in.readLine();
		while (line != null) {
			try {
				if (line.split("\t").length == 2) {
					String key = line.split("\t")[0];
					String values = line.split("\t")[1];
					String[] splitValues = values.trim().split(" ");
					int tempDeg = 0;
					for (String value : splitValues) {
						tempDeg++;
					}
					degreeMap.put(key, new Integer(tempDeg));
					if (tempDeg > degreeMap.get(largestKey)) {
						largestValue = tempDeg;
						largestKey = key;
					}
				}
			}
			catch (Exception ex) {
				out.write("error processing line "+n+"\n");
			}
			n += 1;
			if (n%5000 == 0) {
				out.write("-->read "+n+" lines\n");
			}
			line = in.readLine();
		}
		double numeratorSum = 0;
		for (String key : degreeMap.keySet()) {
			numeratorSum += (individualDegreeCentrality(largestValue, n)-
					individualDegreeCentrality(degreeMap.get(key), n));
		}
		double globalDegreeCentrality = ((float) numeratorSum)/((float) (n-2));
		out.write("===Results===\n");
		out.write("Global Degree Centrality: "+globalDegreeCentrality+"\n");
		System.out.println("Global Degree Centrality: "+globalDegreeCentrality);
		//Math: Cd(G) = (Sigma{i=1}{mag(V)}(Cd(v*)-Cd(vi)))/(n-2)
		out.flush();
		out.close();
		in.close();
	}
	
	private static double individualDegreeCentrality(double degree, double magV) {
		return degree/(magV-1);
	}

	private static double[] addDegreeToTopTenArrays(double degree, double[] array, String key, String[] idArray) {
		for (int j=0;j<array.length;j++) {
			if (degree > array[j]) {
				for (int i=array.length-1;i>j;i--) {
					array[i] = array[i-1];
					idArray[i] = idArray[i-1];
				}
				array[j] = degree;
				idArray[j] = key;
				break;
			}
		}
		return array;
	}

	public static void calculateDegreeCentrality2N(File inFile, File outFile) throws FileNotFoundException, IOException {
		BufferedReader in  = new BufferedReader(new FileReader(inFile));
		BufferedWriter out = new BufferedWriter(new FileWriter(outFile));
		double[] topTen = new double[10];
		String[] topTenIDs = new String[10];
		for (int i=0;i<topTen.length;i++) { topTen[i] = 0; topTenIDs[i] = ""; }
		double n = 0;
		String line = in.readLine();
		while (line != null) {
			try {
				if (line.split("\t").length == 2) {
					String key = line.split("\t")[0];
					String values = line.split("\t")[1];
					String[] splitValues = values.trim().split(" ");
					long tempDeg = 0;
					for (String value : splitValues) {
						tempDeg++;
					}
					addDegreeToTopTenArrays(tempDeg, topTen, key, topTenIDs);
				}
			}
			catch (Exception ex) {
				out.write("error processing line "+n+"\n");
			}
			n += 1;
			if (((long)n)%5000 == 0) {
				out.write("-->read "+n+" lines\n");
			}
			line = in.readLine();
		}
		in.close();
		out.write("===Second Pass===\n");
		in  = new BufferedReader(new FileReader(inFile));
		line = in.readLine();
		double numeratorSum = 0;
		double indivSum = 0;
		double degSum = 0;
		long steps = 0;
		while (line != null) {
			try{
				if (line.split("\t").length == 2) {
					String key = line.split("\t")[0];
					String values = line.split("\t")[1];
					String[] splitValues = values.trim().split(" ");
					long tempDeg = 0;
					for (String value : splitValues) {
						tempDeg += 1;
					}
					degSum += tempDeg;
					numeratorSum += (individualDegreeCentrality(topTen[0], n)-
						individualDegreeCentrality(tempDeg, n));
					indivSum += individualDegreeCentrality(tempDeg, n);
				}
			}
			catch (Exception ex) {
				out.write("error processing line "+n+"\n");
			}
			steps++;
			line = in.readLine();
		}
		double globalDegreeCentrality = (numeratorSum)/(n-2);
		out.write("===Results===\n");
		out.write("Top Ten Degrees:\n");
		for (int i=0;i<topTen.length;i++) {
			out.write("\t"+(i+1)+". "+topTenIDs[i]+":  "+topTen[i]+"\n");
		}
		out.write("Top Ten DCs:\n");
		for (int i=0;i<topTen.length;i++) {
			out.write("\t"+(i+1)+". "+topTenIDs[i]+":  "+individualDegreeCentrality(topTen[i], n)+"\n");
		}
		out.write("Average Degree:  "+(degSum/n)+"\n");
		out.write("Average Degree Centrality:  "+(indivSum/n)+"\n");
		out.write("Global Degree Centrality:  "+globalDegreeCentrality+"\n");
		//Math: Cd(G) = (Sigma{i=1}{mag(V)}(Cd(v*)-Cd(vi)))/(n-2)
		out.flush();
		out.close();
	
	}
}
