package wikiParser.mapReduce.metrics;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;



public class Density {
	
	/*
	 * Two ways to do this.
	 * D = (2*mag(E))/(mag(V)*(mag(V)-1))
	 * 
	 * 1. Map/Reduce
	 * 	Map:
	 * 	-count value.split(' ') = edges = n
	 * 	-each map emits "E n" and "V 1"
	 *  Reduce:
	 *  -if "V": emit "V values.sum()"
	 *  -if "E": emit "E values.sum()"
	 *  Normal:
	 *  -read in values, do math
	 * 2. Normal
	 * 	-split(\t)
	 *  -[2].split(' ')
	 *  -count them = edges
	 *  -count num lines = vertices
	 *  -math
	 *  	-need to stream, since the files are large, but loop-friendly
	 */
	
	//TODO: library to read in graph
	
	public static void main (String [] args) throws IOException {
		calculateDensity(new File("/data/colin/comm_links.txt"), new File("/data/colin/results/density.txt"));
	}

	private static void calculateDensity(File inFile, File outFile) throws FileNotFoundException,
			IOException {
		BufferedReader in = new BufferedReader(new FileReader(inFile));
		BufferedWriter out = new BufferedWriter(new FileWriter(outFile));
		double e = 0;
		double[] edges = new double[13];
		for (int i=0;i<edges.length;i++) {
			edges[i] = 0;
		}
		double v = 0;
		String tmp = null;
		String line = new String("");
		while (line != null) {
			line = in.readLine();
			try {
				String values = line.split("\t")[1];
				String[] splitValues = values.split(" ");
				for (String value : splitValues) {
					tmp = value;
					e += 1;
					if (value.startsWith("a") || value.startsWith("u")) {
						int linkType = Integer.parseInt(value.substring(1,3));
						edges[linkType] += 1;
					}
				}
			}
			catch (NumberFormatException ex) {
				out.write("error at line "+v+": improperly typed link: "+tmp+"\n");
			}
			catch (Exception ex) {
				out.write("error at line "+v+"\n");
			}
			v += 1;
			if (v%5000 == 0) {
				out.write("-->read "+v+" lines\n");
			}
		}
		double density = (e)/(v*(v-1));	//e = 2*mag(E), because all edges are bidirectional in data
		double denSum = 0.0;
		out.write("===Results===\n");
		out.write("edges:  "+e+"\n");
		out.write("vertices:  "+v+"\n");
		out.write("edge counts by link type:\n");
		String linkType = null;
		double edge = 0;
		for (int i=0;i<edges.length;i++) {
			switch (i) {
				case 0:
					linkType = "article-article-link";
					edge = edges[i]+edges[i+1]; 
					break;
				case 1:
					linkType = null;
					break;
				case 2:
					linkType = "user-article-edit";
					edge = edges[i]+edges[i+3];
					break;
				case 3:
					linkType = "user-article-link";
					edge = edges[i]+edges[i+3];
					break;
				case 4:
					linkType = "user-article-mention";
					edge = edges[i]+edges[i+3];
					break;
				case 5:
					linkType = null;
					break;
				case 6:
					linkType = null;
					break;
				case 7:
					linkType = null;
					break;
				case 8:
					linkType = "user-user-edit";
					edge = edges[i];
					break;
				case 9:
					linkType = "user-user-link";
					edge = edges[i];
					break;
				case 10:
					linkType = "user-user-mention";
					edge = edges[i];
					break;
				case 11:
					linkType = "user-user-coauthor";
					edge = edges[i];
					break;
				case 12:
					linkType = "user-user-talk";
					edge = edges[i];
					break;
			}
			if (linkType != null) {
				out.write("\tlink type "+linkType+":  "+edge+"\n");
			}
		}
		out.write("density by link type:\n");
		for (int i=0;i<edges.length;i++) {
			switch (i) {
				case 0:
					linkType = "article-article-link";
					edge = edges[i]+edges[i+1]; 
					break;
				case 1:
					linkType = null;
					break;
				case 2:
					linkType = "user-article-edit";
					edge = edges[i]+edges[i+3];
					break;
				case 3:
					linkType = "user-article-link";
					edge = edges[i]+edges[i+3];
					break;
				case 4:
					linkType = "user-article-mention";
					edge = edges[i]+edges[i+3];
					break;
				case 5:
					linkType = null;
					break;
				case 6:
					linkType = null;
					break;
				case 7:
					linkType = null;
					break;
				case 8:
					linkType = "user-user-edit";
					edge = edges[i];
					break;
				case 9:
					linkType = "user-user-link";
					edge = edges[i];
					break;
				case 10:
					linkType = "user-user-mention";
					edge = edges[i];
					break;
				case 11:
					linkType = "user-user-coauthor";
					edge = edges[i];
					break;
				case 12:
					linkType = "user-user-talk";
					edge = edges[i];
					break;
			}
			double linkDensity = ((edge)/(v*(v-1)));
			if (linkType != null) {
				denSum += linkDensity;
				out.write("\tlink type "+linkType+":  "+linkDensity+"\n");
			}
		}
		out.write("avg density:  "+(denSum/9)+"\n");
		out.write("graph density:  "+density+"\n");
		//Math: (2*mag(E))/(mag(V)*(mag(V)-1))
		out.flush();
		out.close();
		in.close();
	}
}
