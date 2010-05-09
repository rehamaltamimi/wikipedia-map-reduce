package wikiParser.mapReduce.graphs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

public class NameIdSubstitution {

	public static void main (String [] args) throws IOException {
		substitute(new File("/data/colin/nameIdMapReduceOut/article_output.txt"), new File("/data/colin/nameIdMapReduceOut/user_output.txt"), new File("/data/colin/links.cleaned.txt"), new File("/data/colin/sub_links.txt"));
	}

	public static void substitute(File articleIdFile, File userIdFile, File linkInFile, File linkOutFile) throws FileNotFoundException, IOException {
		/* Reads in the name:id pair file,
		 * builds a hashmap,
		 * reads in the link graph file,
		 * and subs
		 */
		HashMap<String, String> articleHashMap = new HashMap<String, String>();
		HashMap<String, String> userHashMap = new HashMap<String, String>();
		BufferedReader in = new BufferedReader(new FileReader(articleIdFile));
		BufferedWriter out = new BufferedWriter(new FileWriter(linkOutFile));
		String key = null, value = null, id=null, newValue = new String(""), newLine = new String("");
		String[] values = null;
		
		// article name:id pairs
		String line = in.readLine();
		while (line != null) {
			if (line.split("\t").length == 2) {
				key = line.split("\t")[0];
				value = line.split("\t")[1];
				articleHashMap.put(key, value.substring(1));
			}
			line = in.readLine();
		}
		in.close();
		
		// user name:id pairs
		in = new BufferedReader(new FileReader(userIdFile));
		line = in.readLine();
		while (line != null) {
			if (line.split("\t").length == 2) {
				key = line.split("\t")[0];
				value = line.split("\t")[1];
				userHashMap.put(key, value.substring(1));
			}
			line = in.readLine();
		}
		in.close();
		
		// link graph substitution
		in = new BufferedReader(new FileReader(linkInFile));
		line = in.readLine();
		int lineNum = 1;			//for testing
		while (line != null) {
			if (line.split("\t").length == 2) {		//this is a key:value pair
				key = line.split("\t")[0];
				value = line.split("\t")[1]; 
				values = value.split(" ");
                                StringBuilder builder = new StringBuilder(key + "\t");
				for (String link : values) {
					if (link.startsWith("u")) {
						id = userHashMap.get(link.substring(3));
					} else if (link.startsWith("a")) {
						id = articleHashMap.get(link.substring(3));
					}
					if (id == null) {
						builder.append(link+" ");
                                        } else {
						builder.append(link.substring(0, 3)+id+" ");
					}
				}
				out.write(builder.toString().trim());
                                out.write("\n");
				newValue = "";
				newLine = "";
			} else {	//this is not a key:value pair
				System.out.println(line.split("\t").length);
				System.out.println("error at line"+lineNum+": "+line);
			}
			line = in.readLine();
			lineNum++;
		}
		in.close();
		out.close();
	}

}
