package wikiParser.mapReduce.graphs;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import wikiParser.mapReduce.util.SimpleJobConf;
import wikiParser.mapReduce.util.UniqueConcatenateReduce;

public class CommutativeLinkMapReduce {
	
	/*
	 * Takes InitialLinkMapReduce's key-value pairs and applies all links commutatively.
	 */

	public static class Map extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text value, OutputCollector<Text, Text> output, 
				Reporter reporter) throws IOException {
			/*
			 * Input: ArticleID-Links key-value pairs.
			 * Output: ID-ID pairs.
			 * 1. For each link, emit a link-key pair and a key-link pair.
			 */
			String valueType = null, linkType = null, valueId = null, keyType = null, keyId = null;
			for (String link : value.toString().split(" ")) {
				if (link.length() > 3) {
					valueType = link.substring(0, 1);
					linkType = link.substring(1, 3);
					valueId = link.substring(3);
					keyType = key.toString().substring(0,1);
					keyId = key.toString().substring(1);
					output.collect(key, new Text(link));
					output.collect(new Text(valueType+valueId), new Text(keyType+linkType+keyId));
				}
			}
		}
	}

	public static void runMe(String inputFiles[], String outputDir, String jobName) throws IOException{
		SimpleJobConf conf = new SimpleJobConf(Map.class, UniqueConcatenateReduce.class, inputFiles, outputDir, jobName);
		conf.run();
	}
	
}
