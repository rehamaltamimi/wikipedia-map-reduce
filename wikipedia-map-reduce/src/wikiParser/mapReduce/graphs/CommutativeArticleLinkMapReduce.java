/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wikiParser.mapReduce.graphs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import wikiParser.mapReduce.util.SimpleJobConf;

/**
 *
 * @author Nathaniel Miller
 * 
 * This class properly implements commutativity for weighted edges
 */

public class CommutativeArticleLinkMapReduce {
        /*
	 * Takes InitialArticleLinkMapReduce's key-value pairs and applies all links commutatively.
	 */
    
    public static class Map extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text value, OutputCollector<Text, Text> output, 
				Reporter reporter) throws IOException {
			/*
			 * Input: ArticleID-Links key-value pairs.
			 * Output: ID-ID pairs.
			 * 1. For each link, emit a link-key pair and a key-link pair.
			 */
                    for (String link : value.toString().split(" ")) {
                        output.collect(key, new Text(link));
                        String valueType, linkType, valueId, keyType, keyId, weight;
                        valueType = link.substring(0, 1);
                        linkType = link.substring(1, 3);
                        weight = link.split("\\|")[1];
                        valueId = link.split("\\|")[2];
                        keyType = key.toString().substring(0,1);
                        keyId = key.toString().substring(1);
                        output.collect(new Text(valueType + valueId), new Text(keyType + linkType + "|" + weight + "|" + keyId));
                    }
		}
	}

    /*
     * Subtly different reduce method than in InitialArticleLinkMapReduce. Uses +, not Math.max in combining weights.
     */
    
        public static class Reduce extends MapReduceBase implements Reducer<Text,Text,Text,Text> {

        @Override
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            
            HashMap<String,String> edges = new HashMap<String, String>();
            while (values.hasNext()) {
                String v = values.next().toString();
                String k = v.split("\\|")[2];
                if (!edges.containsKey(k)) {
                    edges.put(k,v);
                } else {
                    String[] split = edges.get(k).split("\\|");
                    edges.put(k, split[0] + "|" + (Integer.parseInt(split[1]) + Integer.parseInt(v.split("\\|")[1])) + "|" + k);
                }
            }
            StringBuilder builder = new StringBuilder();
            for (String v  : edges.values()) {
                builder.append(v).append(" ");
            }
            output.collect(key, new Text(builder.toString()));
        }
        
    }
    
	public static void runMe(String inputFiles[], String outputDir, String jobName) throws IOException{
		SimpleJobConf conf = new SimpleJobConf(Map.class, Reduce.class, inputFiles, outputDir, jobName);
		conf.run();
	}
	
}
