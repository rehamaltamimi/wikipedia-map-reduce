/**
 * 
 */
package wikiParser.mapReduce.util;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class IdentityMap extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
	public void map(Text key, Text value, OutputCollector<Text, Text> output, 
			Reporter reporter) throws IOException {
		output.collect(new Text(""), value);
	}
}