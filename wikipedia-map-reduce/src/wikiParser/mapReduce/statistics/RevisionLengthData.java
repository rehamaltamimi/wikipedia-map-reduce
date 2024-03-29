package wikiParser.mapReduce.statistics;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import wmr.core.Page;
import wmr.core.PageParser;
import wmr.core.Revision;
import wikiParser.mapReduce.util.*;
import wmr.util.LzmaDecompresser;
import wmr.util.Utils;

public class RevisionLengthData {

	public static class Map extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text value, OutputCollector<Text, Text> output, 
				Reporter reporter) throws IOException {
			/*
			 * Input: ArticleID-7zipHash key-value pairs.
			 * Output: ArticleID-ArticleName key-value pairs.
			 * 1. Unzip value
			 * 2. Get revision lengths
			 * 3. Emit individual ArticleID_RevisionID-revision length pairs
			 */
			LzmaDecompresser pipe = null;
			try {
				byte [] unescaped = Utils.unescape(value.getBytes(), value.getLength());
				pipe = new LzmaDecompresser(unescaped);
				PageParser parser = new PageParser(pipe.decompress());
				parser.setStoreFullTextInArticle(false);
				parser.setStoreRevisionMetadata(false);
				Page art = parser.getArticle();
				Revision rev = parser.getNextRevision();
				while (rev != null) {
					output.collect(new Text(art.getId()+"_"+rev.getId()),
							new Text(Integer.toString(rev.getText().length())));
					rev = parser.getNextRevision();
				}
			} catch (Exception e) {
				System.err.println("error when processing " + key + ":");
				e.printStackTrace();
			} finally {
				if (pipe != null) {
					pipe.cleanup();
				}
			}
		}

		
	}

	public static void runMe(String[] inputs, String outputDir, String jobName) throws IOException{
            SimpleJobConf conf = new SimpleJobConf(Map.class, IdentityReduce.class, inputs, outputDir, jobName);
            conf.run();
	}

}
