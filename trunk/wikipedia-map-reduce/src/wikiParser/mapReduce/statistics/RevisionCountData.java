package wikiParser.mapReduce.statistics;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import wmr.core.PageParser;
import wmr.core.Revision;
import wmr.util.LzmaDecompresser;
import wikiParser.mapReduce.util.*;

public class RevisionCountData {

    public static class Map extends MapReduceBase implements Mapper<Text, Text, Text, Text> {

        public void map(Text key, Text value, OutputCollector<Text, Text> output,
                Reporter reporter) throws IOException {
            /*
             * Input: ArticleID-7zipHash key-value pairs.
             * Output: ArticleID-ArticleName key-value pairs.
             * 1. Unzip value
             * 2. Get page info
             * 3. Emit individual ID-name pairs
             */
            LzmaDecompresser pipe = null;
            try {
                byte[] unescaped = MapReduceUtils.unescape(value.getBytes(), value.getLength());
                pipe = new LzmaDecompresser(unescaped);
                PageParser parser = new PageParser(pipe.decompress());
                parser.setStoreFullTextInArticle(false);
                parser.setStoreRevisionMetadata(false);
                int revCount = 0;
                Revision rev = parser.getNextRevision();
                if (rev == null) {
                    throw new Exception("no revisions");
                }
                revCount++;
                while (rev != null) {
                    rev = parser.getNextRevision();
                    revCount++;
                }
                output.collect(new Text(parser.getArticle().getId()), new Text(Integer.toString(revCount)));
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

    public static void runMe(String inputs[], String outputDir, String jobName) throws IOException {
        SimpleJobConf conf = new SimpleJobConf(Map.class, IdentityReduce.class, inputs, outputDir, jobName);
        conf.run();
    }
}
