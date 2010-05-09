package wikiParser.mapReduce.graphs;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import wikiParser.Page;
import wikiParser.PageParser;
import wikiParser.Revision;
import wikiParser.mapReduce.util.IdentityReduce;
import wikiParser.mapReduce.util.MapReduceUtils;
import wikiParser.mapReduce.util.SimpleJobConf;
import wikiParser.util.LzmaPipe;

public class UserNameIdMapReduce {

    public static class Map extends MapReduceBase implements Mapper<Text, Text, Text, Text> {

        public void map(Text key, Text value, OutputCollector<Text, Text> output,
                Reporter reporter) throws IOException {
            /*
             * Input: ArticleID-7zipHash key-value pairs.
             * Output: ArticleID-ArticleName key-value pairs.
             * 1. Unzip value
             * 2. Get page info
             * 3. Emit individual name-ID pairs
             *
             * Alternate idea for users:
             *  ouput all editor id -> editor name pairs
             */
            LzmaPipe pipe = null;
            try {
                byte[] unescaped = MapReduceUtils.unescape(value.getBytes(), value.getLength());
                pipe = new LzmaPipe(unescaped);
                PageParser parser = new PageParser(pipe.decompress());
                Page article = parser.getArticle();
                while (true) {
                    Revision rev = parser.getNextRevision();
                    if (rev == null) {
                        break;
                    }
                    output.collect(new Text(rev.getContributor().toUnderscoredString()), new Text("u" + rev.getContributor().getId()));
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

    public static void runMe(String[] inputs, String outputDir, String jobName) throws IOException {
        SimpleJobConf conf = new SimpleJobConf(Map.class, IdentityReduce.class, inputs, outputDir, jobName);
        conf.run();
    }
}
