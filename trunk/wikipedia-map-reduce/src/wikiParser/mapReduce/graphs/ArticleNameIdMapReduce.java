package wikiParser.mapReduce.graphs;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import wmr.core.Page;
import wmr.core.PageParser;
import wikiParser.mapReduce.util.IdentityReduce;
import wikiParser.mapReduce.util.MapReduceUtils;
import wikiParser.mapReduce.util.SimpleJobConf;
import wmr.util.LzmaPipe;

public class ArticleNameIdMapReduce {

    public static class Map extends MapReduceBase implements Mapper<Text, Text, Text, Text> {

        public void map(Text key, Text value, OutputCollector<Text, Text> output,
                Reporter reporter) throws IOException {
            /*
             * Input: ArticleID-7zipHash key-value pairs.
             * Output: ArticleID-ArticleName key-value pairs.
             * 1. Unzip value
             * 2. Get page info
             * 3. Emit individual name-ID pairs
             */
            LzmaPipe pipe = null;
            try {
                byte[] unescaped = MapReduceUtils.unescape(value.getBytes(), value.getLength());
                pipe = new LzmaPipe(unescaped);
                PageParser parser = new PageParser(pipe.decompress());
                Page article = parser.getArticle();
                output.collect(new Text(article.toUnderscoredString()), new Text("a" + article.getId()));
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

    public static void main(String args[]) throws IOException {
        SimpleJobConf conf = new SimpleJobConf(Map.class,
                IdentityReduce.class, MapReduceUtils.S3_INPUTS,
                MapReduceUtils.S3_OUTPUT + "/articleIds",
                "articleIdMapReduce");
        conf.getConf().setNumMapTasks(40);
        conf.getConf().setNumReduceTasks(10);
    }
}
