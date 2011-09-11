package wikiParser.mapReduce.graphs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import wikiParser.Page;

import wikiParser.PageParser;
import wikiParser.Edge;
import wikiParser.Revision;
import wikiParser.edges.ArticleArticleGenerator;
import wikiParser.mapReduce.util.KeyValueTextInputFormat;
import wikiParser.mapReduce.util.MapReduceUtils;
import wikiParser.mapReduce.util.SimpleJobConf;
import wikiParser.util.LzmaPipe;
/**
 * Creates Article to Article graph with directed edges using links only
 * @author Nathaniel Miller
 */
public class InitialArticleLinkMapReduce extends Configured implements Tool {

    /*
     * Takes key-value 7zip hashes and outputs ID-links pairs.
     */
    public static class Map extends Mapper<Text,Text,Text,Text> {

        public void map(Text key, Text value, OutputCollector<Text, Text> output,
                Reporter reporter) throws IOException {
            
            /*
             * Input: ArticleID-7zipHash key-value pairs.
             * Output: ArticleID-Edge key-value pairs with a 2-digit connection type demarcation.
             * 1. Unzip value
             * 2. Get page info
             * 3. Build connection list
             * 4. Find links
             * 5. Emit individual ID-link pairs with connection type markers.
             */
            LzmaPipe pipe = null;
            try {
                reporter.progress();
                int length = MapReduceUtils.unescapeInPlace(value.getBytes(), value.getLength());
                pipe = new LzmaPipe(value.getBytes(), length);
                PageParser parser = new PageParser(pipe.decompress());
                Page article = parser.getArticle();
                System.err.println("processing article " + key + "(" + parser.getArticle().getName() + ")");
                ArticleArticleGenerator edgeGenerator = new ArticleArticleGenerator();
                Revision latest = null;
                while (true) {
                    Revision rev = parser.getNextRevision();
                    if (rev == null) {
                        break;
                    }
                    latest = rev;
                }
                if (latest != null) {
                    for (Edge link : edgeGenerator.generateWeighted(article, latest)) {
                        if (article.isUserTalk() || article.isUser()) {
                            output.collect(new Text("u" + article.getUser().getId()), new Text(link.toOutputString()));
                        } else {
                            output.collect(new Text("a" + article.getId()), new Text(link.toOutputString()));
                        }
                    }
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

    public static class Reduce extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            
            HashMap<String,String> edges = new HashMap<String, String>();
            while (values.hasNext()) {
                String v = values.next().toString();
                String k = v.split("\\|")[2];
                if (!edges.containsKey(k)) {
                    edges.put(k,v);
                } else {
                    String[] split = edges.get(k).split("\\|");
                    edges.put(k, split[0] + "|" + Math.max(Integer.parseInt(split[1]), Integer.parseInt(v.split("\\|")[1])) + "|" + k);
                }
            }
            StringBuilder result = new StringBuilder();
            for (String v  : edges.values()) {
                result.append(v).append(" ");
            }
            output.collect(key, new Text(result.toString()));
        }
        
    }
    
    public static void runMe(String inputFiles[], String outputDir, String jobName) throws IOException {
        SimpleJobConf conf = new SimpleJobConf(Map.class, Reduce.class, inputFiles, outputDir, jobName);
        conf.run();
    }
    
    public int run(String args[]) throws Exception {

        if (args.length < 2) {
            System.out.println("usage: [input output]");
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        Configuration conf = getConf();
        Job job = new Job(conf, this.getClass().toString());

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        
        job.setJarByClass(InitialArticleLinkMapReduce.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        FileSystem hdfs = FileSystem.get(outputPath.toUri(), conf);
        if (hdfs.exists(outputPath)) {
            hdfs.delete(outputPath, true);
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Dispatches command-line arguments to the tool via the
     * <code>ToolRunner</code>.
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new InitialArticleLinkMapReduce(), args);
        System.exit(res);
    }
}
