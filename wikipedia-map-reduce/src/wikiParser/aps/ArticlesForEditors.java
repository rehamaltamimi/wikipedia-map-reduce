package wikiParser.aps;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;

import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import wikiParser.Page;

import wikiParser.PageParser;
import wikiParser.Revision;
import wikiParser.User;
import wikiParser.edges.ArticleArticleGenerator;
import wikiParser.mapReduce.util.KeyValueTextInputFormat;
import wikiParser.mapReduce.util.MapReduceUtils;
import wikiParser.util.LzmaPipe;
/**
 * Creates Article to Article graph with directed edges using links only
 * @author Nathaniel Miller
 */
public class ArticlesForEditors extends Configured implements Tool {
    private static long lastMillis = System.currentTimeMillis();
    private static long maxElapsed = -1;


    private static String USERNAMES_PATH_KEY = "USERNAMES_PATH";

    /*
     * Takes key-value 7zip hashes and outputs ID-links pairs.
     */
    public static class MyMap extends Mapper<Text,Text,Text,Text> {

        @Override
        public void map(Text key, Text value, Mapper.Context context) throws IOException {
            
            /*
             * Input: ArticleID-7zipHash key-value pairs (no article text).
             * Output: user article revision date
             */
            LzmaPipe pipe = null;
            try {
                reportProgress(context, "init");
                int length = MapReduceUtils.unescapeInPlace(value.getBytes(), value.getLength());
                reportProgress(context, "unescape");
                Set<String> users = readUsernames(context);
                reportProgress(context, "users");
                pipe = new LzmaPipe(value.getBytes(), length);
                PageParser parser = new PageParser(pipe.decompress());
                parser.setHasText(false);
                Page article = parser.getArticle();
                ArticleArticleGenerator edgeGenerator = new ArticleArticleGenerator();
                reportProgress(context, "lzma");
                while (true) {
                    Revision rev = parser.getNextRevision();
                    if (rev == null) {
                        break;
                    }
                    reportProgress(context, "rev");
                    User user = rev.getContributor();
                    if (user != null && user.getName() != null && users.contains(user.getName())) {
                        context.write(new Text(user.getName()),
                                new Text(article.getName() + "\t" + rev.getId() + "\t" + rev.getTimestamp()));
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
            reportProgress(context, "cleanup");
        }

        private Set<String> readUsernames(Mapper.Context context) {
            Configuration conf = context.getConfiguration();
            String path = conf.get(USERNAMES_PATH_KEY);
            try {
                Set<String> names = new HashSet<String>();
                FileSystem hdfs = FileSystem.get(new URI(path), conf);
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(hdfs.open(new Path(path))));

                String line = null;
                while ((line = reader.readLine()) != null) {
                    names.add(line.trim());
                }
                return names;
            } catch (Exception e) {
                System.err.println("open of " + path + " failed:");
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        private final void reportProgress(Mapper.Context context, String label) {
            long now = System.currentTimeMillis();
            long elapsed = now - lastMillis;
            if (maxElapsed < elapsed) {
                System.err.println("elapsed grew to " + (elapsed/1000.0) + " for " + label);
                maxElapsed = elapsed;
            }
            lastMillis = now;
            context.progress();
        }

    }

    @Override
    public int run(String args[]) throws Exception {

        if (args.length < 3) {
            System.out.println("usage: usernames.txt input output");
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String usernamePath = args[0];
        Path inputPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        Configuration conf = getConf();
        conf.set(USERNAMES_PATH_KEY, usernamePath);
        Job job = new Job(conf, this.getClass().toString());

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
       
        job.setJarByClass(ArticlesForEditors.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setMapperClass(MyMap.class);
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
        int res = ToolRunner.run(new ArticlesForEditors(), args);
        System.exit(res);
    }
}
