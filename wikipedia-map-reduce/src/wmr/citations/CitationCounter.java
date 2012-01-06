/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wmr.citations;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import wmr.core.*;
import wikiParser.mapReduce.util.*;
import wmr.util.*;

/**
 *
 * @author Nathaniel Miller
 * 
 * First stage of citation counting process
 * Output: 
 * K: url@articleId, V: #added  #removed    #revisions
 */
public class CitationCounter extends Configured implements Tool {

    /*
     * Takes key-value 7zip hashes and outputs url-id pairs.
     */
    public static class MyMapper extends Mapper<Text, Text, Text, Text> {

        Map<String, Map<String,Integer>> citeCounts = new HashMap<String, Map<String,Integer>>();
        public void map(Text key, Text value, Mapper.Context context) throws IOException {
            LzmaPipe pipe = null;
            try {
                context.progress();
                int length = MapReduceUtils.unescapeInPlace(value.getBytes(), value.getLength());
                pipe = new LzmaPipe(value.getBytes(), length);
                PageParser parser = new PageParser(pipe.decompress());
                Page article = parser.getArticle();
                Map<String, Integer> citeCounts = new HashMap<String, Integer>();
                System.err.println("processing article " + key + "(" + parser.getArticle().getName() + ")");
                if (article.isNormalPage()) {//main namespace only
                    Set <String> urls = new HashSet<String>();
                    while (true) {
                        context.progress();
                        Revision rev = parser.getNextRevision();
                        if (rev == null) {
                            break;
                        }
                        //System.err.println("doing revision " + rev.getId() + " at " + rev.getTimestamp());
                        Map<String, Integer> newCiteCounts = processRevision(parser.getArticle(), rev);

                        writeDiff(context, article, rev, citeCounts, newCiteCounts);

                        citeCounts = newCiteCounts;
                    }
                }
            } catch (Exception e) {
                context.progress();
                System.err.println("error when processing " + key + ":");
                e.printStackTrace();
            } finally {
                if (pipe != null) {
                    pipe.cleanup();
                }
            }
        }

        private void writeDiff(Mapper.Context context, Page article, Revision rev,
                Map<String, Integer> citeCounts, Map<String, Integer> newCiteCounts) throws IOException, InterruptedException {

            // deleted assessments
            for (String url : citeCounts.keySet()) {
                if (newCiteCounts.containsKey(url)) {
                    continue;   // will be listed later.
                }
                User u = rev.getContributor();
                int c0 = citeCounts.get(url);
                int c1 = 0;
                if (c0 == 0) {
                    continue;   // shouldn't really happen
                }
                context.write(
                    new Text(article.getName() + "@" + article.getId()),
                    new Text(
                            rev.getTimestamp() + "\t" +
                            rev.getId() + "\t" +
                            u.getName() + "@" + u.getId() + "\t" +
                            u.isBot() + "\t" +
                            url + "\t" +
                            c0 + "\t" +
                            c1 + "\t"
                        )
                    );
            }

            // added and updated assessments
            for (String url : newCiteCounts.keySet()) {
                User u = rev.getContributor();
                int c0 = citeCounts.containsKey(url) ? citeCounts.get(url) : 0;
                int c1 = newCiteCounts.get(url);
                if (c0 != c1) {
                    context.write(
                        new Text(article.getName() + "@" + article.getId()),
                        new Text(
                                rev.getTimestamp() + "\t" +
                                rev.getId() + "\t" +
                                u.getName() + "@" + u.getId() + "\t" +
                                u.isBot() + "\t" +
                                url + "\t" +
                                c0 + "\t" +
                                c1 + "\t"

                            )
                    );
                }
            }
        }

        private Map<String, Integer> processRevision(Page article, Revision rev) throws IOException {
            HashMap<String, Integer> citeCounts = new HashMap<String, Integer>();
            for (Citation c : rev.getCitations(article)) {
                String url = c.getUrl();
                url = (url == null) ? "noURL" : url.replaceAll("[\\s]+", " ");
                if (citeCounts.containsKey(url)) {
                    citeCounts.put(url, citeCounts.get(url) + 1);
                } else {
                    citeCounts.put(url, 1);
                }
            }
            return citeCounts;
        }
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

        
        job.setJarByClass(CitationCounter.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(Reducer.class); // identity reducer
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
        int res = ToolRunner.run(new CitationCounter(), args);
        System.exit(res);
    }
}
