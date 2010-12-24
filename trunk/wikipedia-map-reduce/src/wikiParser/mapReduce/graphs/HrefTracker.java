package wikiParser.mapReduce.graphs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import wikiParser.Page;

import wikiParser.PageParser;
import wikiParser.Revision;
import wikiParser.mapReduce.util.MapReduceUtils;
import wikiParser.mapReduce.util.SecondarySortOnHash;
import wikiParser.util.LzmaPipe;

public class HrefTracker extends Configured implements Tool {
    /*
     * Takes key-value 7zip hashes and outputs ID-links pairs.
     */
    public static class MyMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {

        public void map(Text key, Text value, OutputCollector<Text, Text> output,
                Reporter reporter) throws IOException {
            
            LzmaPipe pipe = null;
            try {
                byte[] unescaped = MapReduceUtils.unescape(value.getBytes(), value.getLength());
                pipe = new LzmaPipe(unescaped);
                PageParser parser = new PageParser(pipe.decompress());
                Page article = parser.getArticle();
                java.util.Map<String, Integer> linkCounts = new HashMap<String, Integer>();
//                System.err.println("processing article " + key + "(" + parser.getArticle().getName() + ")");
                while (true) {
                    Revision rev = parser.getNextRevision();
//                    System.err.println("doing revision " + rev.getId() + " at " + rev.getTimestamp());
                    if (rev == null) {
                        break;
                    }
                    linkCounts = processRevision(parser.getArticle(), rev, linkCounts, output);
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

        private Map<String, Integer> processRevision(Page article, Revision rev, Map<String, Integer> prevLinkCounts, OutputCollector<Text, Text> output) throws IOException {
            int numUnique = 0;
            int numPrinted = 0;
            String title = article.getName();
            Map<String, Integer> nextLinkCounts = new HashMap<String, Integer>();
            for (String link : rev.getAnchorLinksWithoutFragments()) {
                if (link.contains("\n") || link.contains("\t")) {
                    continue;
                }
                if (!nextLinkCounts.containsKey(link)) {
                    nextLinkCounts.put(link, 0);
                }
                nextLinkCounts.put(link, nextLinkCounts.get(link) + 1);
            }
            for (String link : nextLinkCounts.keySet()) {
                numUnique++;
                Integer nc = nextLinkCounts.get(link);
                Integer pc = prevLinkCounts.get(link);
                if (pc == null || !pc.equals(nc)) {
                    numPrinted++;
                    output.collect(new Text(title  + "#" + link), new Text(rev.getTimestamp() + "#" + nc));
                }
            }
            for (String link : prevLinkCounts.keySet()) {
                if (!nextLinkCounts.containsKey(link)) {
                    numUnique++;
                    numPrinted++;
                    output.collect(new Text(title  + "#" + link), new Text(rev.getTimestamp() + "#" + 0));
                }
            }
//            if (Math.random() > 0.99) {
//                System.err.println("for revision " + rev.getTimestamp() + " found " + numPrinted + " of " + numUnique);
//            }
            return nextLinkCounts;
        }
    }

    /**
     * Convert the date to a UNIX seconds since the epoch, pad it if necessary.
     * @param rev
     * @return
     */
    public static String padUnixTimestamp(Revision rev) {
        Date d = rev.getTimestampAsDate();
        String s = "" + d.getTime();
        while (s.length() < 10) {
            s = "0" + s;
        }
        return s;
    }


    public static class MyReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text,Text> output,
                        Reporter reporter) throws IOException {
            List<String> allValues = new ArrayList<String>();
            while (values.hasNext()) {
                allValues.add(values.next().toString());
            }
            Collections.sort(allValues);
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < allValues.size(); i++) {
                if (i != 0) {
                    sb.append("\t");
                }
                sb.append(allValues.get(i));
            }
            output.collect(key, new Text(sb.toString()));
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

        JobConf job = new JobConf(getConf(), this.getClass());
        job.setJobName(this.getClass().toString());
        SecondarySortOnHash.setupSecondarySortOnHash(job);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setInputFormat(KeyValueTextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        FileSystem hdfs = FileSystem.get(outputPath.toUri(), job);
        if (hdfs.exists(outputPath)) {
            hdfs.delete(outputPath, true);
        }

        JobClient.runJob(job);

        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the
     * <code>ToolRunner</code>.
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new HrefTracker(), args);
        System.exit(res);
        return;
    }
}
