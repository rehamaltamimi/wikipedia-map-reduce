/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wmr.categories;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
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
import wikiParser.mapReduce.util.KeyValueTextInputFormat;
import wmr.util.EasyLineReader;

/**
 *@author Shilad Sen
 */
public class HadoopCategoryComparer extends Configured implements Tool {
    static final String CATEGORY_PATH_KEY = "CAT_PATH";
    public static class HadoopCategoryComparerWorker extends CategoryComparer {
        EasyLineReader reader;
        Path path;
        Configuration conf;
        
        public HadoopCategoryComparerWorker(Path path, Configuration conf) throws IOException {
            this.path = path;
            this.conf = conf;
        }

        @Override
        public void openFile() throws IOException {
            closeFile();    // just to be safe
            reader = new EasyLineReader(path, conf);
        }

        @Override
        public String readLine() throws IOException {
            return reader.readLine();
        }

        @Override
        public void writeResults(CategoryRecord record, LinkedHashMap<Integer, Double> distances) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public synchronized void closeFile() throws IOException {
            if (reader != null) {
                reader.close();
                reader = null;
            }
        }

    }

    /*
     * Reads output of CitationCounter
     */
    public static class MyMapper extends Mapper<Text, Text, Text, Text> {
        HadoopCategoryComparerWorker worker;

        @Override
        public void setup(Mapper.Context context) throws IOException {
            String path = context.getConfiguration().get(CATEGORY_PATH_KEY);
            worker = new HadoopCategoryComparerWorker(new Path(path), context.getConfiguration());
            worker.prepareDataStructures();
        }


        @Override
        public void map(Text key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            try {
                String line = key.toString() + "\t" + value.toString();
                CategoryRecord rec = worker.parseLine(line, true);
                context.progress();
                if (rec == null) {
                    System.err.println("invalid line: '" + line + "'");
                    return;
                }
                LinkedHashMap<Integer, Double> results = worker.findSimilar(rec);
                StringBuilder builder = new StringBuilder("\"");
                int pageId1 = rec.getPageId();
                for (Integer pageId2 : results.keySet()) {
                    double distance = results.get(pageId2);
                    double score = -Math.log(0.00000001 + distance) / 3.0;
                    if (builder.length() > 1) {
                        builder.append("|");
                    }
                    builder.append(pageId2).append(",").append(truncateDouble("" + score, 5));
                }
                builder.append("\"");
                context.write(new Text("\"" +pageId1 + "\""), new Text(builder.toString()));
            } catch (Exception e) {
                System.err.println("error processing page with id " + key);
                e.printStackTrace();
            }
        }

        private static String truncateDouble(String s, int n) {
            if (s.length() <= n) {
                return s;
            }
            int i = s.indexOf("E");
            if (i >= 0) {
                return s.substring(0, n-2) + s.substring(i);
            } else {
                return s.substring(0, n);
            }
        }

    }

    public int run(String args[]) throws Exception {

        if (args.length < 2) {
            System.out.println("usage: input output [cat_path - defaults to input]");
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        Configuration conf = getConf();
        conf.set(CATEGORY_PATH_KEY, args.length >= 3 ? args[2] : args[0]);
        Job job = new Job(conf, this.getClass().toString());

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        
        job.setJarByClass(HadoopCategoryComparer.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setMapperClass(MyMapper.class);
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
        int res = ToolRunner.run(new HadoopCategoryComparer(), args);
        System.exit(res);
    }
}
