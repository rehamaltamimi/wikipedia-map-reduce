/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package wmr.wmf;

import com.google.gson.Gson;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import name.fraser.neil.plaintext.diff_match_patch;
import name.fraser.neil.plaintext.diff_match_patch.Diff;
import name.fraser.neil.plaintext.diff_match_patch.Operation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import wmr.core.AllRevisions;
import wmr.core.AllRevisionsInputFormat;
import wmr.core.Page;
import wmr.core.Revision;
import wmr.util.SecondarySortOnSpace;
import wmr.util.Utils;

/**
 *
 * @author shilad
 */
public class WmfDiffCreator extends Configured implements Tool {
    static class MyMapper extends Mapper<Long, AllRevisions, Text, Text> {
        @Override
        public void map(Long pageId, AllRevisions revs, Context context) throws IOException, InterruptedException {
            Gson gson = new Gson();
            Page page = revs.getPage();
            
            for (Revision r : revs.getRevisions()) {
                Map<String, Object> record = new HashMap<String, Object>();
                record.put("_id", Integer.valueOf(r.getId()));
                record.put("pageId", new Integer(pageId.intValue()));
                record.put("title", page.getName());
                record.put("tstamp", r.getTimestamp());
                record.put("comment", r.getComment());
                record.put("text", r.getText());
                if (r.getContributor().getName() != null) {
                    record.put("editorName", r.getContributor().getName());
                }
                if (r.getContributor().getId() != null) {
                    record.put("editorId", r.getContributor().getId());
                }

                // left-pad the revision id to make sorting nice.
                String revId = "" + r.getId();
                while (revId.length() < 11) {
                    revId = "0" + revId;
                }

                context.write(new Text("" + pageId + " " + revId), new Text(gson.toJson(record)));
            }
        }
    }

    static class MyReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Integer prevId = null;
            String prevText = "";
            Gson gson = new Gson();
            for (Text val : values) {
                Map<String, Object> jsonObj = gson.fromJson(val.toString(), Map.class);
                String text = (String)jsonObj.get("text");

                diff_match_patch dmp = new diff_match_patch();
                List<Diff> diffs = dmp.diff_main(prevText, text, false);

                List<Map<String, Object>> diffJson = new ArrayList<Map<String, Object>>();
                int insertedBytes = 0;
                int deletedBytes = 0;
                int origLocation = 0;
                for (Diff d : diffs) {
                    if (d.operation.equals(Operation.EQUAL)) {
                        origLocation += d.text.length();
                        continue;
                    }
                    Map<String, Object> diffRec = new HashMap<String, Object>();

                    if (d.operation.equals(Operation.INSERT)) {
                        diffRec.put("loc", origLocation);
                        diffRec.put("op", "i");
                        diffRec.put("text", d.text);
                        insertedBytes += d.text.length();
                    } else if(d.operation.equals(Operation.DELETE)) {
                        diffRec.put("loc", origLocation);
                        diffRec.put("op", "d");
                        diffRec.put("size", d.text.length());

                        origLocation += d.text.length();
                        deletedBytes += d.text.length();
                    } else {
                        assert(false);
                    }
                    diffJson.add(diffRec);
                }
                jsonObj.remove("text");
                jsonObj.put("diffs", diffJson);
                jsonObj.put("totalBytes", text.length());
                jsonObj.put("insertedBytes", insertedBytes);
                jsonObj.put("deletedBytes", deletedBytes);
                jsonObj.put("md5", Utils.md5(text));
                if (prevId != null) {
                    jsonObj.put("prevId", prevId);
                }
                context.write(new Text(gson.toJson(jsonObj)), new Text(""));

                prevId = (Integer)jsonObj.get("_id");
                prevText = text;
            }
        }
    }


    public int run(String[] args) throws Exception {
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
        SecondarySortOnSpace.setupSecondarySortOnSpace(job);


        job.setJarByClass(WmfDiffCreator.class);
        job.setInputFormatClass(AllRevisionsInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class); // identity reducer

        FileSystem hdfs = FileSystem.get(outputPath.toUri(), conf);
        if (hdfs.exists(outputPath)) {
            hdfs.delete(outputPath, true);
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new WmfDiffCreator(), args);
        System.exit(res);
    }
}
