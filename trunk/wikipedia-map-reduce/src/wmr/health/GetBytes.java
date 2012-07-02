/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wmr.health;


import java.io.IOException;
import java.util.LinkedList;

import name.fraser.neil.plaintext.diff_match_patch;
import name.fraser.neil.plaintext.diff_match_patch.Diff;
import name.fraser.neil.plaintext.diff_match_patch.Operation;

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


/**
 *
 * @author Shilad Sen
 * @author Guanyu Wang
 * 
 */
public class GetBytes extends Configured implements Tool {

    public static class MyMapper extends Mapper<Long, AllRevisions, Text, Text> {

        /**
         * Outputs the editor name, namespace, bytes of the inserted text, bytes of the 
         * deleted text, delta bytes, and year of the edit.  -- Guanyu  
         */
        @Override
        public void map(Long pageId, AllRevisions revs, Mapper.Context context) throws IOException, InterruptedException {
            Page article = revs.getPage();
            context.progress();
            
            //int prev = 0;
            String prevtext = "";
            
            for (Revision rev : revs.getRevisions()) {
               context.progress();
               if (rev == null) {
                    continue;
               }
               User u = rev.getContributor();
               
               /**
                * For RevertAwareAllRevisionsInputFormat, add limitations for u:
                * !u.isReverted() && !u.isVandalism()
                */
               if (!u.isBot() && !u.isAnonymous()) {  
                  String key = u.getName();
                  String namespace = "" + article.getNamespace();  
                  String val = "" + rev.getTimestamp();
                  String year = val.substring(0, 4);
                  
                  String text = rev.getText();              // get the text edited
                  //int numbytes = text.length();                              
                  //int delta = numbytes - prev;
                  
                  int[] deltatext = addRevDiffs(prevtext,text);         // an array of inserted and deleted text  
                  String insertedtext = Integer.toString(deltatext[0]);  
                  String deletedtext = Integer.toString(deltatext[1]);
                  
                  //String entries = namespace + "\t" + Integer.toString(delta) + "\t" + year;
                  String entries = namespace + "\t" + insertedtext + "\t"  
                                   + deletedtext + "\t" + Integer.toString(deltatext[0]-deltatext[1]) 
                                   + "\t" + year;
                  context.write(new Text(key), new Text(entries));
               }
               //prev = rev.getText().length();
               prevtext = rev.getText();
            }            
        }
    }
    
    public static int[] addRevDiffs(String prevText, String text) {
        diff_match_patch dmp = new diff_match_patch();
        LinkedList<Diff> diffs = dmp.diff_main(prevText, text, false);

        // may be slightly non-optimal, but makes much more sense
        dmp.diff_cleanupSemantic(diffs);

        //List<Map<String, Object>> diffJson = new ArrayList<Map<String, Object>>();
        int insertedBytes = 0;
        int deletedBytes = 0;
        int origLocation = 0;
        for (Diff d : diffs) {
            if (d.operation.equals(Operation.EQUAL)) {
                origLocation += d.text.length();
                continue;
            }
            //Map<String, Object> diffRec = new HashMap<String, Object>();
            if (d.operation.equals(Operation.INSERT)) {
                insertedBytes += d.text.length();
            } else if (d.operation.equals(Operation.DELETE)) {
                origLocation += d.text.length();
                deletedBytes += d.text.length();
            } else {
                assert (false);
            }
            //diffJson.add(diffRec);           
        }
        int[] deltatext = new int[]{insertedBytes,deletedBytes};
        return deltatext;
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

        
        job.setJarByClass(GetBytes.class);
        job.setInputFormatClass(AllRevisionsInputFormat.class); /**RevertAwareAllRevisionsInputFormat*/
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
        int res = ToolRunner.run(new GetBytes(), args);
        System.exit(res);
    }
}
