/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package wikiParser.mapReduce.util;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import wikiParser.mapReduce.statistics.RevisionCountData;

/**
 * @author shilad
 */
public class SimpleJobConf {
    private static final int NUM_MAP_TASKS = 200;
    private static final int NUM_REDUCE_TASKS = 20;
    private static final int NUM_MAP_TASK_FAILURES_PERCENT= 5;
    private static final int NUM_REDUCE_TASK_FAILURES_PERCENT= 5;
    private static final int NUM_MAX_ATTEMPTS = 2;
    private static final String MAP_REDUCE_CHILD_OPTS = "";
    
   // millis between map-reduce outputs
    private static final int TIMEOUT_MILLIS = 1000 * 60 * 10;
    /**
	 * @uml.property  name="conf"
	 * @uml.associationEnd  multiplicity="(1 1)"
	 */
    private JobConf conf;

    /**
     * Creates a new job configuration with reasonable defaults.
     * Defaults can be overridden by calling getConf() and changing the values directly.
     * Use the static makeEC2Conf method to make things a bit easer
     * .
     * @param mapperClass
     * @param reducerClass
     * @param inputFiles
     * @param outputDir
     * @param jobName
     * @throws IOException
     */
    public SimpleJobConf(Class mapperClass, Class reducerClass, String[] inputFiles, String outputDir, String jobName) throws IOException{
        conf = new JobConf(RevisionCountData.class);
        conf.setJobName(jobName);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(mapperClass);
        conf.setReducerClass(reducerClass);
        conf.setInputFormat(KeyValueTextInputFormat.class);  // input to mapper
        conf.setOutputFormat(TextOutputFormat.class);  //output file from reducer

        Path [] inputPaths = new Path[inputFiles.length];
        for (int i = 0; i < inputFiles.length; i++) {
            inputPaths[i] = new Path(inputFiles[i]);
        }

        Path outputPath = new Path(outputDir);
        FileSystem hdfs = FileSystem.get(outputPath.toUri(), conf);
        if (hdfs.exists(outputPath)) {
            hdfs.delete(new Path(outputDir), true);
        }

        FileInputFormat.setInputPaths(conf, inputPaths);
        FileOutputFormat.setOutputPath(conf, outputPath);

        // note that taskTimeout is a number of miliseconds
        conf.setLong("mapred.task.timeout", TIMEOUT_MILLIS);
//        conf.set("mapred.child.java.opts", MAP_REDUCE_CHILD_OPTS);
//        conf.setNumMapTasks(NUM_MAP_TASKS);
//        conf.setNumReduceTasks(NUM_REDUCE_TASKS);
        conf.setMaxMapTaskFailuresPercent(NUM_MAP_TASK_FAILURES_PERCENT);
        conf.setMaxReduceTaskFailuresPercent(NUM_REDUCE_TASK_FAILURES_PERCENT);
        conf.setMaxMapAttempts(NUM_MAX_ATTEMPTS);
    }

    /**
	 * @return
	 * @uml.property  name="conf"
	 */
    public JobConf getConf() {
        return conf;
    }

    public void run() throws IOException {
        JobClient.runJob(conf);
    }

    public void runTest() throws IOException {
        conf.setNumMapTasks(10);
        conf.setNumReduceTasks(4);
        JobClient.runJob(conf);
    }
}
