package wmr.core;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import wikiParser.mapReduce.util.MapReduceUtils;
import wmr.util.LzmaDecompresser;
import wmr.util.SecondarySortOnSpace;
import wmr.util.Utils;

public class ChronoSequencer extends Configured implements Tool {
	private static int MAX_LENGTH = 500000;
	
    private static final Logger LOG = Logger.getLogger(ChronoSequencer.class.getName());
    
    public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
    	private static final String KEY_LENGTH = "00";
		private static final String KEY_HEADER = "01";
		private static final String KEY_FOOTER = "99";
		
		LzmaDecompresser pipe = null;
    	BufferedReader reader = null;
    	
        @Override
        public void map(LongWritable position, Text record, Mapper.Context context) throws IOException, InterruptedException {
            closePipe();

            int i = record.find("\t");
            if (i < 0) {
                String beg = new String(record.getBytes(), 0, Math.min(50, record.getLength()));
                LOG.warning("Didn't find tab in line beginning with '" + beg + "'");
                return;
            }
            String pageId = new String(record.getBytes(), 0, i);
            
            context.progress();
            int length = MapReduceUtils.unescapeInPlace(record.getBytes(), i+1, record.getLength());
            try {
            	pipe = new LzmaDecompresser(record.getBytes(), length);
            	reader = new BufferedReader(new InputStreamReader(pipe.decompress()));
            	long nChars = 0;
            	
            	// write header
            	StringBuilder buffer = new StringBuilder();
            	while (true) {
            		String line = reader.readLine();
            		if (line == null) {
        				LOG.warning("EOF encountered in header for article " + pageId);
            			return;
            		}
            		buffer.append(line);
            		buffer.append("\n");
            		
            		// on to revisions....
            		if (line.trim().equals("<page>")) {
            			break;
            		}
            	} 
            	nChars += writeRevision(context, pageId, KEY_HEADER, buffer);
            	
            	// write revisions
            	buffer = new StringBuilder();
            	String tstamp = null;
            	boolean capped = false;
            	while (true) {
            		String line = reader.readLine();
            		if (line == null) {
        				LOG.warning("EOF encountered in body for article " + pageId);
            			return;
            		}
            		String trimmed = line.trim();
					if (trimmed.equals("</page>")) {
            			buffer = new StringBuilder(line + "\n");
            			break;
            		} else if(trimmed.equals("</revision>")) {
            			if (tstamp == null || tstamp.length() < 4) {
            				LOG.warning("no timestamp encountered for article " + pageId);
            				tstamp = "1990-01-01T01:01:01Z";
            			}
            			buffer.append(line + "\n");
            			nChars += writeRevision(context, pageId, tstamp, buffer);
            			
            			buffer = new StringBuilder();
            			tstamp = null;
            			capped = false;
            		} else {
            			if (!capped) {
            				if (buffer.length() + line.length() > MAX_LENGTH) {
                				LOG.warning("capping revision for " + pageId + " at length " + MAX_LENGTH);
                				capped = true;
            				} else {
                    			buffer.append(line + "\n");
            				}
            			}
            			if (trimmed.startsWith("<timestamp>") && trimmed.endsWith("</timestamp>")) {
            				int l = "<timestamp>".length();
            				tstamp = trimmed.substring(l, trimmed.length() - (l + 1));
            			}
            		}
            	}

            	// write footer
            	while (true) {
            		String line = reader.readLine();
            		if (line == null) {
            			break;
            		}
        			buffer.append(line + "\n");
            	}
            	nChars += writeRevision(context, pageId, KEY_FOOTER, buffer);
            	writeRevision(context, pageId, KEY_LENGTH, new StringBuilder(""+nChars));
            } finally {
            	closePipe();
            }
        }

		private int writeRevision(Mapper.Context context, String pageId, String tstamp, StringBuilder buffer) throws IOException, InterruptedException {
			String escaped = Utils.escapeWhitespace(buffer.toString());
			context.write(new Text(pageId.toString() + " " + tstamp), new Text(escaped));
			return escaped.length();
		}
        
        private void closePipe() {
            if (reader != null) {
                try {
                    this.reader.close();
                } catch(Exception e) {
                }
                this.reader = null;
            }
            if (pipe != null) {
                try {
                    this.pipe.cleanup();
                } catch(Exception e) {
                }
                this.pipe = null;
            }
        }

        @Override
        protected void cleanup(Context context
                ) throws IOException, InterruptedException {
        	super.cleanup(context);
        	closePipe();
        }
    	
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
    	ByteArrayOutputStream out = new ByteArrayOutputStream();
        SevenZip.Compression.LZMA.Encoder encoder = new SevenZip.Compression.LZMA.Encoder();
    }
    
    
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("usage: input output");
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

        job.setJarByClass(ChronoSequencer.class);
        job.setInputFormatClass(TextInputFormat.class);
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
        int res = ToolRunner.run(new ChronoSequencer(), args);
        System.exit(res);
    }
}
