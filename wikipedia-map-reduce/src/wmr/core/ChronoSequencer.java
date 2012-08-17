package wmr.core;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.logging.Level;
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

import wmr.util.LzmaCompressor;
import wmr.util.LzmaDecompresser;
import wmr.util.SecondarySortOnSpace;
import wmr.util.Utils;

public class ChronoSequencer extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(ChronoSequencer.class.getName());
	
	// these values must be ordered alphabetically
	private static final char KEY_LENGTH = '0';
	private static final char KEY_HEADER = '1';
	private static final char KEY_REVISION = '2';
	private static final char KEY_FOOTER = '9';
	
	// Maximum number of bytes in the text of a revision 
	private static int MAX_LENGTH = 500000;
	
    
    public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		
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
            int length = Utils.unescapeInPlace(record.getBytes(), i+1, record.getLength());
            try {
            	pipe = new LzmaDecompresser(record.getBytes(), length);
            	reader = new BufferedReader(new InputStreamReader(pipe.decompress(), "UTF-8"));
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
            	nChars += writeRevision(context, pageId, KEY_HEADER, null, buffer);
            	
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
            			nChars += writeRevision(context, pageId, KEY_REVISION, tstamp, buffer);
            			
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
            	nChars += writeRevision(context, pageId, KEY_FOOTER, null, buffer);
            	writeRevision(context, pageId, KEY_LENGTH, null, new StringBuilder(""+nChars));
            } finally {
            	closePipe();
            }
        }

		private int writeRevision(Mapper.Context context, String pageId, char field, String tstamp, StringBuilder buffer) throws IOException, InterruptedException {
			String text = buffer.toString();
			String escaped = Utils.escapeWhitespace(text);
			String key = pageId.toString() + " " + field;
			if (tstamp != null) key += tstamp;
			context.write(new Text(key), new Text(escaped.getBytes("UTF-8")));
			return text.length();
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
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	String pageId = getPageId(key);
        	char code = getFieldCode(key);
        	if (code != KEY_LENGTH) {
        		LOG.warning("illegal first key in reducer: " + key);
        		return;
        	}
        	Iterator<Text> iter = values.iterator();
        	long length = Long.valueOf(iter.next().toString());
        	
        	LzmaCompressor pipe = null;
        	OutputStream out = null;
        	try {
        		pipe = new LzmaCompressor(length);
        		out = pipe.compress();
        		if (getFieldCode(key) != KEY_HEADER) {
            		LOG.warning("illegal second key in reducer: " + key);
            		return;
        		}
        		out.write(iter.next().getBytes());
        		while (true) {
        			if (getFieldCode(key) != KEY_REVISION) {
        				break;
        			}
            		out.write(iter.next().getBytes());
        		}
        		if (getFieldCode(key) != KEY_FOOTER) {
            		LOG.warning("illegal key in reducer (expected footer): " + key);
            		return;
        		}
        		out.write(iter.next().getBytes());
        		if (iter.hasNext()) {
            		LOG.warning("extra unexpected values with key " + key + " (ignoring them)");
        		}
        		pipe.cleanup();
        		String compressed = new String(pipe.getCompressed(), "UTF-8");
        		pipe = null;
        		context.write(new Text(pageId), new Text(Utils.escapeWhitespace(compressed)));
        	} finally {
        		try {
        			if (pipe != null) pipe.cleanup();
        			out.close();
        		} catch (Exception e) {
        			LOG.log(Level.WARNING, "Pipe cleanup failed", e);
        		}
        	}
        }

        private String getPageId(Text key) {
        	int i = key.find(" ");
        	if (i < 0) {
        		throw new IllegalStateException("invalid key: " + key);
        	}
        	return key.toString().substring(0, i);
        }
        
        private char getFieldCode(Text key) {
        	int i = key.find(" ");
        	if (i < 0 || (i+1) >= key.getLength()) {
        		throw new IllegalStateException("invalid key: " + key);
        	}
        	return (char)key.charAt(i+1);
        }
        
        private String getTstamp(Text key) {
        	int i = key.find(" ");
        	if (i < 0 || (i+1) >= key.getLength()) {
        		throw new IllegalStateException("invalid key: " + key);
        	}
        	return key.toString().substring(i+2);
        }
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
