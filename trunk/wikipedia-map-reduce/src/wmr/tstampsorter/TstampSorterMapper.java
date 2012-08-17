package wmr.tstampsorter;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Logger;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import wmr.util.LzmaDecompresser;
import wmr.util.Utils;

public class TstampSorterMapper extends Mapper<LongWritable, Text, Text, Text> {
    static final Logger LOG = Logger.getLogger(TstampSorterMapper.class.getName());
	
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
        	ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        	while (true) {
        		String line = reader.readLine();
        		if (line == null) {
    				LOG.warning("EOF encountered in header for article " + pageId);
        			return;
        		}
        		buffer.write(line.getBytes("UTF-8"));
        		buffer.write('\n');
        		
        		// on to revisions....
        		if (line.trim().equals("<page>")) {
        			break;
        		}
        	} 
        	nChars += writeRevision(context, pageId, TstampSorterMain.KEY_HEADER, null, buffer);
        	
        	// write revisions
        	buffer.reset();
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
					buffer.reset();
					buffer.write(line.getBytes("UTF-8"));
            		buffer.write('\n');
        			break;
        		} else if(trimmed.equals("</revision>")) {
        			if (tstamp == null || tstamp.length() < 4) {
        				LOG.warning("no timestamp encountered for article " + pageId);
        				tstamp = "1990-01-01T01:01:01Z";
        			}
					buffer.write(line.getBytes("UTF-8"));
            		buffer.write('\n');
        			nChars += writeRevision(context, pageId, TstampSorterMain.KEY_REVISION, tstamp, buffer);
        			
        			buffer.reset();
        			tstamp = null;
        			capped = false;
        		} else {
        			if (!capped) {
        				if (buffer.size() + line.length() > TstampSorterMain.MAX_LENGTH) {
            				LOG.warning("capping revision for " + pageId + " at length " + TstampSorterMain.MAX_LENGTH);
            				capped = true;
        				} else {
    						buffer.write(line.getBytes("UTF-8"));
    	            		buffer.write('\n');
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
				buffer.write(line.getBytes("UTF-8"));
        		buffer.write('\n');
        	}
        	nChars += writeRevision(context, pageId, TstampSorterMain.KEY_FOOTER, null, buffer);
        	buffer.reset();
        	buffer.write(("" + nChars).getBytes());
        	writeRevision(context, pageId, TstampSorterMain.KEY_LENGTH, null, buffer);
        } finally {
        	closePipe();
        }
    }

	private int writeRevision(Mapper.Context context, String pageId, char field, String tstamp, ByteArrayOutputStream buffer) throws IOException, InterruptedException {
		byte[] escaped = Utils.escapeWhitespace(buffer.toByteArray());
		String key = pageId.toString() + " " + field;
		if (tstamp != null) key += tstamp;
		context.write(new Text(key), new Text(escaped));
		return buffer.size();
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