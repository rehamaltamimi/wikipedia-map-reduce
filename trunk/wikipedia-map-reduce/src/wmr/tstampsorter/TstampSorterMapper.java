package wmr.tstampsorter;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
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
        	
        	// write header
        	ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        	long nChars = writeHeader(context, pageId, 0, buffer);
        	if (nChars < 0) {
        		return;
        	}
        	
        	// write revisions
        	nChars = writeRevisions(context, pageId, buffer, nChars);
        	if (nChars < 0) {
        		return;
        	}

        	writeFooter(context, pageId, nChars, buffer);
        } finally {
        	closePipe();
        }
    }

	private long writeRevisions(Mapper.Context context, String pageId,
			ByteArrayOutputStream buffer, long nChars) throws IOException,
			UnsupportedEncodingException, InterruptedException {
		
		String tstamp = null;
		long numTruncated = 0;
		
		while (true) {
			String line = reader.readLine();
			if (line == null) {
				LOG.warning("EOF encountered in body for article " + pageId);
				return -1;
			}
			String trimmed = line.trim();
			
			// are we finished with revisions?
			if (trimmed.equals("</page>")) {
				buffer.reset();
				buffer.write(line.getBytes("UTF-8"));
				buffer.write('\n');
				break;
			}
			
			if (trimmed.equals("</revision>")) { // end of revision
				if (tstamp == null || tstamp.length() < 4) {
					LOG.warning("no timestamp encountered for article " + pageId);
					tstamp = "1990-01-01T01:01:01Z";
				}
				boolean valid = true;
				if (numTruncated > 0) {
					LOG.warning("truncated revision of pageid " + pageId + ", rev " + tstamp + 
							" to " + TstampSorterMain.MAX_LENGTH + 
							" (truncated " + numTruncated + " bytes)");
					String current = buffer.toString();
					if (current.indexOf("<text") < 0) {
						LOG.warning("did not reach text segment before reaching byte limit for " + pageId + " rev " + tstamp);
						valid = false;
					} else if (current.indexOf("</text>") < 0) {
						buffer.write("      </text>\n".getBytes());
					}
				} 
				if (valid) {
					buffer.write(line.getBytes("UTF-8"));
		    		buffer.write('\n');
					nChars += writeSegment(context, pageId, TstampSorterMain.KEY_REVISION, tstamp, buffer);
				}
				
				buffer.reset();
				tstamp = null;
				numTruncated = 0;
			} else {	// middle of revision
				if (numTruncated > 0) {
					numTruncated += line.length();
				} else if (buffer.size() + line.length() > TstampSorterMain.MAX_LENGTH) {
					numTruncated = Math.max(line.length(), 1);
				} else {
					buffer.write(line.getBytes("UTF-8"));
	        		buffer.write('\n');
				}
				if (trimmed.startsWith("<timestamp>") && trimmed.endsWith("</timestamp>")) {
					int l = "<timestamp>".length();
					tstamp = trimmed.substring(l, trimmed.length() - (l + 1));
				}
			}
		}
		return nChars;
	}

	private long writeHeader(Mapper.Context context, String pageId,
			long nChars, ByteArrayOutputStream buffer) throws IOException,
			UnsupportedEncodingException, InterruptedException {
		
		String line = null;
		while (true) {
			line = reader.readLine();
			if (line == null) {
				LOG.warning("EOF encountered in header for article " + pageId);
				return -1;
			}
			// on to revisions....
			if (line.trim().equals("<revision>")) {
				break;
			}
			
			buffer.write(line.getBytes("UTF-8"));
			buffer.write('\n');			
		} 
		nChars += writeSegment(context, pageId, TstampSorterMain.KEY_HEADER, null, buffer);
		
		// push the first "<revision>" tag into the buffer.
		buffer.reset();
		buffer.write(line.getBytes("UTF-8"));
		buffer.write('\n');
		
		return nChars;
	}

	private void writeFooter(Mapper.Context context, String pageId,
			long nChars, ByteArrayOutputStream buffer) throws IOException,
			UnsupportedEncodingException, InterruptedException {
		// write footer
		while (true) {
			String line = reader.readLine();
			if (line == null) {
				break;
			}
			buffer.write(line.getBytes("UTF-8"));
			buffer.write('\n');
		}
		nChars += writeSegment(context, pageId, TstampSorterMain.KEY_FOOTER, null, buffer);
		buffer.reset();
		buffer.write(("" + nChars).getBytes());
		writeSegment(context, pageId, TstampSorterMain.KEY_LENGTH, null, buffer);
	}

	private int writeSegment(Mapper.Context context, String pageId, char field, String tstamp, ByteArrayOutputStream buffer) throws IOException, InterruptedException {
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